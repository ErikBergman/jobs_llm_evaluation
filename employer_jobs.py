#!/usr/bin/env python3
"""Index public employer ads into the existing LinkedIn matcher's waiting room."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, time as daytime, timezone
from email.utils import parsedate_to_datetime
from html import escape, unescape
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPCookieProcessor, Request, build_opener

from linkedin_guest_jobs import (
    DEFAULT_MATCHING_CONFIG_PATH, JobDetail, apply_prefilter_metadata,
    extract_requirements_text_from_fragment, load_matching_config, load_seen_job_ids,
)
from dataclasses import asdict

CONFIG_PATH = Path(__file__).with_name("employer_scraping.json")
VOID_TAGS = set("area base br col embed hr img input link meta param source track wbr".split())


class SourceError(ValueError):
    """A source failed validation; never interpret this as an empty job board."""


@dataclass
class Node:
    tag: str
    attrs: dict[str, str] = field(default_factory=dict)
    children: list = field(default_factory=list)

    def find(self, tag=None, cls=None, **attrs):
        for child in self.children:
            if not isinstance(child, Node):
                continue
            if ((tag is None or child.tag == tag)
                    and (cls is None or cls in child.attrs.get("class", "").split())
                    and all(child.attrs.get(k) == v for k, v in attrs.items())):
                yield child
            yield from child.find(tag, cls, **attrs)

    def text(self):
        if self.tag in {"script", "style", "noscript"}:
            return ""
        parts = [c.text() if isinstance(c, Node) else c for c in self.children]
        text = "".join(parts)
        return text + ("\n" if self.tag in {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4"} else "")

    def html(self):
        if self.tag in {"script", "style", "noscript"}:
            return ""
        body = "".join(c.html() if isinstance(c, Node) else escape(c) for c in self.children)
        return f"<{self.tag}>" + body + (f"</{self.tag}>" if self.tag not in VOID_TAGS else "")


class Document(HTMLParser):
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.root = Node("root")
        self.stack = [self.root]
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        node = Node(tag, {k: v or "" for k, v in attrs})
        self.stack[-1].children.append(node)
        if tag not in VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID_TAGS:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)


def clean(value):
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def description_fragment(value):
    value = str(value or "")
    # Feeds and JSON-LD can encode HTML once or twice.
    for _ in range(2):
        decoded = unescape(value)
        if decoded == value:
            break
        value = decoded
    if not re.search(r"</?(?:p|div|h[1-6]|li|ul|br|strong|span)\b", value, re.I):
        value = "".join(f"<p>{escape(line)}</p>" for line in value.splitlines() if line.strip())
    return value


def plain_text(fragment):
    text = Document(fragment).root.text()
    return "\n".join(clean(line) for line in text.splitlines() if clean(line))


def canonical_url(url):
    u = urlsplit(unescape(url))
    query = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"ref", "referrer", "fbclid", "gclid"}]
    return urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path.rstrip("/"), urlencode(sorted(query)), ""))


def native_id(url, fallback=""):
    for pattern in (r"jobID:(\d+)", r"/jobs/(\d+)", r"/job/[^?]+/(\d+)/?(?:\?|$)"):
        match = re.search(pattern, url, re.I)
        if match:
            return match.group(1)
    query = parse_qs(urlsplit(url).query)
    return (query.get("job_id") or [fallback or hashlib.sha256(canonical_url(url).encode()).hexdigest()[:20]])[0]


def iso_date(value):
    if not value:
        return ""
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        try:
            return parsedate_to_datetime(str(value)).date().isoformat()
        except (ValueError, TypeError):
            return ""


def expired(value, today=None):
    if not value:
        return False
    try:
        deadline = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if len(value) == 10:
            deadline = datetime.combine(deadline.date(), daytime.max, tzinfo=timezone.utc)
        elif deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        return deadline < (today or datetime.now(timezone.utc))
    except ValueError as exc:
        raise SourceError(f"Unrecognised expiration date: {value}") from exc


class SafeRedirect(HTTPRedirectHandler):
    def __init__(self, allowed):
        self.allowed = allowed

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_url(newurl, self.allowed)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def validate_url(url, allowed):
    u = urlsplit(url)
    if u.scheme != "https" or u.hostname not in allowed or u.username or u.password or u.port not in {None, 443}:
        raise SourceError(f"URL is outside this source's configured HTTPS hosts: {url}")


class Client:
    def __init__(self, hosts, delay=1.0, timeout=30):
        self.hosts = hosts
        self.delay = delay
        self.timeout = timeout
        self.requests = 0
        self.last_request = 0.0
        self.opener = build_opener(SafeRedirect(hosts), HTTPCookieProcessor(CookieJar()))

    def get(self, url, data=None):
        validate_url(url, self.hosts)
        for attempt in range(3):
            time.sleep(max(0, self.delay - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            self.requests += 1
            headers = {"User-Agent": "LundJobSearch/1.0 (public recruitment indexing)", "Accept": "*/*"}
            body = None
            if data is not None:
                headers.update({"Content-Type": "application/json", "Accept": "application/json"})
                body = json.dumps(data).encode()
            try:
                with self.opener.open(Request(url, data=body, headers=headers), timeout=self.timeout) as response:
                    raw = response.read(10_000_001)
                    if len(raw) > 10_000_000:
                        raise SourceError("Response exceeds the 10 MB limit")
                    return raw.decode(response.headers.get_content_charset() or "utf-8", errors="strict")
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                    raise SourceError(f"HTTP {exc.code}: {url}") from exc
                retry = exc.headers.get("Retry-After", "")
                try:
                    seconds = float(retry)
                except ValueError:
                    try:
                        seconds = (parsedate_to_datetime(retry) - datetime.now(timezone.utc)).total_seconds()
                    except (ValueError, TypeError):
                        seconds = 2 ** (attempt + 1)
                if seconds > 30:
                    raise SourceError(f"Server requested retry after {seconds:.0f}s; defer this source") from exc
                time.sleep(max(0, seconds))
            except (URLError, TimeoutError, UnicodeError) as exc:
                raise SourceError(f"Fetch failed for {url}: {exc}") from exc


@dataclass
class RawJob:
    native_id: str
    url: str
    title: str = ""
    location: str = ""
    description: str = ""
    posted_date: str = ""
    valid_through: str = ""
    method: str = ""
    active: bool = True
    location_method: str = "source_metadata"


def jsonld_job(html, url):
    def walk(value):
        if isinstance(value, list):
            for child in value:
                yield from walk(child)
        elif isinstance(value, dict):
            types = value.get("@type", [])
            if isinstance(types, str):
                types = [types]
            if any(isinstance(t, str) and t.rsplit("/", 1)[-1] == "JobPosting" for t in types):
                yield value
            else:
                for child in value.values():
                    if isinstance(child, (dict, list)):
                        yield from walk(child)
    for node in Document(html).root.find("script", type="application/ld+json"):
        try:
            records = list(walk(json.loads("".join(c for c in node.children if isinstance(c, str)))))
        except json.JSONDecodeError:
            continue
        if len(records) > 1:
            raise SourceError("Expected one JobPosting on a detail page")
        for record in records:
            locations = record.get("jobLocation", [])
            if isinstance(locations, dict):
                locations = [locations]
            names = []
            for location in locations:
                address = location.get("address", {}) if isinstance(location, dict) else {}
                if isinstance(address, dict):
                    parts = [address.get(k, "") for k in ("addressLocality", "addressRegion", "addressCountry")]
                    names.append(", ".join(clean(x.get("name", "") if isinstance(x, dict) else x) for x in parts if x))
            return RawJob(native_id(url), url, clean(record.get("title")), "; ".join(names),
                          record.get("description", ""), iso_date(record.get("datePosted")),
                          record.get("validThrough", ""), "jsonld")
    raise SourceError(f"No usable JobPosting JSON-LD on {url}")


def varbi_metadata(html):
    result = {}
    for row in Document(html).root.find("tr"):
        town = next(row.find(cls="pos-town"), None)
        title = next(row.find(cls="pos-title"), None)
        if town is None or title is None:
            continue
        link = next(title.find("a"), None)
        if link is not None:
            ends = next(row.find(cls="pos-ends"), None)
            result[native_id(link.attrs.get("href", ""))] = (clean(town.text()), clean(ends.text()) if ends else "")
    return result


def xml_value(item, name):
    return next((e.text or "" for e in item if e.tag.rsplit("}", 1)[-1] == name), "")


def discover_feed(source, client, max_pages, page_size):
    metadata = {}
    board = None
    if source.get("metadata") == "varbi" or source.get("delegated_links"):
        board = client.get(source["url"])
    if source.get("metadata") == "varbi":
        metadata = varbi_metadata(board)
        if not metadata:
            raise SourceError("Varbi location table is missing; refusing to infer city from employer name")
    jobs = {}
    feed_url = source["feed_url"]
    visited = set()
    for _ in range(max_pages):
        if feed_url in visited:
            raise SourceError("Repeated RSS pagination URL")
        visited.add(feed_url)
        try:
            root = ET.fromstring(client.get(feed_url))
        except ET.ParseError as exc:
            raise SourceError("Expected an RSS feed, not an error or HTML page") from exc
        channel = root.find("channel") if root.tag == "rss" else None
        if channel is None:
            raise SourceError("Expected RSS channel")
        for item in channel.findall("item"):
            url = xml_value(item, "link").strip()
            validate_url(url, source["hosts"])
            key = native_id(url, xml_value(item, "guid"))
            location = clean(xml_value(item, "location"))
            cities = [clean(el.text) for el in item.iter() if el.tag.rsplit("}", 1)[-1] == "city" and el.text]
            if cities:
                location = "; ".join(dict.fromkeys(cities))
            deadline = xml_value(item, "expiration_date")
            if source.get("metadata") == "varbi":
                location, deadline = metadata.get(key, ("", ""))
            jobs[canonical_url(url)] = RawJob(key, url, clean(xml_value(item, "title")), location,
                                              xml_value(item, "description"), iso_date(xml_value(item, "pubDate")),
                                              deadline, "rss")
        next_link = next((e.attrib.get("href") for e in channel
                          if e.tag.rsplit("}", 1)[-1] == "link" and e.attrib.get("rel") == "next"), None)
        if not next_link:
            break
        feed_url = urljoin(feed_url, next_link)
    else:
        raise SourceError("RSS pagination cap reached; coverage is incomplete")
    if source.get("delegated_links"):
        for a in Document(board).root.find("a"):
            url = urljoin(source["url"], a.attrs.get("href", ""))
            if re.search(r"/jobs/\d+", url):
                validate_url(url, source["hosts"])
                jobs.setdefault(canonical_url(url), RawJob(native_id(url), url, clean(a.text()), method="delegated-jsonld"))
    return list(jobs.values())


def discover_workday(source, client, max_pages, page_size):
    base = source["api_url"]
    bootstrap = json.loads(client.get(base + "/jobs", {"limit": 1, "offset": 0, "searchText": "", "appliedFacets": {}}))
    if not isinstance(bootstrap, dict):
        raise SourceError("Workday bootstrap schema changed")
    def location_facets(facets):
        for facet in facets:
            values = facet.get("values", [])
            if facet.get("facetParameter") == "locations":
                ids = [v["id"] for v in values if v.get("id") and re.search(r"\b" + re.escape(source.get("location", "Lund")) + r"\b", v.get("descriptor", ""), re.I)]
                if ids:
                    return {"locations": ids}
            nested = location_facets([v for v in values if "facetParameter" in v])
            if nested:
                return nested
        return {}
    facets = location_facets(bootstrap.get("facets", []))
    if not facets:
        raise SourceError("Workday Lund location facet missing; keyword search is not a geography filter")
    jobs = {}
    offset = 0
    for _ in range(max_pages):
        data = json.loads(client.get(base + "/jobs", {"limit": min(page_size, 20), "offset": offset,
                                                      "searchText": "", "appliedFacets": facets}))
        if not isinstance(data, dict) or not isinstance(data.get("total"), int) or not isinstance(data.get("jobPostings"), list):
            raise SourceError("Workday list schema changed")
        page = data["jobPostings"]
        if not page and offset < data["total"]:
            raise SourceError("Workday returned an empty page before its advertised total")
        old_count = len(jobs)
        for item in page:
            path = item.get("externalPath", "")
            if not path.startswith("/job/"):
                raise SourceError("Workday external job path is missing")
            url = source["url"].rstrip("/") + path
            jobs[path] = RawJob(native_id(url), url, clean(item.get("title")), clean(item.get("locationsText")), method="workday")
        if len(jobs) - old_count != len(page):
            raise SourceError("Workday repeated job IDs across pagination; coverage is incomplete")
        offset += len(page)
        if offset >= data["total"]:
            return list(jobs.values())
        if len(jobs) == old_count:
            raise SourceError("Workday repeated a page without progress")
    raise SourceError("Workday pagination cap reached; coverage is incomplete")


def discover_reachmee(source, client, max_pages, page_size):
    html = client.get(source["url"])
    scripts = [urljoin(source["url"], n.attrs.get("src", "")) for n in Document(html).root.find("script")
               if re.search(r"/customerjs/I\d+-\d+-\d+\.js", n.attrs.get("src", ""))]
    if len(scripts) != 1:
        raise SourceError("ReachMee bootstrap script missing or ambiguous")
    script = client.get(scripts[0])
    def variable(name):
        match = re.search(r"\bvar\s+" + re.escape(name) + r"\s*=\s*['\"]([^'\"]+)['\"]", script)
        if not match:
            raise SourceError(f"ReachMee bootstrap field missing: {name}")
        return match.group(1)
    base = variable("iFrameUrl").rstrip("/") + "/" + variable("iid") + "/" + variable("customer") + "/"
    url = base + "main?" + urlencode({"site": variable("site"), "validator": variable("validator"), "lang": variable("langDef")})
    board = Document(client.get(url)).root
    if next(board.find("table", id="jobsTable"), None) is None:
        raise SourceError("ReachMee listing container is missing")
    jobs = []
    for row in board.find("tr", cls="jobs"):
        a = next(row.find("a"), None)
        if a is None or "job_id" not in parse_qs(urlsplit(a.attrs.get("href", "")).query):
            raise SourceError("ReachMee job row has no detail URL")
        detail_url = urljoin(url, a.attrs["href"])
        country = next(row.find(cls="jobcountry"), None)
        deadline = next(row.find(cls="publishto"), None)
        order = next(deadline.find("span"), None) if deadline else None
        jobs.append(RawJob(native_id(detail_url), detail_url, clean(a.text()),
                           clean(country.text()) if country else "",
                           valid_through=clean(order.text()) if order else "", method="reachmee"))
    return jobs


DISCOVERERS = {"feed": discover_feed, "workday": discover_workday, "reachmee": discover_reachmee}


def complete_detail(raw, source, client):
    if raw.method == "workday":
        path = urlsplit(raw.url).path.split("/job/", 1)[1]
        data = json.loads(client.get(source["api_url"] + "/job/" + path))
        info = data.get("jobPostingInfo", {}) if isinstance(data, dict) else {}
        if not isinstance(info, dict) or not info.get("jobReqId") or not info.get("jobDescription"):
            raise SourceError("Workday detail schema changed")
        locations = [clean(info.get("location"))]
        extra = info.get("additionalLocations", []) or []
        for location in extra:
            locations.append(clean(location.get("descriptor", "") if isinstance(location, dict) else location))
        location = "; ".join(dict.fromkeys(x for x in locations if x)) or raw.location
        return RawJob(info["jobReqId"], raw.url, clean(info.get("title")), location,
                      info["jobDescription"], iso_date(info.get("startDate")), info.get("endDate", ""),
                      "workday-json", info.get("canApply") is not False)
    if not raw.description:
        html = client.get(raw.url)
        if source["adapter"] == "reachmee":
            doc = Document(html).root
            title = next(doc.find("h1"), None)
            description = next(doc.find(cls="jobDescription"), None)
            if title is None or description is None:
                raise SourceError("ReachMee title or description container missing")
            raw.title = clean(title.text())
            raw.description = description.html()
            raw.method = "reachmee-html"
            # Only an explicit workplace sentence may supplement country-only metadata.
            # A general mention of ESS's Lund facility is deliberately insufficient.
            if raw.location in {"", "Sweden", "Sverige"}:
                body = plain_text(raw.description)
                patterns = [
                    r"\b(?:position|role|workplace)\b[^.!?\n]{0,45}?\b(?:based|located|situated)\s+in\s+Lund\b",
                    r"\byou\s+will\s+be\s+based\s+in\s+Lund\b",
                    r"\b(?:tjänsten|rollen|arbetsplatsen)\b[^.!?\n]{0,35}?\b(?:placerad|belägen)\s+i\s+Lund\b",
                ]
                for pattern in patterns:
                    match = re.search(pattern, body, re.I)
                    if match and not re.search(r"\b(?:not|never|inte)\b", match.group(), re.I):
                        raw.location = "Lund, Sweden"
                        raw.location_method = "explicit_workplace_sentence"
                        break
            return raw
        return jsonld_job(html, raw.url)
    return raw


def normalize(raw, source, matching_config):
    fragment = description_fragment(raw.description)
    description = plain_text(fragment)
    if not raw.title or len(description) < 40:
        raise SourceError("Missing title or substantive full job description")
    requirements = extract_requirements_text_from_fragment(fragment)
    job = JobDetail(job_id=f"employer:{source['id']}:{raw.native_id}", title=clean(raw.title),
                    company=source["employer"], location=clean(raw.location), url=canonical_url(raw.url),
                    posted_date=iso_date(raw.posted_date), description=description,
                    source_searches=[f"employer:{source['id']}"], requirements_text=requirements or description,
                    requirements_extraction_method="pattern" if requirements else "full_description")
    payload = asdict(apply_prefilter_metadata(job, matching_config.prefilter))
    payload.update(source="employer", source_id=source["id"], source_job_id=raw.native_id,
                   extraction_method=raw.method, indexed_at=datetime.now(timezone.utc).isoformat(),
                   valid_through=raw.valid_through, location_extraction_method=raw.location_method)
    return payload


def load_sources(path=CONFIG_PATH):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    sources = data.get("sources")
    if data.get("version") != 1 or not isinstance(sources, list) or not sources:
        raise SourceError("Expected version 1 source configuration with a nonempty sources array")
    ids = set()
    for source in sources:
        if not re.fullmatch(r"[a-z0-9-]+", source.get("id", "")) or source["id"] in ids:
            raise SourceError("Invalid or duplicate source ID")
        ids.add(source["id"])
        if source.get("adapter") not in DISCOVERERS or not source.get("employer") or not source.get("hosts"):
            raise SourceError("Invalid adapter, employer or allowed hosts")
        for key in ("url", "feed_url" if source["adapter"] == "feed" else "api_url" if source["adapter"] == "workday" else "url"):
            validate_url(source.get(key, ""), source["hosts"])
    return sources


def seen_urls(results_root):
    urls = set()
    def walk(value):
        if isinstance(value, dict):
            if value.get("job_id") and isinstance(value.get("url"), str):
                urls.add(canonical_url(value["url"]))
            for child in value.values():
                if isinstance(child, (dict, list)):
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    for path in Path(results_root).rglob("*.json"):
        try:
            walk(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    return urls


def index_sources(sources, results_root, matching_config, limit=25, max_pages=20, page_size=20,
                  delay=1, timeout=30, client_factory=Client, today=None):
    seen = load_seen_job_ids(Path(results_root))
    urls = seen_urls(Path(results_root))
    jobs, audits = [], []
    for source in sources:
        client = client_factory(source["hosts"], delay, timeout)
        audit = dict(source_id=source["id"], employer=source["employer"], status="ok", discovered=0,
                     indexed=0, already_seen=0, location_filtered=0, location_unknown=0, expired=0,
                     selection_truncated=False, errors=[], location_review=[])
        try:
            raw_jobs = DISCOVERERS[source["adapter"]](source, client, max_pages, page_size)
            audit["discovered"] = len(raw_jobs)
            for raw in raw_jobs:
                if audit["indexed"] >= limit:
                    audit["selection_truncated"] = True
                    break
                validate_url(raw.url, source["hosts"])
                # Workday's stable requisition ID is available on the detail response.
                if canonical_url(raw.url) in urls or f"employer:{source['id']}:{raw.native_id}" in seen:
                    audit["already_seen"] += 1
                    continue
                if (raw.method != "workday" and raw.location and raw.location not in {"Sweden", "Sverige"}
                        and not re.search(r"\bLund\b", raw.location, re.I)):
                    audit["location_filtered"] += 1
                    continue
                try:
                    raw = complete_detail(raw, source, client)
                    if not raw.location or raw.location in {"Sweden", "Sverige"}:
                        audit["location_unknown"] += 1
                        audit["location_review"].append({"url": raw.url, "title": raw.title, "reported_location": raw.location})
                        continue
                    if not re.search(r"\bLund\b", raw.location, re.I):
                        audit["location_filtered"] += 1
                        continue
                    if not raw.active or expired(raw.valid_through, today):
                        audit["expired"] += 1
                        continue
                    payload = normalize(raw, source, matching_config)
                    if payload["job_id"] in seen or payload["url"] in urls:
                        audit["already_seen"] += 1
                        continue
                    seen.add(payload["job_id"])
                    urls.add(payload["url"])
                    jobs.append(payload)
                    audit["indexed"] += 1
                except (SourceError, ValueError, OSError, KeyError, TypeError) as exc:
                    audit["errors"].append({"url": raw.url, "message": str(exc)})
            if audit["errors"] or audit["location_unknown"]:
                audit["status"] = "partial"
        except (SourceError, ValueError, OSError, KeyError, TypeError) as exc:
            audit["status"] = "failed"
            audit["errors"].append({"url": source["url"], "message": str(exc)})
        audit["requests"] = client.requests
        audits.append(audit)
        print(f"[{source['id']}] {audit['status']}: discovered={audit['discovered']} indexed={audit['indexed']} "
              f"seen={audit['already_seen']} other_location={audit['location_filtered']} unknown_location={audit['location_unknown']} errors={len(audit['errors'])}", flush=True)
    return jobs, audits


def write_results(jobs, audits, results_root, bucket="waiting_room"):
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    folder = Path(results_root) / bucket / stamp
    folder.mkdir(parents=True, exist_ok=False)
    path = folder / "employer_jobs.json"
    path.write_text(json.dumps(jobs, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    metadata = {"indexed_at": datetime.now(timezone.utc).isoformat(), "job_count": len(jobs), "sources": audits}
    path.with_name("employer_jobs_metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--source", action="append", help="Source ID; repeat to select several, default all five")
    parser.add_argument("--results-root", type=Path, default=Path("results"))
    parser.add_argument("--results-bucket", choices=("waiting_room", "discard"), default="waiting_room")
    parser.add_argument("--matching-config", type=Path)
    parser.add_argument("--limit", type=int, default=25, help="Maximum new Lund jobs per source per run")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--request-delay", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)
    if min(args.limit, args.max_pages, args.page_size) <= 0 or args.request_delay < 0 or args.timeout <= 0:
        parser.error("Limits, page size and timeout must be positive; request delay cannot be negative")
    sources = load_sources(args.config)
    if args.source:
        missing = set(args.source) - {s["id"] for s in sources}
        if missing:
            parser.error("Unknown source IDs: " + ", ".join(sorted(missing)))
        sources = [s for s in sources if s["id"] in args.source]
    config_path = args.matching_config
    if config_path is None and DEFAULT_MATCHING_CONFIG_PATH.exists():
        config_path = DEFAULT_MATCHING_CONFIG_PATH
    jobs, audits = index_sources(sources, args.results_root, load_matching_config(config_path), args.limit,
                                 args.max_pages, args.page_size, args.request_delay, args.timeout)
    path = write_results(jobs, audits, args.results_root, args.results_bucket)
    print(f"Wrote {path}")
    return 1 if any(a["status"] != "ok" for a in audits) else 0


if __name__ == "__main__":
    raise SystemExit(main())
