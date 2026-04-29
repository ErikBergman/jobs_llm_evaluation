#!/usr/bin/env python3
"""Fetch public LinkedIn job data without Selenium or interactive login."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlencode, urlparse
from urllib.request import Request, urlopen


LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs/search-results/"
GUEST_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
GUEST_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
DEFAULT_INPUT_PATH = Path("linkedin_search_input.json")
DEFAULT_RESULTS_ROOT = Path("results")
DEFAULT_RESULTS_BUCKET = "discard"
WAITING_ROOM_BUCKET = "waiting_room"
DEFAULT_OUTPUT_NAME = "linkedin_jobs_sample.json"
DEFAULT_MAX_SEARCH_PAGES = 2
DEFAULT_REQUEST_DELAY_SECONDS = 0.0
DEFAULT_CHEAT_AD_NAME = "cheat_mode_job_ad.rtf"
CHEAT_JOB_ID = "cheat-mode-perfect-job"
RTF_DESTINATIONS = {
    "fonttbl",
    "colortbl",
    "datastore",
    "stylesheet",
    "info",
    "pict",
    "object",
    "header",
    "footer",
    "footnote",
}
SEARCH_FIELD_ALIASES = {
    "keywords": ("keywords", "search_terms", "query"),
    "location": ("location",),
    "geoId": ("geoId", "geo_id"),
    "distance": ("distance",),
    "f_TPR": ("f_TPR", "date_posted"),
    "f_E": ("f_E", "experience_levels"),
    "f_JT": ("f_JT", "job_types"),
    "f_WT": ("f_WT", "work_types"),
    "f_AL": ("f_AL", "easy_apply"),
    "f_EA": ("f_EA", "easy_apply_alt"),
    "f_JIYN": ("f_JIYN", "in_network"),
    "f_VJ": ("f_VJ", "verified_jobs"),
    "f_C": ("f_C", "company_ids"),
    "f_PP": ("f_PP", "place_ids"),
    "sortBy": ("sortBy", "sort_by"),
}


@dataclass
class JobCard:
    job_id: str
    title: str = ""
    company: str = ""
    company_url: str = ""
    location: str = ""
    benefit: str = ""
    posted: str = ""
    posted_date: str = ""
    url: str = ""
    source_searches: list[str] = field(default_factory=list)


@dataclass
class JobDetail(JobCard):
    description: str = ""


@dataclass
class SearchAudit:
    search: str
    pages_requested: int = 0
    results_seen: int = 0
    throttled: bool = False


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


def rtf_to_text(rtf: str) -> str:
    output: list[str] = []
    ignored_groups = [False]
    pending_ignorable_destination = False
    unicode_skip_count = 1
    chars_to_skip = 0
    index = 0

    while index < len(rtf):
        char = rtf[index]
        if chars_to_skip:
            chars_to_skip -= 1
            index += 1
            continue

        if char == "{":
            ignored_groups.append(ignored_groups[-1])
            pending_ignorable_destination = False
            index += 1
            continue
        if char == "}":
            if len(ignored_groups) > 1:
                ignored_groups.pop()
            pending_ignorable_destination = False
            index += 1
            continue
        if char != "\\":
            if not ignored_groups[-1]:
                output.append(char)
            index += 1
            continue

        index += 1
        if index >= len(rtf):
            break

        escaped = rtf[index]
        if escaped in "\r\n":
            if not ignored_groups[-1]:
                output.append("\n")
            index += 1
            if escaped == "\r" and index < len(rtf) and rtf[index] == "\n":
                index += 1
            continue
        if escaped in "\\{}":
            if not ignored_groups[-1]:
                output.append(escaped)
            index += 1
            continue
        if escaped == "'":
            hex_value = rtf[index + 1 : index + 3]
            if len(hex_value) == 2:
                try:
                    if not ignored_groups[-1]:
                        output.append(bytes.fromhex(hex_value).decode("latin-1"))
                    index += 3
                    continue
                except ValueError:
                    pass
        if escaped == "~":
            if not ignored_groups[-1]:
                output.append(" ")
            index += 1
            continue
        if escaped in "-_":
            index += 1
            continue
        if escaped == "*":
            ignored_groups[-1] = True
            pending_ignorable_destination = True
            index += 1
            continue
        if not escaped.isalpha():
            index += 1
            continue

        word_start = index
        while index < len(rtf) and rtf[index].isalpha():
            index += 1
        word = rtf[word_start:index]
        sign = 1
        if index < len(rtf) and rtf[index] == "-":
            sign = -1
            index += 1
        number_start = index
        while index < len(rtf) and rtf[index].isdigit():
            index += 1
        number = rtf[number_start:index]
        numeric_value = sign * int(number) if number else None
        if index < len(rtf) and rtf[index] == " ":
            index += 1

        if pending_ignorable_destination or word in RTF_DESTINATIONS:
            ignored_groups[-1] = True
            pending_ignorable_destination = False
            continue
        pending_ignorable_destination = False

        if ignored_groups[-1]:
            continue
        if word in ("par", "line"):
            output.append("\n")
        elif word == "tab":
            output.append("\t")
        elif word == "uc" and numeric_value is not None:
            unicode_skip_count = max(numeric_value, 0)
        elif word == "u" and numeric_value is not None:
            output.append(chr(numeric_value if numeric_value >= 0 else numeric_value + 65536))
            chars_to_skip = unicode_skip_count

    return "".join(output)


def env_flag_enabled(name: str, environ: dict[str, str] | None = None) -> bool:
    value = (os.environ if environ is None else environ).get(name, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def class_contains(attrs: dict[str, str], class_name: str) -> bool:
    return class_name in attrs.get("class", "").split()


class SearchResultsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.cards: list[JobCard] = []
        self._card: JobCard | None = None
        self._depth = 0
        self._field: str | None = None
        self._field_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        attrs = {name: value or "" for name, value in attrs_list}

        if self._card is None:
            urn = attrs.get("data-entity-urn", "")
            if tag == "div" and class_contains(attrs, "base-card") and "jobPosting:" in urn:
                self._card = JobCard(job_id=urn.rsplit(":", 1)[-1])
                self._depth = 1
            return

        self._depth += 1
        if tag == "a" and class_contains(attrs, "base-card__full-link"):
            self._card.url = unescape(attrs.get("href", "")).strip()
            self._start_field("title")
        elif tag == "h3" and class_contains(attrs, "base-search-card__title"):
            self._start_field("title")
        elif tag == "a" and class_contains(attrs, "hidden-nested-link"):
            self._card.company_url = unescape(attrs.get("href", "")).strip()
            self._start_field("company")
        elif tag == "span" and class_contains(attrs, "job-search-card__location"):
            self._start_field("location")
        elif tag == "span" and class_contains(attrs, "job-posting-benefits__text"):
            self._start_field("benefit")
        elif tag == "time" and "job-search-card__listdate" in attrs.get("class", ""):
            self._card.posted_date = attrs.get("datetime", "")
            self._start_field("posted")

    def handle_endtag(self, tag: str) -> None:
        if self._card is None:
            return

        if self._field and tag in {"a", "h3", "span", "time"}:
            setattr(self._card, self._field, clean_text("".join(self._field_chunks)))
            self._field = None
            self._field_chunks = []

        self._depth -= 1
        if self._depth == 0:
            self.cards.append(self._card)
            self._card = None

    def handle_data(self, data: str) -> None:
        if self._field:
            self._field_chunks.append(data)

    def _start_field(self, field: str) -> None:
        if not self._field:
            self._field = field
            self._field_chunks = []


class JobDetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fields: dict[str, str] = {}
        self._field: str | None = None
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        attrs = {name: value or "" for name, value in attrs_list}

        if tag == "h2" and class_contains(attrs, "top-card-layout__title"):
            self._start_field("title")
        elif tag == "a" and class_contains(attrs, "topcard__org-name-link"):
            self._start_field("company")
        elif tag == "span" and class_contains(attrs, "posted-time-ago__text"):
            self._start_field("posted")

    def handle_endtag(self, tag: str) -> None:
        if self._field and tag in {"h2", "a", "span"}:
            self._finish_field()

    def handle_data(self, data: str) -> None:
        if self._field:
            self._chunks.append(data)

    def _start_field(self, field: str) -> None:
        if not self._field:
            self._field = field
            self._chunks = []

    def _finish_field(self) -> None:
        if self._field:
            self.fields[self._field] = clean_text("".join(self._chunks))
        self._field = None
        self._chunks = []


class TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "p", "li", "ul", "ol"}:
            self.chunks.append("\n")

    def handle_data(self, data: str) -> None:
        self.chunks.append(data)

    def text(self) -> str:
        return clean_text("".join(self.chunks))


def strip_json_comments(text: str) -> str:
    output: list[str] = []
    index = 0
    in_string = False
    escape = False
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_string:
            output.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            output.append(char)
            index += 1
        elif char == "/" and next_char == "/":
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
        elif char == "/" and next_char == "*":
            index += 2
            while index + 1 < len(text) and not (text[index] == "*" and text[index + 1] == "/"):
                index += 1
            index += 2
        else:
            output.append(char)
            index += 1
    return "".join(output)


def strip_json_trailing_commas(text: str) -> str:
    output: list[str] = []
    index = 0
    in_string = False
    escape = False
    while index < len(text):
        char = text[index]

        if in_string:
            output.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            output.append(char)
            index += 1
        elif char == ",":
            lookahead = index + 1
            while lookahead < len(text) and text[lookahead].isspace():
                lookahead += 1
            if lookahead < len(text) and text[lookahead] in "}]":
                index += 1
            else:
                output.append(char)
                index += 1
        else:
            output.append(char)
            index += 1
    return "".join(output)


def load_search_config(input_path: Path) -> dict[str, object]:
    with open(input_path, encoding="utf-8") as input_file:
        data = json.loads(strip_json_trailing_commas(strip_json_comments(input_file.read())))
    if not isinstance(data, dict):
        raise ValueError(f"{input_path} must contain a JSON object")
    return data


def first_config_value(config: dict[str, object], aliases: tuple[str, ...]) -> object | None:
    for alias in aliases:
        value = config.get(alias)
        if value not in (None, "", []):
            return value
    return None


def normalize_query_value(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)


def search_url_from_config(config: dict[str, object]) -> str:
    search_url = config.get("search_url")
    if isinstance(search_url, str) and search_url.strip():
        return search_url.strip()

    params: dict[str, str] = {"origin": "JOB_SEARCH_PAGE_JOB_FILTER"}
    for linked_in_key, aliases in SEARCH_FIELD_ALIASES.items():
        value = first_config_value(config, aliases)
        if value not in (None, "", []):
            params[linked_in_key] = normalize_query_value(value)

    extra_params = config.get("extra_params")
    if isinstance(extra_params, dict):
        for key, value in extra_params.items():
            if value not in (None, "", []):
                params[str(key)] = normalize_query_value(value)

    if not any(key in params for key in ("keywords", "location", "geoId", "f_PP")):
        raise ValueError("Input JSON must contain search_url, keywords/search_terms, location, geo_id, or place_ids")

    return f"{LINKEDIN_SEARCH_URL}?{urlencode(params)}"


def search_configs_from_config(config: dict[str, object]) -> list[dict[str, object]]:
    searches = config.get("searches")
    if searches in (None, "", []):
        return [config]
    if not isinstance(searches, list):
        raise ValueError("searches must be a list of search config objects")
    if not searches:
        raise ValueError("searches must contain at least one search config")

    configs: list[dict[str, object]] = []
    for index, search_config in enumerate(searches, start=1):
        if not isinstance(search_config, dict):
            raise ValueError(f"searches[{index}] must be a search config object")
        configs.append(search_config)
    return configs


def search_urls_from_config(config: dict[str, object]) -> list[str]:
    return [search_url_from_config(search_config) for search_config in search_configs_from_config(config)]


def guest_search_url(search_url: str, start: int = 0) -> str:
    query = dict(parse_qsl(urlparse(search_url).query, keep_blank_values=True))
    allowed_keys = {"start", *SEARCH_FIELD_ALIASES}
    params = {key: value for key, value in query.items() if key in allowed_keys and value}
    params["start"] = str(start)
    return f"{GUEST_SEARCH_URL}?{urlencode(params)}"


def fetch(url: str) -> str:
    request = Request(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        },
    )
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", "replace")


def sleep_before_request(delay_seconds: float) -> None:
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def parse_search_results(html: str) -> list[JobCard]:
    parser = SearchResultsParser()
    parser.feed(html)
    return parser.cards


def job_ids_from_payload(payload: object) -> set[str]:
    job_ids: set[str] = set()
    if isinstance(payload, dict):
        job_id = payload.get("job_id")
        if job_id not in (None, ""):
            job_ids.add(str(job_id))
        for value in payload.values():
            job_ids.update(job_ids_from_payload(value))
    elif isinstance(payload, list):
        for item in payload:
            job_ids.update(job_ids_from_payload(item))
    return job_ids


def load_seen_job_ids(results_root: Path) -> set[str]:
    seen_job_ids: set[str] = set()
    if not results_root.exists():
        return seen_job_ids

    for result_path in results_root.rglob("*.json"):
        try:
            with open(result_path, encoding="utf-8") as result_file:
                seen_job_ids.update(job_ids_from_payload(json.load(result_file)))
        except (OSError, json.JSONDecodeError):
            print(f"Skipping unreadable result memory file: {result_path}", file=sys.stderr)
    return seen_job_ids


def select_unseen_cards(cards: Iterable[JobCard], seen_job_ids: set[str], limit: int) -> list[JobCard]:
    if limit <= 0:
        return []
    selected: list[JobCard] = []
    for card in cards:
        if card.job_id in seen_job_ids:
            continue
        selected.append(card)
        seen_job_ids.add(card.job_id)
        if len(selected) >= limit:
            break
    return selected


def posted_sort_key(card: JobCard) -> tuple[int, int]:
    posted = card.posted.strip().lower()
    if posted in {"just now", "now"}:
        return (0, 0)

    relative_match = re.search(r"(\d+)\s+(minute|minutes|hour|hours|day|days|week|weeks|month|months)", posted)
    if relative_match:
        value = int(relative_match.group(1))
        unit = relative_match.group(2)
        multipliers = {
            "minute": 1,
            "minutes": 1,
            "hour": 60,
            "hours": 60,
            "day": 24 * 60,
            "days": 24 * 60,
            "week": 7 * 24 * 60,
            "weeks": 7 * 24 * 60,
            "month": 30 * 24 * 60,
            "months": 30 * 24 * 60,
        }
        return (0, value * multipliers[unit])

    if card.posted_date:
        try:
            posted_day = date.fromisoformat(card.posted_date)
            days_ago = max((date.today() - posted_day).days, 0)
            return (1, days_ago)
        except ValueError:
            pass

    return (2, 0)


def sort_cards_by_latest_posted(cards: Iterable[JobCard]) -> list[JobCard]:
    return sorted(cards, key=posted_sort_key)


def search_label(search_url: str) -> str:
    params = dict(parse_qsl(urlparse(search_url).query))
    keywords = params.get("keywords", "").strip()
    if keywords:
        return keywords
    geo_id = params.get("geoId", "").strip()
    return f"geoId={geo_id}" if geo_id else search_url


def collect_cards(
    search_url: str,
    max_pages: int = DEFAULT_MAX_SEARCH_PAGES,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    fetch_html: Callable[[str], str] = fetch,
    audit: SearchAudit | None = None,
) -> list[JobCard]:
    cards: list[JobCard] = []
    start = 0
    for _ in range(max_pages):
        url = guest_search_url(search_url, start=start)
        if audit is not None:
            audit.pages_requested += 1
        try:
            sleep_before_request(request_delay_seconds)
            page_cards = parse_search_results(fetch_html(url))
        except HTTPError as error:
            if error.code == 429:
                if audit is not None:
                    audit.throttled = True
                print(f"Skipping throttled LinkedIn search page: {url}", file=sys.stderr)
                break
            raise
        if not page_cards:
            break
        if audit is not None:
            audit.results_seen += len(page_cards)
        cards.extend(page_cards)
        start += len(page_cards)
    return cards


def collect_unseen_cards(
    search_url: str,
    limit: int,
    seen_job_ids: set[str],
    max_pages: int = DEFAULT_MAX_SEARCH_PAGES,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    fetch_html: Callable[[str], str] = fetch,
) -> list[JobCard]:
    if limit <= 0:
        return []
    cards = collect_cards(
        search_url,
        max_pages=max_pages,
        request_delay_seconds=request_delay_seconds,
        fetch_html=fetch_html,
    )
    return select_unseen_cards(sort_cards_by_latest_posted(cards), seen_job_ids, limit)


def collect_unseen_cards_from_search_urls(
    search_urls: Iterable[str],
    limit: int,
    seen_job_ids: set[str],
    max_pages: int = DEFAULT_MAX_SEARCH_PAGES,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    fetch_html: Callable[[str], str] = fetch,
    audits: list[SearchAudit] | None = None,
) -> list[JobCard]:
    if limit <= 0:
        return []

    cards_by_id: dict[str, JobCard] = {}
    for search_url in search_urls:
        label = search_label(search_url)
        audit = SearchAudit(search=label)
        if audits is not None:
            audits.append(audit)
        for card in collect_cards(
            search_url,
            max_pages=max_pages,
            request_delay_seconds=request_delay_seconds,
            fetch_html=fetch_html,
            audit=audit,
        ):
            if card.job_id in cards_by_id:
                if label not in cards_by_id[card.job_id].source_searches:
                    cards_by_id[card.job_id].source_searches.append(label)
                continue
            card_with_source = copy.copy(card)
            card_with_source.source_searches = [label]
            cards_by_id[card.job_id] = card_with_source
    return select_unseen_cards(sort_cards_by_latest_posted(cards_by_id.values()), seen_job_ids, limit)


def format_search_audit_table(audits: Iterable[SearchAudit]) -> str:
    rows = [
        (audit.search, str(audit.pages_requested), str(audit.results_seen), "yes" if audit.throttled else "no")
        for audit in audits
    ]
    headers = ("Search", "Pages", "Results looked at", "Throttled")
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows)) if rows else len(headers[index])
        for index in range(len(headers))
    ]

    def border() -> str:
        return "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def render_row(values: tuple[str, str, str, str]) -> str:
        return "| " + " | ".join(value.ljust(widths[index]) for index, value in enumerate(values)) + " |"

    lines = ["Search audit", border(), render_row(headers), border()]
    lines.extend(render_row(row) for row in rows)
    lines.append(border())
    return "\n".join(lines)


def extract_description(html: str) -> str:
    class_match = re.search(r'<div\b[^>]*class="[^"]*\bshow-more-less-html__markup\b[^"]*"[^>]*>', html)
    if not class_match:
        return ""

    void_tags = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
    depth = 1
    content_start = class_match.end()
    for tag_match in re.finditer(r"<\s*(/)?\s*([a-zA-Z0-9:-]+)\b[^>]*?>", html[content_start:]):
        tag_text = tag_match.group(0)
        tag_name = tag_match.group(2).lower()
        is_end = bool(tag_match.group(1))
        is_self_closing = tag_text.rstrip().endswith("/>") or tag_name in void_tags

        if is_end:
            depth -= 1
        elif not is_self_closing:
            depth += 1

        if depth == 0:
            fragment = html[content_start : content_start + tag_match.start()]
            parser = TextParser()
            parser.feed(fragment)
            return parser.text()
    return ""


def parse_job_detail(html: str, card: JobCard) -> JobDetail:
    parser = JobDetailParser()
    parser.feed(html)
    payload = asdict(card)
    payload.update(
        {
            "title": parser.fields.get("title", card.title),
            "company": parser.fields.get("company", card.company),
            "posted": parser.fields.get("posted", card.posted),
            "description": extract_description(html),
        }
    )
    return JobDetail(**payload)


def parse_cheat_job_ad(text: str) -> JobDetail:
    labels = {
        "Job Title": "title",
        "Company": "company",
        "Location": "location",
        "Benefit": "benefit",
        "Posted": "posted",
        "Job Description": "description",
    }
    fields: dict[str, list[str]] = {field: [] for field in labels.values()}
    current_field: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line in labels:
            current_field = labels[line]
            continue
        if current_field and line:
            fields[current_field].append(line)

    title = clean_text(" ".join(fields["title"]))
    description = "\n".join(fields["description"]).strip()
    if not title or not description:
        raise ValueError("Cheat-mode job ad must include 'Job Title' and 'Job Description' sections")

    return JobDetail(
        job_id=CHEAT_JOB_ID,
        title=title,
        company=clean_text(" ".join(fields["company"])),
        location=clean_text(" ".join(fields["location"])),
        benefit=clean_text(" ".join(fields["benefit"])),
        posted=clean_text(" ".join(fields["posted"])) or "Today",
        posted_date=datetime.now().strftime("%Y-%m-%d"),
        url="cheat-mode://perfect-job-ad",
        description=description,
    )


def load_cheat_job_ad(path: Path) -> JobDetail:
    if not path.exists():
        raise ValueError(f"Cheat mode is enabled but the cheat job ad was not found: {path}")
    raw_bytes = path.read_bytes()
    try:
        raw_ad = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raw_ad = raw_bytes.decode("latin-1")
    text = rtf_to_text(raw_ad).strip() if raw_ad.lstrip().startswith("{\\rtf") else raw_ad.strip()
    if not text:
        raise ValueError(f"Cheat-mode job ad is empty: {path}")
    return parse_cheat_job_ad(text)


def cheat_ad_path(results_root: Path, explicit_path: Path | None = None) -> Path:
    if explicit_path is not None:
        return explicit_path
    env_path = os.environ.get("CHEAT_MODE_JOB_AD_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return results_root / DEFAULT_CHEAT_AD_NAME


def fetch_job_details(
    cards: Iterable[JobCard],
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
) -> list[JobDetail]:
    jobs: list[JobDetail] = []
    for card in cards:
        url = GUEST_DETAIL_URL.format(job_id=card.job_id)
        try:
            sleep_before_request(request_delay_seconds)
            detail_html = fetch(url)
        except HTTPError as error:
            if error.code == 429:
                print(f"Skipping throttled LinkedIn job detail: {url}", file=sys.stderr)
                continue
            raise
        jobs.append(parse_job_detail(detail_html, card))
    return jobs


def timestamped_output_path(
    output_name: str,
    results_root: Path = DEFAULT_RESULTS_ROOT,
    results_bucket: str = DEFAULT_RESULTS_BUCKET,
) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return results_root / results_bucket / timestamp / Path(output_name).name


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH, help="Search input JSON path")
    parser.add_argument("--limit", type=int, default=2, help="Number of new jobs to fetch")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_NAME, help="JSON output filename")
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT, help="Results memory root")
    parser.add_argument(
        "--results-bucket",
        default=DEFAULT_RESULTS_BUCKET,
        choices=(DEFAULT_RESULTS_BUCKET, WAITING_ROOM_BUCKET),
        help="Results bucket to write scraped jobs into",
    )
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_SEARCH_PAGES, help="Maximum search pages to scan")
    parser.add_argument(
        "--request-delay",
        type=float,
        default=float(os.environ.get("JOB_SEARCH_REQUEST_DELAY", DEFAULT_REQUEST_DELAY_SECONDS)),
        help="Seconds to wait before each LinkedIn request",
    )
    parser.add_argument(
        "--cheat-ad",
        type=Path,
        default=None,
        help="Optional path to cheat_mode_job_ad.rtf when CHEAT_MODE=true",
    )
    args = parser.parse_args()

    search_urls = search_urls_from_config(load_search_config(args.input))
    seen_job_ids = load_seen_job_ids(args.results_root)
    search_audits: list[SearchAudit] = []
    cards = collect_unseen_cards_from_search_urls(
        search_urls,
        args.limit,
        seen_job_ids,
        max_pages=args.max_pages,
        request_delay_seconds=args.request_delay,
        audits=search_audits,
    )
    jobs = fetch_job_details(cards, request_delay_seconds=args.request_delay)
    if env_flag_enabled("CHEAT_MODE"):
        jobs.insert(0, load_cheat_job_ad(cheat_ad_path(args.results_root, args.cheat_ad)))

    if not jobs:
        print(format_search_audit_table(search_audits))
        print("No new public job cards found.", file=sys.stderr)
        return 1

    payload = [asdict(job) for job in jobs]
    output_path = timestamped_output_path(args.output, results_root=args.results_root, results_bucket=args.results_bucket)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(format_search_audit_table(search_audits))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
