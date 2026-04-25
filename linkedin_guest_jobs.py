#!/usr/bin/env python3
"""Fetch public LinkedIn job data without Selenium or interactive login."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse
from urllib.request import Request, urlopen


LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs/search-results/"
GUEST_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
GUEST_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
DEFAULT_INPUT_PATH = Path("linkedin_search_input.json")
DEFAULT_RESULTS_ROOT = Path("results")
DEFAULT_RESULTS_BUCKET = "discard"
DEFAULT_OUTPUT_NAME = "linkedin_jobs_sample.json"
DEFAULT_MAX_SEARCH_PAGES = 10
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


@dataclass
class JobDetail(JobCard):
    description: str = ""


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value)).strip()


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

    if "keywords" not in params and "location" not in params:
        raise ValueError("Input JSON must contain search_url, keywords/search_terms, or location")

    return f"{LINKEDIN_SEARCH_URL}?{urlencode(params)}"


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


def collect_unseen_cards(
    search_url: str,
    limit: int,
    seen_job_ids: set[str],
    max_pages: int = DEFAULT_MAX_SEARCH_PAGES,
    fetch_html: Callable[[str], str] = fetch,
) -> list[JobCard]:
    if limit <= 0:
        return []
    selected: list[JobCard] = []
    start = 0
    for _ in range(max_pages):
        page_cards = parse_search_results(fetch_html(guest_search_url(search_url, start=start)))
        if not page_cards:
            break
        selected.extend(select_unseen_cards(page_cards, seen_job_ids, limit - len(selected)))
        if len(selected) >= limit:
            break
        start += len(page_cards)
    return selected


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


def fetch_job_details(cards: Iterable[JobCard]) -> list[JobDetail]:
    jobs: list[JobDetail] = []
    for card in cards:
        detail_html = fetch(GUEST_DETAIL_URL.format(job_id=card.job_id))
        jobs.append(parse_job_detail(detail_html, card))
    return jobs


def timestamped_output_path(output_name: str, results_root: Path = DEFAULT_RESULTS_ROOT) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return results_root / DEFAULT_RESULTS_BUCKET / timestamp / Path(output_name).name


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH, help="Search input JSON path")
    parser.add_argument("--limit", type=int, default=2, help="Number of new jobs to fetch")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_NAME, help="JSON output filename")
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT, help="Results memory root")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_SEARCH_PAGES, help="Maximum search pages to scan")
    args = parser.parse_args()

    search_url = search_url_from_config(load_search_config(args.input))
    seen_job_ids = load_seen_job_ids(args.results_root)
    cards = collect_unseen_cards(search_url, args.limit, seen_job_ids, max_pages=args.max_pages)
    if not cards:
        print("No new public job cards found.", file=sys.stderr)
        return 1

    jobs = fetch_job_details(cards)
    payload = [asdict(job) for job in jobs]
    output_path = timestamped_output_path(args.output, results_root=args.results_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
