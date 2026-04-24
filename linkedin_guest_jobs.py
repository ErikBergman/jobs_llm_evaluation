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
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlparse
from urllib.request import Request, urlopen


LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs/search-results/"
GUEST_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
GUEST_DETAIL_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
DEFAULT_INPUT_PATH = Path("linkedin_search_input.json")
DEFAULT_RESULTS_ROOT = Path("results")
DEFAULT_RESULTS_BUCKET = "discard"
DEFAULT_OUTPUT_NAME = "linkedin_jobs_sample.json"
SEARCH_FIELD_ALIASES = {
    "keywords": ("keywords", "search_terms", "query"),
    "location": ("location",),
    "geoId": ("geoId", "geo_id"),
    "distance": ("distance",),
    "f_TPR": ("f_TPR", "date_posted"),
    "f_E": ("f_E", "experience_levels"),
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


def load_search_config(input_path: Path) -> dict[str, object]:
    with open(input_path, encoding="utf-8") as input_file:
        data = json.load(input_file)
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

    if "keywords" not in params and "location" not in params:
        raise ValueError("Input JSON must contain search_url, keywords/search_terms, or location")

    return f"{LINKEDIN_SEARCH_URL}?{urlencode(params)}"


def guest_search_url(search_url: str, start: int = 0) -> str:
    query = dict(parse_qsl(urlparse(search_url).query, keep_blank_values=True))
    allowed_keys = {"keywords", "geoId", "location", "distance", "f_TPR", "f_E"}
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
    parser.add_argument("--limit", type=int, default=2, help="Number of jobs to fetch")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_NAME, help="JSON output filename")
    args = parser.parse_args()

    search_url = search_url_from_config(load_search_config(args.input))
    search_html = fetch(guest_search_url(search_url))
    cards = parse_search_results(search_html)
    if not cards:
        print("No public job cards found.", file=sys.stderr)
        return 1

    jobs = fetch_job_details(cards[: args.limit])
    payload = [asdict(job) for job in jobs]
    output_path = timestamped_output_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
