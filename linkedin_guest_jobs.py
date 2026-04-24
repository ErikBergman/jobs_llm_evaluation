#!/usr/bin/env python3
"""Fetch public LinkedIn job data without Selenium or interactive login."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse
from urllib.request import Request, urlopen


DEFAULT_SEARCH_URL = (
    "https://www.linkedin.com/jobs/search-results/?currentJobId=&"
    "keywords=engineer%20in%20lund&origin=JOB_SEARCH_PAGE_JOB_FILTER&"
    "referralSearchId=nIEYkI%2BL42AY3hrGVYz2Jg%3D%3D&geoId=105734258&"
    "distance=0.0&f_TPR=r86400"
)

GUEST_SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"


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


def main() -> int:
    print(fetch(guest_search_url(DEFAULT_SEARCH_URL))[:1000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
