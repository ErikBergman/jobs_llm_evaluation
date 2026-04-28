import io
import json
import tempfile
import unittest
from contextlib import contextmanager
from unittest import mock
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from linkedin_guest_jobs import (
    CHEAT_JOB_ID,
    GUEST_SEARCH_URL,
    JobCard,
    collect_unseen_cards_from_search_urls,
    collect_unseen_cards,
    env_flag_enabled,
    guest_search_url,
    load_cheat_job_ad,
    load_search_config,
    load_seen_job_ids,
    main,
    parse_cheat_job_ad,
    search_url_from_config,
    search_urls_from_config,
    select_unseen_cards,
    sort_cards_by_latest_posted,
)


@contextmanager
def raising_http_error(url: str, code: int, message: str):
    error = HTTPError(url, code, message, hdrs=None, fp=None)
    try:
        yield error
    finally:
        error.close()


class SearchUrlTests(unittest.TestCase):
    def test_uses_explicit_search_url_when_present(self) -> None:
        search_url = "https://www.linkedin.com/jobs/search-results/?keywords=engineer&geoId=105734258"

        self.assertEqual(search_url_from_config({"search_url": search_url, "keywords": "ignored"}), search_url)

    def test_constructs_linkedin_search_url_from_fields(self) -> None:
        search_url = search_url_from_config(
            {
                "search_terms": "engineer in lund",
                "geo_id": "105734258",
                "distance": "0.0",
                "date_posted": "r86400",
                "experience_levels": ["2", "3"],
                "job_types": ["F", "C"],
                "work_types": ["2", "3"],
                "easy_apply": True,
                "verified_jobs": True,
                "sort_by": "DD",
            }
        )

        parsed = urlparse(search_url)
        query = parse_qs(parsed.query)

        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "www.linkedin.com")
        self.assertEqual(parsed.path, "/jobs/search-results/")
        self.assertEqual(query["keywords"], ["engineer in lund"])
        self.assertEqual(query["geoId"], ["105734258"])
        self.assertEqual(query["distance"], ["0.0"])
        self.assertEqual(query["f_TPR"], ["r86400"])
        self.assertEqual(query["f_E"], ["2,3"])
        self.assertEqual(query["f_JT"], ["F,C"])
        self.assertEqual(query["f_WT"], ["2,3"])
        self.assertEqual(query["f_AL"], ["true"])
        self.assertEqual(query["f_VJ"], ["true"])
        self.assertEqual(query["sortBy"], ["DD"])

    def test_constructed_url_maps_to_guest_endpoint(self) -> None:
        search_url = search_url_from_config(
            {
                "keywords": "engineer in lund",
                "geo_id": "105734258",
                "distance": "0.0",
                "date_posted": "r86400",
            }
        )

        guest_url = guest_search_url(search_url)
        parsed = urlparse(guest_url)
        query = parse_qs(parsed.query)

        self.assertEqual(f"{parsed.scheme}://{parsed.netloc}{parsed.path}", GUEST_SEARCH_URL)
        self.assertEqual(query["keywords"], ["engineer in lund"])
        self.assertEqual(query["geoId"], ["105734258"])
        self.assertEqual(query["distance"], ["0.0"])
        self.assertEqual(query["f_TPR"], ["r86400"])
        self.assertEqual(query["start"], ["0"])

    def test_constructs_geo_only_search_url(self) -> None:
        search_url = search_url_from_config(
            {
                "geo_id": "105734258",
                "distance": "0.0",
                "date_posted": "r86400",
                "sort_by": "DD",
            }
        )

        parsed = urlparse(search_url)
        query = parse_qs(parsed.query)

        self.assertNotIn("keywords", query)
        self.assertEqual(query["geoId"], ["105734258"])
        self.assertEqual(query["distance"], ["0.0"])
        self.assertEqual(query["f_TPR"], ["r86400"])
        self.assertEqual(query["sortBy"], ["DD"])

    def test_search_urls_from_config_supports_multiple_searches(self) -> None:
        search_urls = search_urls_from_config(
            {
                "searches": [
                    {"keywords": "engineer", "geo_id": "105734258"},
                    {"geo_id": "105734258", "date_posted": "r86400"},
                ]
            }
        )

        self.assertEqual(len(search_urls), 2)
        self.assertIn("keywords=engineer", search_urls[0])
        self.assertNotIn("keywords=", search_urls[1])
        self.assertIn("geoId=105734258", search_urls[1])

    def test_loads_jsonc_template_style_comments(self) -> None:
        content = """
        {
          // Comment outside a string.
          "keywords": "engineer // this remains text",
          "geo_id": "105734258",
          /* Block comment. */
          "date_posted": "r86400",
        }
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "input.jsonc"
            input_path.write_text(content, encoding="utf-8")

            config = load_search_config(input_path)

        self.assertEqual(config["keywords"], "engineer // this remains text")
        self.assertEqual(config["geo_id"], "105734258")
        self.assertEqual(config["date_posted"], "r86400")

    def test_load_seen_job_ids_reads_nested_result_jsons(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            results_root = Path(temp_dir) / "results"
            (results_root / "discard" / "20260425_120000").mkdir(parents=True)
            (results_root / "hits" / "20260425_120000").mkdir(parents=True)
            (results_root / "discard" / "20260425_120000" / "jobs.json").write_text(
                json.dumps([{"job_id": 123}, {"nested": {"job_id": "456"}}]),
                encoding="utf-8",
            )
            (results_root / "hits" / "20260425_120000" / "jobs_hits.json").write_text(
                json.dumps([{"job_id": "789"}]),
                encoding="utf-8",
            )

            self.assertEqual(load_seen_job_ids(results_root), {"123", "456", "789"})

    def test_select_unseen_cards_does_not_count_seen_jobs_toward_limit(self) -> None:
        cards = [JobCard("seen"), JobCard("new-1"), JobCard("new-2"), JobCard("new-3")]
        seen_job_ids = {"seen"}

        selected = select_unseen_cards(cards, seen_job_ids, limit=2)

        self.assertEqual([card.job_id for card in selected], ["new-1", "new-2"])
        self.assertEqual(seen_job_ids, {"seen", "new-1", "new-2"})

    def test_sort_cards_by_latest_posted_prefers_relative_age(self) -> None:
        cards = [
            JobCard("day-old", posted="1 day ago", posted_date="2026-04-27"),
            JobCard("minutes-old", posted="22 minutes ago", posted_date="2026-04-28"),
            JobCard("hours-old", posted="18 hours ago", posted_date="2026-04-27"),
            JobCard("newer-minutes-old", posted="9 minutes ago", posted_date="2026-04-28"),
        ]

        sorted_cards = sort_cards_by_latest_posted(cards)

        self.assertEqual(
            [card.job_id for card in sorted_cards],
            ["newer-minutes-old", "minutes-old", "hours-old", "day-old"],
        )

    def test_collect_unseen_cards_sorts_page_before_applying_limit(self) -> None:
        pages = {
            "start=0": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:older">
                    <time class="job-search-card__listdate" datetime="2026-04-27">1 day ago</time>
                </div>
                <div class="base-card" data-entity-urn="urn:li:jobPosting:newer">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">9 minutes ago</time>
                </div>
            """,
        }

        def fake_fetch(url: str) -> str:
            for marker, html in pages.items():
                if marker in url:
                    return html
            return ""

        cards = collect_unseen_cards(
            "https://www.linkedin.com/jobs/search-results/?keywords=engineer",
            limit=1,
            seen_job_ids=set(),
            max_pages=1,
            fetch_html=fake_fetch,
        )

        self.assertEqual([card.job_id for card in cards], ["newer"])

    def test_collect_unseen_cards_sorts_all_pages_before_applying_limit(self) -> None:
        pages = {
            "start=0": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:older">
                    <time class="job-search-card__listdate" datetime="2026-04-27">1 day ago</time>
                </div>
            """,
            "start=1": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:newer">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">9 minutes ago</time>
                </div>
            """,
        }

        def fake_fetch(url: str) -> str:
            for marker, html in pages.items():
                if marker in url:
                    return html
            return ""

        cards = collect_unseen_cards(
            "https://www.linkedin.com/jobs/search-results/?keywords=engineer",
            limit=1,
            seen_job_ids=set(),
            max_pages=2,
            fetch_html=fake_fetch,
        )

        self.assertEqual([card.job_id for card in cards], ["newer"])

    def test_collect_unseen_cards_from_search_urls_dedupes_and_sorts_all_searches(self) -> None:
        pages = {
            "keywords=engineer": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:older">
                    <time class="job-search-card__listdate" datetime="2026-04-27">1 day ago</time>
                </div>
                <div class="base-card" data-entity-urn="urn:li:jobPosting:duplicate">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">20 minutes ago</time>
                </div>
            """,
            "geoId=105734258": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:duplicate">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">20 minutes ago</time>
                </div>
                <div class="base-card" data-entity-urn="urn:li:jobPosting:newer">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">9 minutes ago</time>
                </div>
            """,
        }

        def fake_fetch(url: str) -> str:
            for marker, html in pages.items():
                if marker in url:
                    return html
            return ""

        cards = collect_unseen_cards_from_search_urls(
            [
                "https://www.linkedin.com/jobs/search-results/?keywords=engineer",
                "https://www.linkedin.com/jobs/search-results/?geoId=105734258",
            ],
            limit=3,
            seen_job_ids=set(),
            max_pages=1,
            fetch_html=fake_fetch,
        )

        self.assertEqual([card.job_id for card in cards], ["newer", "duplicate", "older"])

    def test_collect_unseen_cards_from_search_urls_skips_throttled_search(self) -> None:
        def fake_fetch(url: str) -> str:
            if "throttled" in url:
                with raising_http_error(url, 429, "Too Many Requests") as error:
                    raise error
            return """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:newer">
                    <time class="job-search-card__listdate--new" datetime="2026-04-28">9 minutes ago</time>
                </div>
            """

        cards = collect_unseen_cards_from_search_urls(
            [
                "https://www.linkedin.com/jobs/search-results/?keywords=throttled",
                "https://www.linkedin.com/jobs/search-results/?keywords=engineer",
            ],
            limit=2,
            seen_job_ids=set(),
            max_pages=1,
            fetch_html=fake_fetch,
        )

        self.assertEqual([card.job_id for card in cards], ["newer"])

    def test_collect_unseen_cards_paginates_until_limit_of_new_jobs(self) -> None:
        pages = {
            "start=0": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:seen"></div>
                <div class="base-card" data-entity-urn="urn:li:jobPosting:new-1"></div>
            """,
            "start=2": """
                <div class="base-card" data-entity-urn="urn:li:jobPosting:new-2"></div>
            """,
        }

        def fake_fetch(url: str) -> str:
            for marker, html in pages.items():
                if marker in url:
                    return html
            return ""

        cards = collect_unseen_cards(
            "https://www.linkedin.com/jobs/search-results/?keywords=engineer",
            limit=2,
            seen_job_ids={"seen"},
            max_pages=3,
            fetch_html=fake_fetch,
        )

        self.assertEqual([card.job_id for card in cards], ["new-1", "new-2"])

    def test_env_flag_enabled_accepts_github_variable_true(self) -> None:
        self.assertTrue(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "true"}))
        self.assertTrue(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "1"}))
        self.assertFalse(env_flag_enabled("CHEAT_MODE", {"CHEAT_MODE": "false"}))
        self.assertFalse(env_flag_enabled("CHEAT_MODE", {}))

    def test_parse_cheat_job_ad_uses_expected_sections(self) -> None:
        job = parse_cheat_job_ad(
            """
            Job Title
            Backend Python Integration Developer

            Company
            Nordic Diagnostics Automation

            Location
            Lund, Skane County, Sweden

            Benefit
            Hybrid

            Posted
            Today

            Job Description
            Build Python integrations.
            Validate regulated workflows.
            """
        )

        self.assertEqual(job.job_id, CHEAT_JOB_ID)
        self.assertEqual(job.title, "Backend Python Integration Developer")
        self.assertEqual(job.company, "Nordic Diagnostics Automation")
        self.assertEqual(job.location, "Lund, Skane County, Sweden")
        self.assertEqual(job.benefit, "Hybrid")
        self.assertEqual(job.posted, "Today")
        self.assertIn("Build Python integrations.", job.description)
        self.assertIn("Validate regulated workflows.", job.description)

    def test_load_cheat_job_ad_extracts_rtf_text(self) -> None:
        rtf = (
            r"{\rtf1 Job Title\par Backend Python Integration Developer\par "
            r"Job Description\par Build Python integrations.\par}"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cheat_mode_job_ad.rtf"
            path.write_text(rtf, encoding="utf-8")

            job = load_cheat_job_ad(path)

        self.assertEqual(job.title, "Backend Python Integration Developer")
        self.assertEqual(job.description, "Build Python integrations.")

    def test_load_cheat_job_ad_extracts_textutil_line_breaks(self) -> None:
        rtf = "{\\rtf1 Job Title\\\nBackend Python Integration Developer\\\nJob Description\\\nBuild Python integrations.\\\n}"
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cheat_mode_job_ad.rtf"
            path.write_text(rtf, encoding="utf-8")

            job = load_cheat_job_ad(path)

        self.assertEqual(job.title, "Backend Python Integration Developer")
        self.assertEqual(job.description, "Build Python integrations.")

    def test_main_writes_cheat_job_even_when_no_public_cards_are_found(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_path = root / "input.json"
            results_root = root / "results"
            cheat_path = results_root / "cheat_mode_job_ad.rtf"
            input_path.write_text(json.dumps({"keywords": "engineer"}), encoding="utf-8")
            results_root.mkdir()
            cheat_path.write_text(
                "\n".join(
                    [
                        "Job Title",
                        "Backend Python Integration Developer",
                        "Job Description",
                        "Build Python integrations.",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch("linkedin_guest_jobs.collect_unseen_cards_from_search_urls", return_value=[]):
                with mock.patch.dict("os.environ", {"CHEAT_MODE": "true"}, clear=False):
                    with mock.patch("sys.stdout", new_callable=io.StringIO):
                        with mock.patch(
                            "sys.argv",
                            [
                                "linkedin_guest_jobs.py",
                                "--input",
                                str(input_path),
                                "--results-root",
                                str(results_root),
                                "--limit",
                                "2",
                            ],
                        ):
                            exit_code = main()

            output_files = list(results_root.glob("discard/*/linkedin_jobs_sample.json"))
            payload = json.loads(output_files[0].read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(output_files), 1)
        self.assertEqual(payload[0]["job_id"], CHEAT_JOB_ID)
        self.assertEqual(payload[0]["title"], "Backend Python Integration Developer")


if __name__ == "__main__":
    unittest.main()
