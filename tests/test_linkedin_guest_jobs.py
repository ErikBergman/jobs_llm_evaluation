import tempfile
import unittest
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from linkedin_guest_jobs import (
    GUEST_SEARCH_URL,
    JobCard,
    collect_unseen_cards,
    guest_search_url,
    load_search_config,
    load_seen_job_ids,
    search_url_from_config,
    select_unseen_cards,
)


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


if __name__ == "__main__":
    unittest.main()
