import unittest
from urllib.parse import parse_qs, urlparse

from linkedin_guest_jobs import GUEST_SEARCH_URL, search_url_from_config, guest_search_url


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


if __name__ == "__main__":
    unittest.main()
