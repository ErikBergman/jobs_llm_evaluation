import json
import unittest
from dataclasses import asdict
from pathlib import Path

from linkedin_guest_jobs import JobCard, parse_job_detail, prefilter_job
from match_jobs import mock_match_decision


CORPUS_DIR = Path(__file__).parent / "fixtures" / "job_corpus"


def load_corpus() -> list[dict[str, object]]:
    fixtures: list[dict[str, object]] = []
    for path in sorted(CORPUS_DIR.glob("*.json")):
        with open(path, encoding="utf-8") as fixture_file:
            fixture = json.load(fixture_file)
        fixture["_path"] = path.name
        fixtures.append(fixture)
    return fixtures


class OfflineJobCorpusTests(unittest.TestCase):
    def test_corpus_extraction_prefilter_and_mock_route(self) -> None:
        for fixture in load_corpus():
            with self.subTest(fixture=fixture["name"]):
                job_card = JobCard(**fixture["job_card"])
                detail = parse_job_detail(fixture["detail_html"], job_card)
                job_payload = asdict(detail)
                prefilter = prefilter_job(job_payload)
                mock_decision = mock_match_decision(job_payload)
                expected = fixture["expected"]

                self.assertEqual(
                    detail.requirements_text,
                    expected["requirements_text"],
                    f"{fixture['_path']} requirements_text changed",
                )
                self.assertEqual(
                    detail.requirements_extraction_method,
                    expected["requirements_extraction_method"],
                    f"{fixture['_path']} extraction method changed",
                )
                self.assertEqual(
                    prefilter["prefilter_pass"],
                    expected["prefilter_pass"],
                    f"{fixture['_path']} prefilter pass/reject changed",
                )
                self.assertEqual(
                    prefilter["prefilter_positive_matches"],
                    expected["prefilter_positive_matches"],
                    f"{fixture['_path']} positive prefilter matches changed",
                )
                self.assertEqual(
                    prefilter["prefilter_negative_matches"],
                    expected["prefilter_negative_matches"],
                    f"{fixture['_path']} negative prefilter matches changed",
                )
                self.assertEqual(
                    mock_decision["hit"],
                    expected["mock_hit"],
                    f"{fixture['_path']} mock route changed",
                )

    def test_corpus_keeps_required_edge_case_coverage(self) -> None:
        coverage = {
            marker
            for fixture in load_corpus()
            for marker in fixture.get("coverage", [])
        }

        self.assertIn("paragraph_requirements", coverage)
        self.assertIn("list_requirements", coverage)
        self.assertIn("mixed_requirements", coverage)
        self.assertIn("stop_heading", coverage)
        self.assertIn("fallback_full_description", coverage)
        self.assertIn("negative_prefilter", coverage)

    def test_corpus_reports_extraction_reduction_shape(self) -> None:
        pattern_count = 0
        fallback_count = 0
        full_words = 0
        requirements_words = 0

        for fixture in load_corpus():
            detail = parse_job_detail(fixture["detail_html"], JobCard(**fixture["job_card"]))
            if detail.requirements_extraction_method == "pattern":
                pattern_count += 1
                full_words += len(detail.description.split())
                requirements_words += len(detail.requirements_text.split())
            else:
                fallback_count += 1

        self.assertGreater(pattern_count, 0)
        self.assertGreater(fallback_count, 0)
        self.assertLess(requirements_words, full_words)
