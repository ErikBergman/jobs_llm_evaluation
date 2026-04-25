import unittest

from match_jobs import is_mock_hit


class MockMatcherTests(unittest.TestCase):
    def test_engineer_word_is_hit(self) -> None:
        self.assertTrue(is_mock_hit({"description": "Engineer needed"}))

    def test_engineering_is_not_hit(self) -> None:
        self.assertFalse(is_mock_hit({"description": "software engineering"}))

    def test_missing_or_empty_description_is_discard(self) -> None:
        self.assertFalse(is_mock_hit({}))
        self.assertFalse(is_mock_hit({"description": ""}))


if __name__ == "__main__":
    unittest.main()
