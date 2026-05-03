import unittest


def load_tests(loader: unittest.TestLoader, tests: unittest.TestSuite, pattern: str) -> unittest.TestSuite:
    return loader.discover("tests")


if __name__ == "__main__":
    unittest.main()
