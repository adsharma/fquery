import pathlib
import sys
import unittest

PROJECT_DIR = pathlib.Path(__file__).parents[1]
print(PROJECT_DIR)
sys.path.append(str(PROJECT_DIR / "fquery"))


def test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    return test_suite
