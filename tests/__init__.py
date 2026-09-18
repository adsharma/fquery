import pathlib
import unittest

PROJECT_DIR = pathlib.Path(__file__).parents[1]


def test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    return test_suite
