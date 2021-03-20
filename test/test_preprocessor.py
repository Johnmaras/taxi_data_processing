from unittest import TestCase

from preprocessor.src import preprocessor


class Test(TestCase):
    def test_handler(self):
        """Runs the preprocessor module"""
        preprocessor.handler({}, {})
