from unittest import TestCase

from reducer.src import reducer


class Test(TestCase):
    def test_handler(self):
        reducer.handler({}, {})
