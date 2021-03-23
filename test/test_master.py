from unittest import TestCase

from master.src import master


class Test(TestCase):
    def test_handler(self):
        master.handler({}, {})
