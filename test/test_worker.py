from unittest import TestCase

from worker.src import worker


class Test(TestCase):
    def test_handler(self):
        worker.handler({}, {})
