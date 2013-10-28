from StringIO import StringIO
from unittest import TestCase

import logging

import logfire


class LogfireTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fake_logging = FakeLogging()

    def setUp(self):
        logfire.logging = self.fake_logging

    def tearDown(self):
        logfire.logfire = logging
        self.fake_logging.reset()

    def test_regression_log4j_endless_loop(self):
        """Lines with too few columns do no longer cause an endless loop."""

        line1 = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Exception!'
        line2 = '2000-01-01 00:00:00,001 GARBAGE'
        list(logfire.Log4Jparser().read(0, StringIO(line1 + '\n' + line2)))

    def test_skipped_lines_are_logged(self):
        """Skipped lines are logged."""

        list(logfire.Log4Jparser().read(0, StringIO('NO_DATE\n2000-01-01 00:00:00,000 NO_COLUMNS')))
        warnings = self.fake_logging.warnings
        self.assertEqual(len(warnings), 2)
        self.assertTrue('NO_DATE' in warnings[0])
        self.assertTrue('NO_COLUMNS' in warnings[1])



class FakeLogging(object):

    def __init__(self):
        self.reset()        

    def warn(self, msg, *args, **kwargs):
        self.warnings.append(self.format_message(msg, args))

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(self.format_message(msg, args))

    @classmethod
    def format_message(cls, msg, args):
        if len(args) == 1 and isinstance(args[0], dict):
            args = args[0]
        return msg % args

    def reset(self):
        self.warnings = []
