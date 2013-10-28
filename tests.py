from StringIO import StringIO
from unittest import TestCase

import logging

import logfire


class LogfireTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fake_logging = FakeLogging()
        cls.sample_line = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Exception!'

    def setUp(self):
        logfire.logging = self.fake_logging

    def tearDown(self):
        logfire.logfire = logging
        self.fake_logging.reset()

    def test_regression_log4j_endless_loop(self):
        """Lines with too few columns do no longer cause an endless loop."""

        list(logfire.Log4Jparser().read(0, StringIO(self.sample_line + '\n2000-01-01 00:00:00,001 GARBAGE')))

    def test_skipped_lines_are_logged(self):
        """Skipped lines are logged."""

        list(logfire.Log4Jparser().read(0, StringIO('NO_DATE\n2000-01-01 00:00:00,000 NO_COLUMNS')))
        warnings = self.fake_logging.warnings
        self.assertEqual(len(warnings), 2)
        self.assertTrue('NO_DATE' in warnings[0])
        self.assertTrue('NO_COLUMNS' in warnings[1])

    def test_level_is_extracted(self):
        """The log level is extracted from log4j lines."""

        entries = list(logfire.Log4Jparser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].level, logfire.LogLevel.ERROR)

    def test_level_mapping(self):
        """The log4j log level is correctly mapped to LogLevel intances."""

        self.assertEqual(logfire.log_level_from_log4j_tag('TRACE'), logfire.LogLevel.TRACE)
        self.assertEqual(logfire.log_level_from_log4j_tag('[DEBUG]'), logfire.LogLevel.DEBUG)
        self.assertEqual(logfire.log_level_from_log4j_tag('INFO'), logfire.LogLevel.INFO)
        self.assertEqual(logfire.log_level_from_log4j_tag('[WARN]'), logfire.LogLevel.WARN)
        self.assertEqual(logfire.log_level_from_log4j_tag('WARNING'), logfire.LogLevel.WARN)
        self.assertEqual(logfire.log_level_from_log4j_tag('[ERROR]'), logfire.LogLevel.ERROR)
        self.assertEqual(logfire.log_level_from_log4j_tag('FATAL'), logfire.LogLevel.FATAL)


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
