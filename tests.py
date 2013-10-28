from StringIO import StringIO
from unittest import TestCase

import logging

import logfire
from logfire import Log4Jparser, LogLevel


class Log4JparserTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fake_logging = FakeLogging()
        cls.sample_line = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error!'
        cls.another_sample_line = '2000-01-01 00:00:00,001 FlowID INFO Thread C.m(C.java:25): No error! That\'s weird.'
        cls.sample_multiline_entry = cls.sample_line + '\nE: :(\n        at D.n(D.java:42)\n        at E.o(E.java:5)'

    def setUp(self):
        logfire.logging = self.fake_logging

    def tearDown(self):
        logfire.logfire = logging
        self.fake_logging.reset()

    def test_regression_too_few_columns_endless_loop(self):
        """Lines with too few columns do no longer cause an endless loop."""

        list(Log4Jparser().read(0, StringIO(self.sample_line + '\n2000-01-01 00:00:00,001 GARBAGE')))

    def test_skipped_lines_are_logged(self):
        """Skipped lines are logged."""

        list(Log4Jparser().read(0, StringIO('NO_DATE\n2000-01-01 00:00:00,000 NO_COLUMNS')))
        warnings = self.fake_logging.warnings
        self.assertEqual(len(warnings), 2)
        self.assertTrue('NO_DATE' in warnings[0])
        self.assertTrue('NO_COLUMNS' in warnings[1])

    def test_level_is_read(self):
        """The log level is read correctly."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].level, LogLevel.ERROR)

    def test_flow_id_is_read(self):
        """The flow ID is read correctly."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].flowid, 'FlowID')

    def test_thread_is_read(self):
        """The thread is read correctly."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].thread, 'Thread')

    def test_code_position_is_read(self):
        """The code position is read correctly."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].clazz, 'C')
        self.assertEqual(entries[0].method, 'm')
        self.assertEqual(entries[0].file, 'C.java')
        self.assertEqual(entries[0].line, 23)

    def test_continuation_lines_are_read(self):
        """Multiline messages are read correctly."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_multiline_entry + '\n' + self.another_sample_line)))        
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].message, 'Error!\nE: :(\n        at D.n(D.java:42)\n        at E.o(E.java:5)')

    def test_first_non_continuation_line_is_handles(self):
        """The code for reading multiline messages does not cause log entries to be skipped."""

        entries = list(Log4Jparser().read(0, StringIO(self.sample_multiline_entry + '\n' + self.another_sample_line)))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[1].message, 'No error! That\'s weird.')


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
