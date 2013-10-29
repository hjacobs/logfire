from StringIO import StringIO
from unittest import TestCase

import logging
import os

import logfire
from logfire import Log4jParser, LogLevel, LogReader


class Log4jParserTests(TestCase):

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

    def test_autoconfigure_with_thread_and_flow_id(self):
        parser = Log4jParser()
        parser.autoconfigure(StringIO('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Spaced message!'))
        self.assertEqual(parser.delimiter, ' ')
        self.assertEqual(parser.flow_id_column_index, 0)
        self.assertEqual(parser.level_column_index, 1)
        self.assertEqual(parser.thread_column_index, 2)
        self.assertEqual(parser.location_column_index, 3)
        self.assertEqual(parser.message_column_index, 4)
        self.assertEqual(parser.column_count, 5)

    def test_autoconfigure_with_thread_but_without_flow_id(self):
        parser = Log4jParser()
        parser.autoconfigure(StringIO('2000-01-01 00:00:00,000 ERROR Thread C.m(C.java:23): Spaced message!'))
        self.assertEqual(parser.delimiter, ' ')
        self.assertEqual(parser.level_column_index, 0)
        self.assertEqual(parser.thread_column_index, 1)
        self.assertEqual(parser.location_column_index, 2)
        self.assertEqual(parser.message_column_index, 3)
        self.assertEqual(parser.column_count, 4)
        self.assertEqual(parser.flow_id_column_index, None)

    def test_autoconfigure_without_thread_or_flow_id(self):
        parser = Log4jParser()
        parser.autoconfigure(StringIO('2000-01-01 00:00:00,000 ERROR C.m(C.java:23): Spaced message!'))
        self.assertEqual(parser.delimiter, ' ')
        self.assertEqual(parser.level_column_index, 0)
        self.assertEqual(parser.location_column_index, 1)
        self.assertEqual(parser.message_column_index, 2)
        self.assertEqual(parser.column_count, 3)
        self.assertEqual(parser.thread_column_index, None)
        self.assertEqual(parser.flow_id_column_index, None)

    def test_autoconfigure_without_code_location(self):
        self.assertRaises(Exception, Log4jParser().autoconfigure, StringIO('2000-01-01 00:00:00,000 ERROR: Message!'))

    def test_regression_too_few_columns_endless_loop(self):
        """Lines with too few columns do no longer cause an endless loop."""

        list(Log4jParser().read(0, StringIO(self.sample_line + '\n2000-01-01 00:00:00,001 GARBAGE')))

    def test_skipped_lines_are_logged(self):
        """Skipped lines are logged."""

        list(Log4jParser().read(0, StringIO('NO_DATE\n2000-01-01 00:00:00,000 NO_COLUMNS')))
        warnings = self.fake_logging.warnings
        self.assertEqual(len(warnings), 2)
        self.assertTrue('NO_DATE' in warnings[0])
        self.assertTrue('NO_COLUMNS' in warnings[1])

    def test_read_log_level_mapping(self):
        """The log level is correctly mapped to LogLevel intances."""

        parser = Log4jParser()
        self.assertEqual(parser._read_log_level(['TRACE'], 0), LogLevel.TRACE)
        self.assertEqual(parser._read_log_level(['[DEBUG]'], 0), LogLevel.DEBUG)
        self.assertEqual(parser._read_log_level(['INFO:'], 0), LogLevel.INFO)
        self.assertEqual(parser._read_log_level(['[WARN]'], 0), LogLevel.WARN)
        self.assertEqual(parser._read_log_level(['WARNING'], 0), LogLevel.WARN)
        self.assertEqual(parser._read_log_level(['[ERROR]:'], 0), LogLevel.ERROR)
        self.assertEqual(parser._read_log_level(['FATAL'], 0), LogLevel.FATAL)

    def test_read_log_level_handles_malformed_input(self):
        """Malformed log levels are handled."""

        parser = Log4jParser()
        self.assertEqual(parser._read_log_level([''], 0), LogLevel.FATAL)
        self.assertEqual(parser._read_log_level(['[]'], 0), LogLevel.FATAL)
        self.assertEqual(parser._read_log_level(['BORING'], 0), LogLevel.FATAL)

    def test_read_log_level_in_context(self):
        """The log level is correctly extracted from the log entry."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].level, LogLevel.ERROR)

    def test_read_flow_id_strips_colons(self):
        """Trailing colons are correctly stripped from flow IDs."""

        parser = Log4jParser()
        self.assertEqual(parser._read_flow_id(['FlowID'], 0), 'FlowID')
        self.assertEqual(parser._read_flow_id(['FlowID:'], 0), 'FlowID')

    def test_read_flow_id_without_flow_id(self):
        """Lines without flow ID are handled correctly."""

        self.assertEqual(Log4jParser()._read_flow_id(['not a flow ID'], None), None)

    def test_read_flow_id_in_context(self):
        """The flow ID is correctly extracted from the log entry."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].flowid, 'FlowID')

    def test_read_thread_strips_colons(self):
        """Trailing colons are correctly stripped from threads."""

        parser = Log4jParser()
        self.assertEqual(parser._read_thread(['Thread'], 0), 'Thread')
        self.assertEqual(parser._read_thread(['Thread:'], 0), 'Thread')

    def test_read_thread_without_thread(self):
        """Lines without thread are handled correctly."""

        self.assertEqual(Log4jParser()._read_thread(['not a thread'], None), None)

    def test_read_flow_id_in_context(self):
        """The thread is correctly extracted from the log entry."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].thread, 'Thread')

    def test_read_code_position_strips_trailing_colons(self):
        """Trailing colons are correctly stripped from code positions."""

        parser = Log4jParser()
        self.assertEqual(parser._read_code_position(['C.m(C.java:23)'], 0), ('C', 'm', 'C.java', 23))
        self.assertEqual(parser._read_code_position(['C.m(C.java:23):'], 0), ('C', 'm', 'C.java', 23))

    def test_read_code_position_handles_malformed_input(self):
        """Malformed code positions are handled."""

        def assert_is_parsed(code_position_string):
            code_position = parser._read_code_position([code_position_string], 0)
            self.assertEqual(len(code_position), 4)
            self.assertTrue(isinstance(code_position[3], int))

        parser = Log4jParser()
        assert_is_parsed('?(C.java:23)')               # ('', '?', 'C.java', 23))
        assert_is_parsed('.m(C.java:23)')              # ('', 'm', 'C.java', 23))
        assert_is_parsed('C.(C.java:23)')              # ('C', '', 'C.java', 23))
        assert_is_parsed('.(C.java:23)')               # ('', '', 'C.java', 23))
        assert_is_parsed('(C.java:23)')                # ('', '', 'C.java', 23))
        assert_is_parsed('C.m(?)')                     # ('C', 'm', '?', -1))
        assert_is_parsed('C.m(:23)')                   # ('C', 'm', '', 23))
        assert_is_parsed('C.m(C.java:)')               # ('C', 'm', 'C.java', -1))
        assert_is_parsed('C.m(:)')                     # ('C', 'm', '', -1))
        assert_is_parsed('C.m()')                      # ('C', 'm', '', -1))
        assert_is_parsed('C.m(C.java:NaN)')            # ('C', 'm', 'C.java', -1))
        assert_is_parsed('C.m(C.java:3rr0r)')          # ('C', 'm', 'C.java', -1))
        assert_is_parsed('?.?:?')                      # ('', '', '?.?', -1))
        assert_is_parsed('(C.java:23)')                # ('', '', 'C.java', 23))
        assert_is_parsed('C.m(')                       # ('C', 'm', '', -1))
        assert_is_parsed('(')                          # ('', '', '', -1))
        assert_is_parsed('')                           # ('', '', '', -1))
        assert_is_parsed('C.m(C.java:23:42)')          # ('C', 'm', 'C.java', -1))
        assert_is_parsed('C.m(C.java:23)(D.java:42)')  # ('C.m(C', 'java:23)', 'D.java', 42))
        assert_is_parsed('C.m(C.ja(D.java:42)va:23)')  # ('C.m(C', 'ja', 'D.java', -1))
        assert_is_parsed('C.m(C.java:23')              # ('C', 'm', 'C.java', 23))
        assert_is_parsed('C.m(C.java:23:')             # ('C', 'm', 'C.java', 23))

    def test_read_code_position_in_context(self):
        """The code position is correctly extracted from the log entry."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_line)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].clazz, 'C')
        self.assertEqual(entries[0].method, 'm')
        self.assertEqual(entries[0].file, 'C.java')
        self.assertEqual(entries[0].line, 23)

    def test_read_message_reads_single_line_entries(self):
        """Single-line messages are read correctly."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_line + '\n' + self.another_sample_line)))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].message, 'Error!')
        self.assertEqual(entries[1].message, 'No error! That\'s weird.')

    def test_read_message_reads_multiline_entries(self):
        """Multiline messages are read correctly."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_multiline_entry + '\n' + self.another_sample_line)))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].message, 'Error!\nE: :(\n        at D.n(D.java:42)\n        at E.o(E.java:5)')

    def test_read_message_reads_terminal_multiline_entries(self):
        """Multiline messages at the end of the file are read correctly."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_multiline_entry)))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].message, 'Error!\nE: :(\n        at D.n(D.java:42)\n        at E.o(E.java:5)')

    def test_read_message_does_not_cause_log_entries_to_be_skipped(self):
        """The code for reading multiline messages does not cause log entries to be skipped."""

        entries = list(Log4jParser().read(0, StringIO(self.sample_multiline_entry + '\n' + self.another_sample_line)))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[1].message, 'No error! That\'s weird.')


class LogReaderTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fake_logging = FakeLogging()

    def setUp(self):
        logfire.logging = self.fake_logging

    def tearDown(self):
        logfire.logfire = logging
        self.fake_logging.reset()
        try:
            os.remove('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794')
        except OSError:
            pass
        try:
            os.remove('log.log')
        except OSError:
            pass

    def test_seek_tail_from_sincedb(self):
        self.write_log_file('XXXX\n' * 100)
        self.write_sincedb_file('log.log 23 50 75')
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', 'DUMMY PARSER', 'DUMMY RECEIVER', sincedb='since.db')
            reader._file = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 50)
            self.assertEqual(reader._fid, 23)

    def test_seek_tail_from_sincedb_no_sincedb(self):
        self.write_log_file('XXXX\n' * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', 'DUMMY PARSER', 'DUMMY RECEIVER', sincedb='since.db', tail=100)
            reader._file = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 0)
            self.assertEqual(self.fake_logging.warnings, ['Failed to read the sincedb file for "log.log".'])

    def test_seek_file_without_sincedb_not_enough_lines(self):
        self.write_log_file('XXXX\n' * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', 'DUMMY PARSER', 'DUMMY RECEIVER', tail=100)
            reader._file = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 0)

    def write_log_file(self, *lines):
        with open('log.log', 'wb') as f:
            f.write('\n'.join(lines))

    def write_sincedb_file(self, contents):
        with open('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794', 'wb') as f:
            f.write(contents)




class FakeLogging(object):

    def __init__(self):
        self.reset()

    def debug(self, msg, *args, **kwargs):
        self.debugs.append(self.format_message(msg, args))

    def info(self, msg, *args, **kwargs):
        self.infos.append(self.format_message(msg, args))

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
        self.debugs = []
        self.infos = []
        self.warnings = []
