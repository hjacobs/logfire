from StringIO import StringIO
from unittest import TestCase

import gzip
import logging
import os
import sys

import logfire
import logreader
from logfire import Log4jParser, LogLevel
from logreader import LogReader, get_device_and_inode_string


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
        logreader.logging = self.fake_logging
        self.files_to_delete = ['log.log']

    def tearDown(self):
        logfire.logfire = logging
        self.fake_logging.reset()
        for f in self.files_to_delete:
            try:
                os.remove(f)
            except OSError:
                pass

    def test_seek_sincedb_position(self):
        self.write_log_file('XXXX\n' * 100)
        self.write_sincedb_file('log.log 123g456 50 75')
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 50)
            self.assertEqual(reader._file_device_and_inode_string, '123g456')

    def test_seek_sincedb_position_no_sincedb(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 20)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db', tail=10)
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 10 * 75)
            self.assertEqual(self.fake_logging.warnings, ['Failed to read the sincedb file for "log.log".'])

    def test_seek_tail_not_enough_lines(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', tail=20)
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 0)

    def test_seek_tail_one_chunk(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 20)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', tail=10)
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 10 * 75)

    def test_seek_tail_multiple_chunks(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 1000)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', tail=900)
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 100 * 75)

    def test_seek_tail_with_multiline_messages_one_chunk(self):
        message = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' + 'X' * 24 + '\n'
        self.write_log_file(message * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', tail=5)
            reader._file = f
            reader._seek_position()
            self.assertEqual(f.tell(), 5 * 100)

    def test_seek_time_in_empty_file(self):
        with prepared_reader(seconds=()) as reader:
            reader._seek_time('2000-01-01 00:00:00,000')
            self.assertEqual(reader._file.tell(), 0)

    def test_seek_time_one_chunk_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader._seek_time('2000-01-01 00:00:05,000')
            self.assertEqual(reader._file.tell(), 5 * 75)

    def test_seek_time_one_chunk_first_line_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader._seek_time('2000-01-01 00:00:00,000')
            self.assertEqual(reader._file.tell(), 0)

    def test_seek_time_one_chunk_last_line_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader._seek_time('2000-01-01 00:00:09,000')
            self.assertEqual(reader._file.tell(), 9 * 75)

    def test_seek_time_one_chunk_between_lines(self):
        with prepared_reader(seconds=range(0, 20, 2)) as reader:
            reader._seek_time('2000-01-01 00:00:15,000')
            self.assertEqual(reader._file.tell(), 8 * 75)

    def test_seek_time_one_chunk_before_file(self):
        with prepared_reader(seconds=range(10, 20)) as reader:
            reader._seek_time('2000-01-01 00:00:05,000')
            self.assertEqual(reader._file.tell(), 0)

    def test_seek_time_one_chunk_after_file(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader._seek_time('2000-01-01 00:00:12,000')
            self.assertEqual(reader._file.tell(), 10 * 75)

    def test_seek_time_continuation_lines_excact_match(self):
        with prepared_reader(seconds=range(60), continuation_line_count=5) as reader:
            reader._seek_time('2000-01-01 00:00:30,000')
            self.assertEqual(reader._file.tell(), 30 * 200)

    def test_seek_time_continuation_lines_between_lines(self):
        with prepared_reader(seconds=range(0, 60, 2), continuation_line_count=5) as reader:
            reader._seek_time('2000-01-01 00:00:31,000')
            self.assertEqual(reader._file.tell(), 16 * 200)

    def test_seek_time_continuation_lines_before_file(self):
        with prepared_reader(seconds=range(60, 120), continuation_line_count=5) as reader:
            reader._seek_time('2000-01-01 00:00:30,000')
            self.assertEqual(reader._file.tell(), 0)

    def test_seek_time_continuation_lines_after_file(self):
        with prepared_reader(seconds=range(60), continuation_line_count=5) as reader:
            reader._seek_time('2000-01-01 00:01:30,000')
            self.assertEqual(reader._file.tell(), 60 * 200)

    ### tests for _open_file() ###

    def test_open_file_with_regular_file(self):
        called = []
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
        reader._seek_position = lambda: called.append('_seek_position')
        reader._open_file()
        try:
            self.assertEqual(reader._file.name, 'log.log')
            self.assertFalse(reader._file.closed)
            self.assertRaises(reader._first)
            self.assertNotEqual(reader._file_device_and_inode_string, None)
            self.assertEqual(reader._file.read(), 'Some file contents!')
            self.assertEqual(called, ['_seek_position'])
        finally:
            reader._file.close()

    def test_open_file_with_gzip_file(self):
        called = []
        self.files_to_delete.append('log.gz')
        with gzip.open('log.gz', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.gz', Log4jParser(), 'DUMMY RECEIVER')
        reader._seek_position = lambda: called.append('_seek_position')
        reader._open_file()
        try:
            self.assertEqual(reader._file.name, 'log.gz')
            self.assertFalse(reader._file.closed)
            self.assertRaises(reader._first)
            self.assertNotEqual(reader._file_device_and_inode_string, None)
            self.assertEqual(reader._file.read(), 'Some file contents!')
            self.assertEqual(called, ['_seek_position'])
        finally:
            reader._file.close()

    def test_open_file_without_seek_position(self):
        called = []
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
        reader._seek_position = lambda: called.append('_seek_position')
        reader._open_file(seek_position=False)
        try:
            self.assertEqual(called, [])
        finally:
            reader._file.close()

    def test_open_file_with_nonexistent_file(self):
        reader = LogReader(0, 'no.such.file', Log4jParser(), 'DUMMY RECEIVER')
        self.assertRaises(IOError, reader._open_file)

    ### tests for _close_file() ###

    def test_close_file(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
        reader._open_file()
        f = reader._file
        reader._close_file()
        self.assertTrue(f.closed)
        self.assertEqual(reader._file, None)

    ### tests for _do_housekeeping() ###

    def test_do_housekeeping_first_time(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._do_housekeeping(23)
        self.assertEqual(called, ['_ensure_file_is_good', '_save_progress'])
        self.assertEqual(reader._last_ensure_file_is_good_call_timestamp, 23)
        self.assertEqual(reader._last_save_progress_call_timestamp, 23)

    def test_do_housekeeping_second_time_too_early(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._last_ensure_file_is_good_call_timestamp = 23
        reader._last_save_progress_call_timestamp = 23
        reader._do_housekeeping(24)
        self.assertEqual(called, [])
        self.assertEqual(reader._last_ensure_file_is_good_call_timestamp, 23)
        self.assertEqual(reader._last_save_progress_call_timestamp, 23)

    def test_do_housekeeping_second_time_late_enough(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._last_ensure_file_is_good_call_timestamp = 23
        reader._last_save_progress_call_timestamp = 23
        reader._do_housekeeping(42)
        self.assertEqual(called, ['_ensure_file_is_good', '_save_progress'])
        self.assertEqual(reader._last_ensure_file_is_good_call_timestamp, 42)
        self.assertEqual(reader._last_save_progress_call_timestamp, 42)
       
    ### tests for _ensure_file_is_good() ###

    def test_ensure_file_is_good_file_does_not_exist(self):
        reader = LogReader(0, 'no.such.file', Log4jParser(), 'DUMMY RECEIVER')
        reader._ensure_file_is_good()
        self.assertEqual(self.fake_logging.infos, ['The file no.such.file has been removed.'])

    def test_ensure_file_is_good_file_has_been_rotated(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
            reader._file = f
            reader._file_device_and_inode_string = 'not matching'
            reader._ensure_file_is_good()
            self.assertTrue(f.closed)
            self.assertNotEqual(reader._file, f)
            self.assertFalse(reader._file.closed)
            self.assertEqual(reader._file.readline(), 'Some file contents!')
            self.assertEqual(self.fake_logging.infos[0], 'The file log.log has been rotated.')

    def test_ensure_file_is_good_file_has_been_truncated(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
            reader._file = f
            reader._file_device_and_inode_string = get_device_and_inode_string(os.fstat(f.fileno()))
            f.truncate(0) 
            reader._ensure_file_is_good()
            self.assertFalse(f.closed)
            self.assertEqual(reader._file, f)
            self.assertEqual(f.tell(), 0)
            self.assertEqual(self.fake_logging.infos, ['The file log.log has been truncated.'])

    def test_ensure_file_is_good_file_is_good(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            f.seek(10)
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
            reader._file = f
            reader._file_device_and_inode_string = get_device_and_inode_string(os.fstat(f.fileno()))
            reader._ensure_file_is_good()
            self.assertFalse(f.closed)
            self.assertEqual(reader._file, f)
            self.assertEqual(f.tell(), 10)
            self.assertEqual(self.fake_logging.infos, [])

    ### tests for _save_progress() ###

    def test_save_progress_success(self):
        self.files_to_delete.append('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._get_progress_string = lambda: 'log.log 123g456 10 19'
        reader._save_progress()
        with open('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794', 'rb') as f:
            self.assertEqual(f.read(), 'log.log 123g456 10 19')        

    def test_save_progress_no_data(self):
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._get_progress_string = lambda: None
        reader._save_progress()
        self.assertFalse(os.path.exists('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794'))

    def test_save_progress_failure(self):
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='invalid\0path')
        reader._get_progress_string = lambda: 'log.log 123g456 10 19'
        reader._save_progress()
        self.assertFalse(os.path.exists('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794'))
        self.assertEqual(self.fake_logging.exception_messages, ['Failed to save progress for log.log.'])

    ### tests for _load_progress() ###

    def test_load_progress_basic(self):
        self.write_sincedb_file('log.log 123g456 50 75')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        self.assertEqual(reader._load_progress(), ('log.log', '123g456', 50, 75))

    def test_load_progress_with_spaces_in_filename(self):
        self.files_to_delete.append('since.dbf4a53d67a02158bcc92d7d702a8f438ad18309488')
        with open('since.dbf4a53d67a02158bcc92d7d702a8f438ad18309488', 'wb') as f:
            f.write('log with spaces in name.log 123g456 50 75')
        reader = LogReader(0, 'log with spaces in name.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        self.assertEqual(reader._load_progress(), ('log with spaces in name.log', '123g456', 50, 75))

    ### tests for _get_progress_string() ###

    def test_get_progress_string_success(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            f.seek(10)
            reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
            reader._file = f
            reader._file_device_and_inode_string = '123g456'
            self.assertEqual(reader._get_progress_string(), 'log.log 123g456 10 19')

    def test_get_progress_string_failure(self):
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER', sincedb='since.db')
        reader._file_device_and_inode_string = '123g456'
        result = reader._get_progress_string()
        self.assertEqual(result, None)
        self.assertEqual(self.fake_logging.exception_messages, ['Failed to gather progress information for log.log.'])


    def write_log_file(self, *lines):
        with open('log.log', 'wb') as f:
            f.write('\n'.join(lines))

    def write_sincedb_file(self, contents):
        self.files_to_delete.append('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794')
        with open('since.dbf16c93d1167446f99a26837c0fdeac6fb73869794', 'wb') as f:
            f.write(contents)



class prepared_reader(object):

    DEFAULT_MESSAGE = '2000-01-01 00:%02d:%02d,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n'
    DEFAULT_CONTINUATION_LINE = ('X' * 24) + '\n'

    def __init__(self, seconds, message_template=None, continuation_line=None, continuation_line_count=0):
        self.seconds = seconds
        self.message_template = message_template or self.DEFAULT_MESSAGE
        self.message_template += (continuation_line or self.DEFAULT_CONTINUATION_LINE) * continuation_line_count

    def __enter__(self):
        lines = []
        for i in self.seconds:
            lines.append(self.message_template % divmod(i, 60))
        with open('log.log', 'wb') as f:
            f.write(''.join(lines))
        self.log = open('log.log', 'rb')
        reader = LogReader(0, 'log.log', Log4jParser(), 'DUMMY RECEIVER')
        reader._file = self.log
        return reader

    def __exit__(self, *args):
        self.log.close()







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

    def exception(self, msg, *args, **kwargs):
        self.exception_messages.append(self.format_message(msg, args))
        self.exception_infos.append(sys.exc_info())

    @classmethod
    def format_message(cls, msg, args):
        if len(args) == 1 and isinstance(args[0], dict):
            args = args[0]
        return msg % args

    def reset(self):
        self.debugs = []
        self.infos = []
        self.warnings = []
        self.exception_messages = []
        self.exception_infos = []
