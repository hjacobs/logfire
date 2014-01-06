from StringIO import StringIO
from unittest import TestCase

import gzip
import logging
import os
import redis

import logfire
import logreader
from common import LogLevel, LogFilter, get_device_and_inode_string
from logfire import Log4jParser, LogEntry, RedisOutputThread, NonOrderedLogAggregator, OrderedLogAggregator
from logreader import LogReader


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
        logfire.logging = logging
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
        log = self.fake_logging.log
        self.assertTrue(log[0].startswith('[WARN]') and 'NO_DATE' in log[0])
        self.assertTrue(log[1].startswith('[WARN]') and 'NO_COLUMNS' in log[1])
        self.assertEqual(len(log), 2)

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
        self.assertEqual(entries[0].flow_id, 'FlowID')

    def test_read_thread_strips_colons(self):
        """Trailing colons are correctly stripped from threads."""

        parser = Log4jParser()
        self.assertEqual(parser._read_thread(['Thread'], 0), 'Thread')
        self.assertEqual(parser._read_thread(['Thread:'], 0), 'Thread')

    def test_read_thread_without_thread(self):
        """Lines without thread are handled correctly."""

        self.assertEqual(Log4jParser()._read_thread(['not a thread'], None), None)

    def test_read_thread_in_context(self):
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
        self.assertEqual(entries[0].class_, 'C')
        self.assertEqual(entries[0].method, 'm')
        self.assertEqual(entries[0].source_file, 'C.java')
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
        logreader.logfire = logging
        self.fake_logging.reset()
        for f in self.files_to_delete:
            try:
                os.remove(f)
            except OSError:
                pass

    ### tests for _run() ###

    def test_run_some_entries(self):
        housekeeping_timestamps = []
        with prepared_reader(seconds=range(60)) as reader:
            reader._maybe_do_housekeeping = lambda current_timestamp: housekeeping_timestamps.append(current_timestamp)
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 61)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:00,000')
            self.assertEqual(reader.receiver.entries[60], 'EOF 0')
            self.assertEqual(len(housekeeping_timestamps), 1)

    def test_run_lots_of_entries(self):
        housekeeping_timestamps = []
        with prepared_reader(seconds=range(3000)) as reader:
            reader._maybe_do_housekeeping = lambda current_timestamp: housekeeping_timestamps.append(current_timestamp)
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 3001)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:00,000')
            self.assertEqual(reader.receiver.entries[3000], 'EOF 0')
            self.assertEqual(len(housekeeping_timestamps), 3)

    def test_run_no_entries_follow(self):
        with prepared_reader(seconds=range(0)) as reader:
            reader.follow = True
            reader.NO_ENTRIES_SLEEP_INTERVAL = 0
            reader._maybe_do_housekeeping = lambda current_timestamp: 1/0
            reader.parser.autoconfigure = lambda logfile: None
            self.assertRaises(ZeroDivisionError, reader.run)
            self.assertEqual(len(reader.receiver.entries), 0)

    def test_run_some_entries_follow(self):
        with prepared_reader(seconds=range(60)) as reader:
            reader.follow = True
            reader.NO_ENTRIES_SLEEP_INTERVAL = 0
            reader._maybe_do_housekeeping = lambda current_timestamp: 1 / (1 - bool(reader.receiver.entries))
            self.assertRaises(ZeroDivisionError, reader.run)
            self.assertEqual(len(reader.receiver.entries), 60)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:00,000')
            self.assertEqual(reader.receiver.entries[59].timestamp, '2000-01-01 00:00:59,000')

    def test_run_seek_tail(self):
        with prepared_reader(seconds=range(60)) as reader:
            reader.tail_length = 30
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 31)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:30,000')
            self.assertEqual(reader.receiver.entries[30], 'EOF 0')

    def test_run_seek_tail_none(self):
        with prepared_reader(seconds=range(60)) as reader:
            reader.tail_length = None
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 61)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:00,000')
            self.assertEqual(reader.receiver.entries[60], 'EOF 0')

    def test_run_seek_tail_zero(self):
        with prepared_reader(seconds=range(60)) as reader:
            reader.tail_length = 0
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 1)
            self.assertEqual(reader.receiver.entries[0], 'EOF 0')

    def test_run_seek_first_unprocessed_position(self):
        with prepared_reader(seconds=range(60)) as reader:
            self.write_progress_file('log.log {0} 2250 2250'.format(get_device_and_inode_string(os.stat('log.log'))))
            reader.progress_file_path = 'progressf16c93d1167446f99a26837c0fdeac6fb73869794'
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 31)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:30,000')
            self.assertEqual(reader.receiver.entries[30], 'EOF 0')

    def test_run_seek_time(self):
        with prepared_reader(seconds=range(60)) as reader:
            reader.entry_filter.time_from = '2000-01-01 00:00:30,000'
            reader.run()
            self.assertEqual(len(reader.receiver.entries), 31)
            self.assertEqual(reader.receiver.entries[0].timestamp, '2000-01-01 00:00:30,000')
            self.assertEqual(reader.receiver.entries[30], 'EOF 0')

    def test_run_with_loglevel_suppression(self):
        with open('log.log', 'wb') as f:
            f.write('2000-01-01 00:00:00,000 FlowID INFO Thread C.m(C.java:23) Info!\n')
            f.write('2000-01-01 00:00:01,000 FlowID ERROR Thread C.m(C.java:23) Error!\n')
            f.write('2000-01-01 00:00:02,000 FlowID TRACE Thread C.m(C.java:23) Trace!\n')
            f.write('2000-01-01 00:00:03,000 FlowID WARNING Thread C.m(C.java:23) Warning!\n')
            f.write('2000-01-01 00:00:04,000 FlowID FATAL Thread C.m(C.java:23) Fatal!')
            f.write('2000-01-01 00:00:05,000 FlowID DEBUG Thread C.m(C.java:23) Debug!')
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
        reader.suppressed_log_level = 1
        reader._maybe_do_housekeeping = lambda self: None
        reader.run()
        self.assertEqual(reader.receiver.entries[0].level, LogLevel.INFO)
        self.assertEqual(reader.receiver.entries[1].level, LogLevel.ERROR)
        self.assertEqual(reader.receiver.entries[2].level, LogLevel.WARN)
        self.assertEqual(reader.receiver.entries[3].level, LogLevel.FATAL)
        self.assertEqual(reader.receiver.entries[4], 'EOF 0')
        self.assertEqual(len(reader.receiver.entries), 5)

    ### tests for _open_file() ###

    def test_open_file_with_regular_file(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
        reader._open_file()
        try:
            self.assertEqual(reader.logfile.name, 'log.log')
            self.assertFalse(reader.logfile.closed)
            self.assertNotEqual(reader.logfile_id, None)
            self.assertEqual(reader.logfile.read(), 'Some file contents!')
        finally:
            reader.logfile.close()

    def test_open_file_with_gzip_file(self):
        self.files_to_delete.append('log.gz')
        with gzip.open('log.gz', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.gz', Log4jParser(), FakeReceiver())
        reader._open_file()
        try:
            self.assertEqual(reader.logfile.name, 'log.gz')
            self.assertFalse(reader.logfile.closed)
            self.assertNotEqual(reader.logfile_id, None)
            self.assertEqual(reader.logfile.read(), 'Some file contents!')
        finally:
            reader.logfile.close()

    def test_open_file_with_nonexistent_file(self):
        reader = LogReader(0, 'no.such.file', Log4jParser(), FakeReceiver())
        self.assertRaises(IOError, reader._open_file)

    ### tests for _close_file() ###

    def test_close_file(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
        reader._open_file()
        f = reader.logfile
        reader._close_file()
        self.assertTrue(f.closed)
        self.assertEqual(reader.logfile, None)

    ### tests for _seek_first_unprocessed_position() ###

    def test_seek_first_unprocessed_position(self):
        self.write_log_file('XXXX\n' * 100)
        self.write_progress_file('log.log 123g456 50 75')
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
            reader.logfile = f
            reader._seek_first_unprocessed_position()
            self.assertEqual(f.tell(), 50)
            self.assertEqual(reader.logfile_id, '123g456')

    def test_seek_first_unprocessed_position_no_progress_file(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 20)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress', tail_length=10)
            reader.logfile = f
            reader._seek_first_unprocessed_position()
            self.assertEqual(f.tell(), 0)
            self.assertEqual(self.fake_logging.log, ['[WARN] Failed to read the progress file for "log.log".'])

    ### tests for _seek_tail() ###

    def test_seek_tail_not_enough_lines(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), tail_length=20)
            reader.logfile = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 0)

    def test_seek_tail_one_chunk(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 20)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), tail_length=10)
            reader.CHUNK_SIZE = 1024
            reader.logfile = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 10 * 75)

    def test_seek_tail_multiple_chunks(self):
        self.write_log_file('2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' * 1000)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), tail_length=900)
            reader.CHUNK_SIZE = 1024
            reader.logfile = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 100 * 75)

    def test_seek_tail_with_multiline_messages_one_chunk(self):
        message = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Error! Nooooo!\n' + 'X' * 24 + '\n'
        self.write_log_file(message * 10)
        with open('log.log', 'rb') as f:
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), tail_length=5)
            reader.CHUNK_SIZE = 1024
            reader.logfile = f
            reader._seek_tail()
            self.assertEqual(f.tell(), 5 * 100)

    ### tests for _seek_time() ###

    def test_seek_time_in_empty_file(self):
        with prepared_reader(seconds=()) as reader:
            reader._seek_time('2000-01-01 00:00:00,000')
            self.assertEqual(reader.logfile.tell(), 0)

    def test_seek_time_one_chunk_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:05,000')
            self.assertEqual(reader.logfile.tell(), 5 * 75)

    def test_seek_time_one_chunk_first_line_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:00,000')
            self.assertEqual(reader.logfile.tell(), 0)

    def test_seek_time_one_chunk_last_line_exact_match(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:09,000')
            self.assertEqual(reader.logfile.tell(), 9 * 75)

    def test_seek_time_one_chunk_between_lines(self):
        with prepared_reader(seconds=range(0, 20, 2)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:15,000')
            self.assertEqual(reader.logfile.tell(), 8 * 75)

    def test_seek_time_one_chunk_before_file(self):
        with prepared_reader(seconds=range(10, 20)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:05,000')
            self.assertEqual(reader.logfile.tell(), 0)

    def test_seek_time_one_chunk_after_file(self):
        with prepared_reader(seconds=range(10)) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:12,000')
            self.assertEqual(reader.logfile.tell(), 10 * 75)

    def test_seek_time_continuation_lines_excact_match(self):
        with prepared_reader(seconds=range(60), continuation_line_count=5) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:30,000')
            self.assertEqual(reader.logfile.tell(), 30 * 200)

    def test_seek_time_continuation_lines_between_lines(self):
        with prepared_reader(seconds=range(0, 60, 2), continuation_line_count=5) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:31,000')
            self.assertEqual(reader.logfile.tell(), 16 * 200)

    def test_seek_time_continuation_lines_before_file(self):
        with prepared_reader(seconds=range(60, 120), continuation_line_count=5) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:00:30,000')
            self.assertEqual(reader.logfile.tell(), 0)

    def test_seek_time_continuation_lines_after_file(self):
        with prepared_reader(seconds=range(60), continuation_line_count=5) as reader:
            reader.CHUNK_SIZE = 1024
            reader._seek_time('2000-01-01 00:01:30,000')
            self.assertEqual(reader.logfile.tell(), 60 * 200)

    ### tests for _maybe_do_housekeeping() ###

    def test_maybe_do_housekeeping_first_time(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._adjust_loglevel_suppression = lambda: called.append('_adjust_loglevel_suppression')
        reader._maybe_do_housekeeping(23)
        self.assertEqual(called, ['_ensure_file_is_good', '_save_progress', '_adjust_loglevel_suppression'])
        self.assertEqual(reader.last_ensure_file_is_good_call_timestamp, 23)
        self.assertEqual(reader.last_save_progress_call_timestamp, 23)
        self.assertEqual(reader.last_adjust_loglevel_suppression_call_timestamp, 23)

    def test_maybe_do_housekeeping_second_time_too_early(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._adjust_loglevel_suppression = lambda: called.append('_adjust_loglevel_suppression')
        reader.last_ensure_file_is_good_call_timestamp = 23
        reader.last_save_progress_call_timestamp = 23
        reader.last_adjust_loglevel_suppression_call_timestamp = 23
        reader._maybe_do_housekeeping(23.5)
        self.assertEqual(called, [])
        self.assertEqual(reader.last_ensure_file_is_good_call_timestamp, 23)
        self.assertEqual(reader.last_save_progress_call_timestamp, 23)
        self.assertEqual(reader.last_adjust_loglevel_suppression_call_timestamp, 23)

    def test_maybe_do_housekeeping_second_time_late_enough(self):
        called = []
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader._ensure_file_is_good = lambda: called.append('_ensure_file_is_good')
        reader._save_progress = lambda: called.append('_save_progress')
        reader._adjust_loglevel_suppression = lambda: called.append('_adjust_loglevel_suppression')
        reader.last_ensure_file_is_good_call_timestamp = 23
        reader.last_save_progress_call_timestamp = 23
        reader.last_adjust_loglevel_suppression_call_timestamp = 23
        reader._maybe_do_housekeeping(42)
        self.assertEqual(called, ['_ensure_file_is_good', '_save_progress', '_adjust_loglevel_suppression'])
        self.assertEqual(reader.last_ensure_file_is_good_call_timestamp, 42)
        self.assertEqual(reader.last_save_progress_call_timestamp, 42)
        self.assertEqual(reader.last_adjust_loglevel_suppression_call_timestamp, 42)

    ### tests for _ensure_file_is_good() ###

    def test_ensure_file_is_good_file_does_not_exist(self):
        reader = LogReader(0, 'no.such.file', Log4jParser(), FakeReceiver())
        reader._ensure_file_is_good()
        self.assertEqual(self.fake_logging.log, ['[INFO] The file no.such.file has been removed.'])

    def test_ensure_file_is_good_file_has_been_rotated(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
            reader.logfile = f
            reader.logfile_id = 'not matching'
            reader._ensure_file_is_good()
            self.assertTrue(f.closed)
            self.assertNotEqual(reader.logfile, f)
            self.assertFalse(reader.logfile.closed)
            self.assertEqual(reader.logfile.readline(), 'Some file contents!')
            self.assertEqual(self.fake_logging.log, ['[INFO] The file log.log has been rotated.',
                                                     '[INFO] Closed log.log.', '[INFO] Opened log.log.'])

    def test_ensure_file_is_good_file_has_been_truncated(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
            reader.logfile = f
            reader.logfile_id = get_device_and_inode_string(os.fstat(f.fileno()))
            f.truncate(0)
            reader._ensure_file_is_good()
            self.assertFalse(f.closed)
            self.assertEqual(reader.logfile, f)
            self.assertEqual(f.tell(), 0)
            self.assertEqual(self.fake_logging.log, ['[INFO] The file log.log has been truncated.'])

    def test_ensure_file_is_good_file_is_good(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            f.seek(10)
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
            reader.logfile = f
            reader.logfile_id = get_device_and_inode_string(os.fstat(f.fileno()))
            reader._ensure_file_is_good()
            self.assertFalse(f.closed)
            self.assertEqual(reader.logfile, f)
            self.assertEqual(f.tell(), 10)
            self.assertEqual(self.fake_logging.log, [])

    ### tests for _adjust_loglevel_suppression() ###

    def test_adjust_loglevel_suppression(self):
        fake_receiver = [1]
        reader = LogReader(0, 'log.log', Log4jParser(), fake_receiver)
        reader.START_SUPPRESSING_TRACE_ENTRIES_QUEUE_LENGTH = 2
        reader.STOP_SUPPRESSING_TRACE_ENTRIES_QUEUE_LENGTH = 1
        reader.START_SUPPRESSING_DEBUG_ENTRIES_QUEUE_LENGTH = 4
        reader.STOP_SUPPRESSING_DEBUG_ENTRIES_QUEUE_LENGTH = 3
        reader.START_SUPPRESSING_INFO_ENTRIES_QUEUE_LENGTH = 6
        reader.STOP_SUPPRESSING_INFO_ENTRIES_QUEUE_LENGTH = 5

        reader._adjust_loglevel_suppression()
        self.assertEqual(reader.suppressed_log_level, -1)

        fake_receiver.append(2)
        reader._adjust_loglevel_suppression()
        self.assertEqual(reader.suppressed_log_level, 0)

        fake_receiver.extend([3, 4])
        reader._adjust_loglevel_suppression()
        self.assertEqual(reader.suppressed_log_level, 1)

        fake_receiver.remove(4)
        reader._adjust_loglevel_suppression()
        self.assertEqual(reader.suppressed_log_level, 0)

        fake_receiver.extend([4, 5, 6])
        reader._adjust_loglevel_suppression()
        self.assertEqual(reader.suppressed_log_level, 2)


    ### tests for _save_progress() ###

    def test_save_progress_success(self):
        self.files_to_delete.append('progressf16c93d1167446f99a26837c0fdeac6fb73869794')
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader._make_progress_string = lambda: 'log.log 123g456 10 19'
        reader._save_progress()
        with open('progressf16c93d1167446f99a26837c0fdeac6fb73869794', 'rb') as f:
            self.assertEqual(f.read(), 'log.log 123g456 10 19')

    def test_save_progress_no_data(self):
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader._make_progress_string = lambda: None
        reader._save_progress()
        self.assertFalse(os.path.exists('progressf16c93d1167446f99a26837c0fdeac6fb73869794'))

    def test_save_progress_failure(self):
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='invalid\0path')
        reader._make_progress_string = lambda: 'log.log 123g456 10 19'
        reader._save_progress()
        self.assertFalse(os.path.exists('progressf16c93d1167446f99a26837c0fdeac6fb73869794'))
        self.assertEqual(self.fake_logging.log[-1], '[ERROR] Failed to save progress for log.log.')

    ### tests for _load_progress() ###

    def test_load_progress_basic(self):
        self.write_progress_file('log.log 123g456 50 75')
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        self.assertEqual(reader._load_progress(), ('log.log', '123g456', 50, 75))

    def test_load_progress_with_spaces_in_filename(self):
        self.files_to_delete.append('progressf4a53d67a02158bcc92d7d702a8f438ad18309488')
        with open('progressf4a53d67a02158bcc92d7d702a8f438ad18309488', 'wb') as f:
            f.write('log with spaces in name.log 123g456 50 75')
        reader = LogReader(0, 'log with spaces in name.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        self.assertEqual(reader._load_progress(), ('log with spaces in name.log', '123g456', 50, 75))

    ### tests for _make_progress_string() ###

    def test_make_progress_string_success(self):
        with open('log.log', 'wb') as f:
            f.write('Some file contents!')
            f.seek(10)
            reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
            reader.logfile = f
            reader.logfile_id = '123g456'
            self.assertEqual(reader._make_progress_string(), 'log.log 123g456 10 19')

    def test_make_progress_string_failure(self):
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver(), progress_file_path_prefix='progress')
        reader.logfile_id = '123g456'
        result = reader._make_progress_string()
        self.assertEqual(result, None)
        self.assertEqual(self.fake_logging.log, ['[ERROR] Failed to gather progress information for log.log.'])

    ### utility methods ###

    def write_log_file(self, *lines):
        with open('log.log', 'wb') as f:
            f.write('\n'.join(lines))

    def write_progress_file(self, contents):
        self.files_to_delete.append('progressf16c93d1167446f99a26837c0fdeac6fb73869794')
        with open('progressf16c93d1167446f99a26837c0fdeac6fb73869794', 'wb') as f:
            f.write(contents)


class LogFilterTests(TestCase):

    def test_filter_by_level(self):
        log_filter = LogFilter(levels=(LogLevel.DEBUG, LogLevel.FATAL))
        self.assertTrue(log_filter.matches(LogEntry(0, 0, 0, 0, LogLevel.DEBUG, 0, 0, 0, 0, 0, 0)))
        self.assertTrue(log_filter.matches(LogEntry(0, 0, 0, 0, LogLevel.FATAL, 0, 0, 0, 0, 0, 0)))
        self.assertFalse(log_filter.matches(LogEntry(0, 0, 0, 0, LogLevel.INFO, 0, 0, 0, 0, 0, 0)))

    def test_filter_by_grep(self):
        log_filter = LogFilter(grep='broken')
        self.assertTrue(log_filter.matches(LogEntry(0, 0, 0, 0, 0, 0, 'UnbrokenThingDoer', 0, 0, 0, 'Error!')))
        self.assertTrue(log_filter.matches(LogEntry(0, 0, 0, 0, 0, 0, 'SomeClass', 0, 0, 0, 'Stuff is broken!')))
        self.assertFalse(log_filter.matches(LogEntry(0, 0, 0, 0, 0, 0, 'BrokenThingDoer', 0, 0, 0, 'Error!')))

    def test_filter_by_time_from(self):
        log_filter = LogFilter(time_from='2000-01-01 00:30:00,000')
        self.assertTrue(log_filter.matches(LogEntry('2000-01-01 00:30:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        self.assertTrue(log_filter.matches(LogEntry('2000-01-01 01:00:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        self.assertFalse(log_filter.matches(LogEntry('2000-01-01 00:15:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))

    def test_filter_by_time_to(self):
        log_filter = LogFilter(time_to='2000-01-01 00:30:00,000')
        self.assertTrue(log_filter.matches(LogEntry('2000-01-01 00:15:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        self.assertFalse(log_filter.matches(LogEntry('2000-01-01 00:30:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
        self.assertFalse(log_filter.matches(LogEntry('2000-01-01 00:45:00,000', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))


class RedisOutputThreadTests(TestCase):

    def setUp(self):
        self.fake_logging = FakeLogging()
        logfire.logging = self.fake_logging

    def tearDown(self):
        logfire.logging = logging
        logfire.redis = redis

    def test_initialization(self):
        fake_redis = self.install_fake_redis()
        rot = RedisOutputThread('DUMMY AGGREGATOR', 'host01', 1234, 'NAMESPACE')
        self.assertEqual(rot.aggregator, 'DUMMY AGGREGATOR')
        self.assertEqual(rot._redis_namespace, 'NAMESPACE')
        self.assertTrue(rot._redis)
        self.assertTrue(rot._pipeline)
        self.assertEqual(fake_redis.log, ["StrictRedis('host01', 1234, socket_timeout=10)", "pipeline(transaction=False)"])

    def test_run_no_failures(self):
        fake_redis = self.install_fake_redis(execute_exceptions=(None, ZeroDivisionError))
        rot = RedisOutputThread(FakeLogAggregator(), 'host01', 1234, 'NAMESPACE')
        rot.REDIS_PUSH_INTERVAL = 0
        self.assertRaises(ZeroDivisionError, rot.run)
        self.assertEqual(len(fake_redis.log), 50)
        self.assertEqual(fake_redis.log[0], "StrictRedis('host01', 1234, socket_timeout=10)")
        self.assertEqual(fake_redis.log[1], "pipeline(transaction=False)")
        for block_start, count_start in ((2, 0), (26, 23)):
            for i in range(23):
                self.assertTrue(fake_redis.log[block_start + i].startswith("rpush('NAMESPACE', '{"))
                self.assertTrue('"@timestamp": "{0}"'.format(count_start + i) in fake_redis.log[block_start + i])
                self.assertTrue('"logfile": "log.log"' in fake_redis.log[block_start + i])
            self.assertEqual(fake_redis.log[block_start + 23], "execute()")

        log = self.fake_logging.log
        self.assertEqual(log[0], '[DEBUG] Starting to push entries to Redis.')
        self.assertTrue(log[1].startswith('[DEBUG] Pushed 23 entries to Redis.'))
        self.assertEqual(len(log), 2)

    def test_run_one_connection_failure(self):
        fake_redis = self.install_fake_redis(execute_exceptions=(redis.exceptions.RedisError, None, ZeroDivisionError))
        rot = RedisOutputThread(FakeLogAggregator(), 'host01', 1234, 'NAMESPACE')
        rot.REDIS_PUSH_INTERVAL = 0
        rot.REDIS_ERROR_RETRY_DELAY = 0
        self.assertRaises(ZeroDivisionError, rot.run)
        self.assertEqual(len(fake_redis.log), 74)
        self.assertEqual(fake_redis.log[0], "StrictRedis('host01', 1234, socket_timeout=10)")
        self.assertEqual(fake_redis.log[1], "pipeline(transaction=False)")
        for block_start, count_start in ((2, 0), (26, 0), (50, 23)):
            for i in range(23):
                self.assertTrue(fake_redis.log[block_start + i].startswith("rpush('NAMESPACE', '{"))
                self.assertTrue('"@timestamp": "{0}"'.format(count_start + i) in fake_redis.log[block_start + i])
                self.assertTrue('"logfile": "log.log"' in fake_redis.log[block_start + i])
            self.assertEqual(fake_redis.log[block_start + 23], "execute()")

        log = self.fake_logging.log
        self.assertEqual(log[0], '[DEBUG] Starting to push entries to Redis.')
        self.assertEqual(log[1], '[ERROR] Failed to push entries to Redis. Will retry in 0 seconds.')
        self.assertEqual(log[2], '[INFO] There are 46 pushable entries queued.')
        self.assertTrue(log[3].startswith('[DEBUG] Pushed 23 entries to Redis.'))
        self.assertEqual(len(log), 4)

    def install_fake_redis(self, *args, **kwargs):
        fake_redis = FakeRedis(*args, **kwargs)
        logfire.redis = fake_redis
        return fake_redis


class LogAggregatorTests(TestCase):

    def test_non_ordered_log_aggregator(self):
        aggregator = NonOrderedLogAggregator([])
        aggregator.add(2)
        aggregator.add(1)
        aggregator.add(3)
        self.assertEqual(len(aggregator), 3)
        self.assertEqual(list(aggregator.get()), [2, 1, 3])
        self.assertEqual(len(aggregator), 0)

    def test_ordered_log_aggregator(self):
        aggregator = OrderedLogAggregator([])
        aggregator.add(2)
        aggregator.add(1)
        aggregator.add(3)
        self.assertEqual(len(aggregator), 3)
        self.assertEqual(list(aggregator.get()), [1, 2, 3])
        self.assertEqual(len(aggregator), 0)

    def test_non_ordered_log_aggregator_eof(self):
        aggregator = NonOrderedLogAggregator(['log.log', 'another.log'])
        self.assertEqual(aggregator.open_files, set([0, 1]))
        aggregator.eof(0)
        self.assertEqual(aggregator.open_files, set([1]))

    def test_ordered_log_aggregator_eof(self):
        aggregator = OrderedLogAggregator(['log.log', 'another.log'])
        self.assertEqual(aggregator.open_files, set([0, 1]))
        aggregator.eof(0)
        self.assertEqual(aggregator.open_files, set([1]))



class MiscellaneousTests(TestCase):

    def test_loglevel_from_first_letter(self):
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['T'], LogLevel.TRACE)
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['D'], LogLevel.DEBUG)
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['I'], LogLevel.INFO)
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['W'], LogLevel.WARN)
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['E'], LogLevel.ERROR)
        self.assertEqual(LogLevel.FROM_FIRST_LETTER['F'], LogLevel.FATAL)

    def test_loglevel_stringification(self):
        self.assertEqual(str(LogLevel.ERROR), 'ERROR')
        self.assertEqual(repr(LogLevel.ERROR), 'ERROR')

    def test_log_entry_as_logstash(self):
        entry = LogEntry('2000-01-01 00:00:00,000', 0, 1000, 'FlowID', LogLevel.WARN, 'Thread', 'ThingDoer', 'doThing',
                         'ThingDoer.java', 2, 'Problem!')
        expected = {'@timestamp': '2000-01-01 00:00:00,000', 'flowid': 'FlowID', 'level': 'WARN', 'thread': 'Thread',
                    'class': 'ThingDoer', 'method': 'doThing', 'file': 'ThingDoer.java', 'line': 2,
                    'message': 'Problem!', 'logfile': 'log.log'}
        self.assertEqual(entry.as_logstash('log.log'), expected)


class FakeReceiver(object):

    def __init__(self):
        self.entries = []

    def __len__(self):
        return len(self.entries)

    def add(self, entry):
        self.entries.append(entry)

    def eof(self, fid):
        self.entries.append('EOF {0}'.format(fid))


class FakeLogAggregator(object):

    def __init__(self):
        self.file_names = {'123g456': 'log.log'}
        self.entry_count = -1

    def __len__(self):
        return 23

    def get(self):
        while True:
            self.entry_count += 1
            yield LogEntry(str(self.entry_count), '123g456', 0, 0, 0, 0, 0, 0, 0, 0, '\xbf')


class FakeRedis(object):

    exceptions = redis.exceptions

    def __init__(self, execute_exceptions=(ZeroDivisionError,)):
        self.log = []
        self.execute_exceptions = list(execute_exceptions)

    def StrictRedis(self, host, port, socket_timeout=None):
        self.log.append('StrictRedis({0!r}, {1!r}, socket_timeout={2!r})'.format(host, port, socket_timeout))
        return self

    def pipeline(self, transaction=None):
        self.log.append('pipeline(transaction={0!r})'.format(transaction))
        return self

    def rpush(self, namespace, data):
        self.log.append('rpush({0!r}, {1!r})'.format(namespace, data))

    def execute(self):
        self.log.append('execute()')
        exception = self.execute_exceptions[0]
        self.execute_exceptions = self.execute_exceptions[1:]
        if exception:
            raise exception()


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
        reader = LogReader(0, 'log.log', Log4jParser(), FakeReceiver())
        reader.logfile = self.log
        return reader

    def __exit__(self, *args):
        self.log.close()


class FakeLogging(object):
    """Can be patched in for the logging module. Stores all logging messages in log."""

    def __init__(self):
        self.reset()

    def debug(self, msg, *args, **kwargs):
        self.add('DEBUG', msg, *args)

    def info(self, msg, *args, **kwargs):
        self.add('INFO', msg, *args)

    def warn(self, msg, *args, **kwargs):
        self.add('WARN', msg, *args)

    def warning(self, msg, *args, **kwargs):
        self.add('WARN', msg, *args)

    def exception(self, msg, *args, **kwargs):
        self.add('ERROR', msg, *args)

    def critical(self, msg, *args, **kwargs):
        self.add('CRITICAL', msg, *args)

    def add(self, level, message, *args):
        if len(args) == 1 and isinstance(args[0], dict):
            args = args[0]
        self.log.append('[%s] %s' % (level, message % args))

    def reset(self):
        self.log = []
