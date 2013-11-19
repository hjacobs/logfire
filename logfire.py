#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import heapq
import itertools
import json
import logging
import os
import signal
import sys
import time
from threading import Thread
from argparse import ArgumentParser

from logreader import LogReader, LogFilter

try:
    import redis
except ImportError:  #pragma: nocover
    pass  # The module might not actually be required.

LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'


class LogLevel(object):

    FROM_FIRST_LETTER = {}

    def __init__(self, priority, name):
        self.priority = priority
        self.name = name
        LogLevel.FROM_FIRST_LETTER[name[0]] = self

    def __repr__(self):
        return self.name


LogLevel.TRACE = LogLevel(0, 'TRACE')
LogLevel.DEBUG = LogLevel(1, 'DEBUG')
LogLevel.INFO = LogLevel(2, 'INFO')
LogLevel.WARN = LogLevel(3, 'WARN')
LogLevel.ERROR = LogLevel(4, 'ERROR')
LogLevel.FATAL = LogLevel(5, 'FATAL')


LOG_ENTRY_FIELDS = 'timestamp reader_id entry_number flow_id level thread class_ method source_file line message'

class LogEntry(collections.namedtuple('LogEntry', LOG_ENTRY_FIELDS)):

    __slots__ = ()

    def as_logstash(self, logfile_name):
        return {
            '@timestamp': self.timestamp,
            'flowid': self.flow_id,
            'level': str(self.level),
            'thread': self.thread,
            'class': self.class_,
            'method': self.method,
            'file': self.source_file,
            'line': self.line,
            'message': self.message,
            'logfile': logfile_name
        }


class Log4jParser(object):

    def __init__(self):
        # default pattern: %d %x %p %t %l: %m%n
        self.delimiter = ' '
        self.column_count = 5
        self.flow_id_column_index = 0
        self.level_column_index = 1
        self.thread_column_index = 2
        self.location_column_index = 3
        self.message_column_index = 4

    def autoconfigure(self, logfile):
        """try to autoconfigure the parser"""
        # TODO: Document that this method will fail if the given file does not yet contain any log entries.

        sample_line = logfile.readline()
        logfile.seek(0)

        # We assume that the delimiter is always a single space character.
        self.delimiter = ' '

        # We assume the date column (%d) is always the first column and do not assign an index to it.
        # The column immediately following the date column has index 0.
        parts = sample_line[24:].split(self.delimiter)

        # The code location column (%l) has a very distinctive format, usually, so we can search for it.
        # We assume it is the second, third, or fourth column after the date column
        for column_index, p in enumerate(parts[1:4], start=1):
            if self._is_valid_code_position(p):
                self.location_column_index = column_index
                break
        else:
            raise Exception('Cannot auto-configure the parser. There does not seem to be a code location.')

        # We assume that the message column (%m) comes immediately after the code location column,
        # and that it is the last column.
        self.message_column_index = self.location_column_index + 1
        self.column_count = self.message_column_index + 1

        # We assume that, if there are exactly three columns (not counting the date column), the first
        # column after the date column is the priority column (%p), and that there are no thread (%t)
        # or flow ID (%x) columns.
        if self.column_count == 3:
            self.level_column_index = 0
            self.thread_column_index = None
            self.flow_id_column_index = None

        # We assume that, if there are exactly four columns (not counting the date column), the first
        # two column after the date column are the priority column (%p) and the thread column (%t),
        # in that order, and that there is no flow ID (%x) column.
        if self.column_count == 4:
            self.level_column_index = 0
            self.thread_column_index = 1
            self.flow_id_column_index = None

        # We assume that, if there are exactly five columns (not counting the date column), the first
        # three column after the date column are the flow ID column (%x), the priority column (%p),
        # and the thread column (%t), in that order.
        if self.column_count == 5:
            self.flow_id_column_index = 0
            self.level_column_index = 1
            self.thread_column_index = 2

        # There cannot be more than five columns (not counting the date column).
        return

    def read(self, reader_id, logfile):
        """read log4j formatted log file"""

        assert 'b' in getattr(logfile, 'mode', 'rb'), 'The file has not been opened in binary mode.'

        maxsplit = self.column_count - 1
        delimiter = self.delimiter
        flow_id_column_index = self.flow_id_column_index
        level_column_index = self.level_column_index
        thread_column_index = self.thread_column_index
        location_column_index = self.location_column_index
        message_column_index = self.message_column_index

        entry_number = 0
        while True:
            line = logfile.readline()
            if not line:
                break
            try:
                timestamp = line[:23]
                if not timestamp.startswith('20'):
                    logging.warn('Skipped a line because it does not appear to start with a date: "%s".', line)
                    continue
                columns = line[24:].split(delimiter, maxsplit)
                if len(columns) < self.column_count:
                    logging.warn('Skipped a line because it does not have a sufficient number of columns: "%s".', line)
                    continue
                level = self._read_log_level(columns, level_column_index)
                flow_id = self._read_flow_id(columns, flow_id_column_index)
                thread = self._read_thread(columns, thread_column_index)
                class_, method, source_file, line_number = self._read_code_position(columns, location_column_index)
                message = self._read_message(columns, message_column_index, logfile)
            except Exception:  #pragma: nocover
                # This shouldn't actually be possible.
                logging.exception('Failed to parse line "%s" of %s', line, reader_id)
            else:
                yield LogEntry(
                    reader_id=reader_id,
                    timestamp=timestamp,
                    entry_number=entry_number,
                    flow_id=flow_id,
                    level=level,
                    thread=thread,
                    class_=class_,
                    method=method,
                    source_file=source_file,
                    line=line_number,
                    message=message,
                )
            entry_number += 1

    def get_time_string(self, line):
        if self.is_continuation_line(line):
            raise Exception('Continuation lines do not have time strings.')
        else:
            return line[:23]

    def _read_log_level(self, columns, index):
        return LogLevel.FROM_FIRST_LETTER.get(columns[index].lstrip('[')[:1], LogLevel.FATAL)

    def _read_flow_id(self, columns, index):
        if index is None:
            return None
        else:
            return columns[index].rstrip(':')

    def _read_thread(self, columns, index):
        if index is None:
            return None
        else:
            return columns[index].rstrip(':')

    def _read_code_position(self, columns, index):
        return self._split_code_position(columns[index])

    def _is_valid_code_position(self, string):
        class_, method, source_file, line_number = self._split_code_position(string)
        return bool(class_ and method and source_file and line_number != -1)

    def _split_code_position(self, string):
        class_and_method, _, file_and_line_number = string.rstrip(':)').rpartition('(')
        class_, _, method = class_and_method.rpartition('.')
        source_file, _, line_number = file_and_line_number.partition(':')
        return class_, method, source_file, try_parsing_int(line_number, default=-1)

    def _read_message(self, columns, index, logfile):
        lines = [columns[index]]
        while True:
            l = logfile.readline()
            if self.is_continuation_line(l):
                lines.append(l)
            else:
                logfile.seek(-len(l), os.SEEK_CUR)
                return ''.join(lines).rstrip()

    def is_continuation_line(self, line):
        return line and not (line.startswith('20') and line[23:24] == ' ')


def try_parsing_int(string, default=None):
    try:
        return int(string)
    except ValueError:
        return default


class Watcher:

    """this class solves two problems with multithreaded
    programs in Python, (1) a signal might be delivered
    to any thread (which is just a malfeature) and (2) if
    the thread that gets the signal is waiting, the signal
    is ignored (which is a bug).

    The watcher is a concurrent process (not thread) that
    waits for a signal and the process that contains the
    threads.  See Appendix A of The Little Book of Semaphores.
    http://greenteapress.com/semaphores/

    I have only tested this on Linux.  I would expect it to
    work on the Macintosh and not work on Windows.
    """

    def __init__(self):
        """ Creates a child thread, which returns.  The parent
            thread waits for a KeyboardInterrupt and then kills
            the child thread.
        """

        self.child = os.fork()
        if self.child == 0:
            return
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            # I put the capital B in KeyBoardInterrupt so I can
            # tell when the Watcher gets the SIGINT
            print '\033[0m' + 'KeyBoardInterrupt'
            self.kill()
        sys.exit()

    def kill(self):
        try:
            os.kill(self.child, signal.SIGKILL)
        except OSError:
            pass


class OrderedLogAggregator(object):

    def __init__(self, file_names):
        self.entries = []
        self.file_names = file_names
        self.open_files = set(range(len(file_names)))

    def add(self, entry):
        heapq.heappush(self.entries, entry)

    def eof(self, fid):
        self.open_files.remove(fid)

    def __len__(self):
        return len(self.entries)

    def get(self):
        while self.open_files or self.entries:
            try:
                yield heapq.heappop(self.entries)
            except IndexError:
                pass


class NonOrderedLogAggregator(object):

    def __init__(self, file_names):
        self.entries = collections.deque()
        self.file_names = file_names
        self.open_files = set(range(len(file_names)))

    def add(self, entry):
        self.entries.append(entry)

    def eof(self, fid):
        self.open_files.remove(fid)

    def __len__(self):
        return len(self.entries)

    def get(self):
        while True:
            try:
                yield self.entries.popleft()
            except IndexError:
                return


COLORS = [
    '\033[31m',
    '\033[32m',
    '\033[33m',
    '\033[34m',
    '\033[35m',
    '\033[36m',
    '\033[37m',
]


class RedisOutputThread(Thread):

    REDIS_PUSH_INTERVAL = 1  # seconds
    REDIS_ERROR_RETRY_DELAY = 5  # seconds
    MAX_CHUNK_SIZE = 1000  # entries

    def __init__(self, aggregator, host, port, namespace):
        Thread.__init__(self, name='RedisOutputThread')
        self.aggregator = aggregator
        self._redis_namespace = namespace
        self._redis = redis.StrictRedis(host, port, socket_timeout=10)
        self._pipeline = self._redis.pipeline(transaction=False)

    def run(self):
        # Performance!
        namespace = self._redis_namespace
        pipeline = self._pipeline
        file_name_by_id = self.aggregator.file_names

        logging.debug('Starting to push entries to Redis.')
        last_report_timestamp = time.time()
        time.sleep(self.REDIS_PUSH_INTERVAL)

        while True:
            poppable_entry_count = min(self.MAX_CHUNK_SIZE, len(self.aggregator))

            if poppable_entry_count > 0:
                log_entries = itertools.islice(self.aggregator.get(), poppable_entry_count)
                json_strings = [json.dumps(e.as_logstash(file_name_by_id[e.reader_id])) for e in log_entries]

                while True:
                    for j in json_strings:
                        pipeline.rpush(namespace, j)
                    try:
                        pipeline.execute()
                    except redis.exceptions.RedisError:
                        message = 'Failed to push entries to Redis. Will retry in %d seconds.'
                        logging.exception(message, self.REDIS_ERROR_RETRY_DELAY)
                        logging.info('There are %s pushable entries queued.', len(json_strings) + len(self.aggregator))
                        time.sleep(self.REDIS_ERROR_RETRY_DELAY)
                    else:
                        break

            if poppable_entry_count < self.MAX_CHUNK_SIZE:
                now = time.time()
                message = 'Pushed %d entries to Redis. (%.1f entries/s; queue length %d)'
                entries_per_second = len(json_strings) / (now - last_report_timestamp)
                logging.debug(message, len(json_strings), entries_per_second, len(self.aggregator))
                last_report_timestamp = now
                time.sleep(self.REDIS_PUSH_INTERVAL)


class OutputThread(Thread):

    def __init__(self, aggregator, fd=sys.stdout, collapse=False, truncate=0):
        Thread.__init__(self, name='OutputThread')
        self.aggregator = aggregator
        self.fd = fd
        self.collapse = collapse
        self.truncate = truncate

    def run(self):
        fd = self.fd
        collapse = self.collapse
        trunc = self.truncate
        file_names = self.aggregator.file_names
        for entry in self.aggregator.get():
            fd.write(file_names[entry.reader_id] + ' ')
            fd.write('\033[97m')
            # do not print year:
            fd.write(entry.timestamp[5:] + ' ')
            if entry.level == LogLevel.FATAL:
                fd.write('\033[95m')
            elif entry.level == LogLevel.ERROR:
                fd.write('\033[91m')
            elif entry.level == LogLevel.WARN:
                fd.write('\033[93m')
            elif entry.level == LogLevel.INFO:
                fd.write('\033[92m')
            else:
                fd.write('\033[94m')
            fd.write(str(entry.level))
            fd.write('\033[0m')
            msg = entry.message
            if collapse:
                msg = msg.replace('\n', '\\n')
            if trunc and len(msg) > trunc:
                msg = msg[:trunc].rsplit(' ', 1)[0] + '...'
            if entry.flow_id:
                fd.write(COLORS[hash(entry.flow_id) % 7])
                fd.write(' ' + entry.flow_id[:2] + '-' + entry.flow_id[-2:])
                fd.write('\033[0m')
            else:
                fd.write('   -  ')
            fd.write(' ' + (entry.thread or '-'))
            fd.write(' ' + entry.class_)
            fd.write('.' + entry.method)
            fd.write(' ' + entry.source_file)
            fd.write(':' + str(entry.line))
            if entry.level == LogLevel.FATAL:
                fd.write('\033[95m')
            elif entry.level == LogLevel.ERROR:
                fd.write('\033[91m')
            elif entry.level == LogLevel.WARN:
                fd.write('\033[93m')
            elif entry.level == LogLevel.INFO:
                fd.write('\033[92m')
            else:
                fd.write('\033[94m')
            fd.write(' ' + msg)
            fd.write('\033[0m ')
            fd.write('\n')


def main():
    Watcher()
    parser = ArgumentParser()
    parser.add_argument('files', nargs='+', help='use custom configuration profile (more than one profile allowed)')
    parser.add_argument('-p', '--profile', help='use custom configuration profile (more than one profile allowed)')
    parser.add_argument('-f', '--follow', action='store_true', help='keep file open reading new lines (like tail)')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose mode')
    parser.add_argument('-n', '--lines', dest='tail_lines', default=100, type=int, metavar='N',
                        help='show last N lines (instead of default 100)')
    parser.add_argument('-c', '--collapse', action='store_true',
                        help='collapse multi-line entries (i.e. each log entry is a single line)')
    parser.add_argument('--truncate', metavar='CHARS', type=int, help='truncate log message to CHARS characters')
    parser.add_argument('-l', '--levels', help='only show log entries with log level(s)')
    parser.add_argument('-g', '--grep', metavar='PATTERN', help='only show log entries matching pattern')
    parser.add_argument('--time-to', metavar='DATETIME', help='only show log entries until DATETIME')
    parser.add_argument('--redis-host', help='redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='redis port')
    parser.add_argument('--redis-namespace', help='redis namespace')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--tail', action='store_true', help='show last N lines (default 100)')
    group.add_argument('--time-from', metavar='DATETIME', help='only show log entries starting at DATETIME')
    group.add_argument('--sincedb', help='sincedb path')

    args = parser.parse_args()

    logging.basicConfig(level=(logging.DEBUG if args.verbose else logging.INFO), format=LOG_FORMAT)

    file_names = args.files

    config_file = os.path.expanduser('~/.logfirerc')
    if not os.path.isfile(config_file):
        # fallback using global configuration file
        config_file = '/etc/logfirerc'
    if os.path.isfile(config_file):
        config = json.load(open(config_file, 'rb'))
        merged_config = {'options': {}, 'files': []}
        active_profiles = ['default']
        if args.profile:
            active_profiles += args.profile.split(',')
        for profile in active_profiles:
            if config.get(profile):
                merged_config['options'].update(config[profile].get('options', {}))
                if config[profile].get('files'):
                    merged_config['files'] += config[profile].get('files')

        for key, val in merged_config['options'].items():
            if not getattr(args, key, None):
                setattr(args, key, val)
        if not args.files:
            file_names = merged_config['files']

    filterdef = LogFilter()
    filterdef.grep = args.grep
    filterdef.time_from = args.time_from
    filterdef.time_to = args.time_to

    if args.levels:
        for lvl in args.levels.split(','):
            lo = getattr(LogLevel, lvl)
            filterdef.levels.add(lo)

    tail_lines = 0
    if args.tail:
        tail_lines = int(args.tail_lines)

    used_file_names = set()
    if args.redis_host:
        aggregator = NonOrderedLogAggregator(file_names)
    else:
        aggregator = OrderedLogAggregator(file_names)
    readers = []
    fid = 0
    for fname_with_name in file_names:
        if ':' in fname_with_name:
            name, unused, fpath = fname_with_name.partition(':')
        else:
            fpath = fname_with_name
            name, ext = os.path.splitext(os.path.basename(fpath))
            name = name[-4:].upper()
        i = 1
        while name in used_file_names:
            name = name + str(i)
            i += 1
        if not args.redis_host:
            file_names[fid] = name
        used_file_names.add(name)
        parser = Log4jParser()
        readers.append(LogReader(
            fid,
            fpath,
            parser,
            aggregator,
            tail_length=tail_lines,
            follow=args.follow,
            entry_filter=filterdef,
            progress_file_path_prefix=args.sincedb,
        ))
        fid += 1
    for reader in readers:
        reader.start()
    if args.redis_host:
        out = RedisOutputThread(aggregator, args.redis_host, args.redis_port, args.redis_namespace)
    else:
        out = OutputThread(aggregator, collapse=args.collapse, truncate=args.truncate)
    out.start()


if __name__ == '__main__':  #pragma: nocover
    main()

