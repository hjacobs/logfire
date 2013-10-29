#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import errno
import gzip
import hashlib
import heapq
import io
import json
import logging
import os
import signal
import sys
import time
import traceback
from threading import Thread
from argparse import ArgumentParser

LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'


class LogLevel(object):

    FROM_FIRST_LETTER = {}

    def __init__(self, priority, name):
        self.priority = priority
        self.name = name
        LogLevel.FROM_FIRST_LETTER[name[0]] = self

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


LogLevel.TRACE = LogLevel(0, 'TRACE')
LogLevel.DEBUG = LogLevel(1, 'DEBUG')
LogLevel.INFO = LogLevel(2, 'INFO')
LogLevel.WARN = LogLevel(3, 'WARN')
LogLevel.ERROR = LogLevel(4, 'ERROR')
LogLevel.FATAL = LogLevel(5, 'FATAL')


class LogEntry(collections.namedtuple('LogEntry', 'ts fid i flowid level thread clazz method file line message')):

    def as_logstash(self):
        d = self._asdict()
        d['@timestamp'] = self.ts
        d['level'] = str(self.level)
        d['class'] = self.clazz
        del d['clazz']
        del d['fid']
        del d['i']
        del d['ts']
        return d


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

    def read(self, fid, logfile):
        """read log4j formatted log file"""

        assert 'b' in getattr(logfile, 'mode', 'rb'), 'The file has not been opened in binary mode.'

        maxsplit = self.column_count - 1
        delimiter = self.delimiter
        flow_id_column_index = self.flow_id_column_index
        level_column_index = self.level_column_index
        thread_column_index = self.thread_column_index
        location_column_index = self.location_column_index
        message_column_index = self.message_column_index

        i = 0
        while True:
            line = logfile.readline()
            if not line:
                break
            try:
                ts = line[:23]
                if not ts.startswith('20'):
                    logging.warn('Skipped a line because it does not appear to start with a date: "%s".', line)
                    continue
                columns = line[24:].split(delimiter, maxsplit)
                if len(columns) < self.column_count:
                    logging.warn('Skipped a line because it does not have a sufficient number of columns: "%s".', line)
                    continue
                level = self._read_log_level(columns, level_column_index)
                flow_id = self._read_flow_id(columns, flow_id_column_index)
                thread = self._read_thread(columns, thread_column_index)
                class_, method, file_, line_number = self._read_code_position(columns, location_column_index)
                message = self._read_message(columns, message_column_index, logfile)
            except Exception:  #pragma: nocover
                # This shouldn't actually be possible.
                logging.exception('Failed to parse line "%s" of %s', line, fid)
            else:
                yield LogEntry(
                    fid=fid,
                    ts=ts,
                    i=i,
                    flowid=flow_id,
                    level=level,
                    thread=thread,
                    clazz=class_,
                    method=method,
                    file=file_,
                    line=line_number,
                    message=message,
                )
            i += 1

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
        class_, method, file_, line_number = self._split_code_position(string)
        return bool(class_ and method and file_ and line_number != -1)

    def _split_code_position(self, string):
        class_and_method, _, file_and_line_number = string.rstrip(':)').rpartition('(')
        class_, _, method = class_and_method.rpartition('.')
        file_, _, line_number = file_and_line_number.partition(':')
        return class_, method, file_, try_parsing_int(line_number, default=-1)

    def _read_message(self, columns, index, logfile):
        lines = [columns[index]]
        while True:
            l = logfile.readline()
            if self._is_continuation_line(l):
                lines.append(l)
            else:
                logfile.seek(-len(l), os.SEEK_CUR)
                return ''.join(lines).rstrip()

    def _is_continuation_line(self, line):
        return line and not (line.startswith('20') and line[23:24] == ' ')


def try_parsing_int(string, default=None):
    try:
        return int(string)
    except ValueError:
        return default


def parse_timestamp(ts):
    """takes a timestamp such as 2011-09-18 16:00:01,123"""

    if len(ts) < 19:
        ts += ':00'
    struct = time.strptime(ts[:19], '%Y-%m-%d %H:%M:%S')
    return time.mktime(struct)


class LogReader(Thread):

    def __init__(
        self,
        fid,
        fname,
        parser,
        receiver,
        tail=0,
        follow=False,
        filterdef=None,
        sincedb=None,
    ):

        Thread.__init__(self, name='LogReader-%d' % (fid, ))
        self.fid = fid
        self._filename = fname
        self._fhash = hashlib.sha1(fname).hexdigest()
        self.parser = parser
        self.receiver = receiver
        self.tail = tail
        self.follow = follow
        self.filterdef = filterdef or LogFilter()
        self._last_file_mapping_update = None
        self._stat_interval = 2
        self._sincedb_write_interval = 5
        self._fid = None
        self._file = None
        self._first = False
        self._sincedb_path = sincedb
        self._last_sincedb_write = None

    def _seek_position(self):
        """seek to start of "tail" (last n lines)"""
        self._seek_sincedb_position() or self._seek_tail()

    def _seek_sincedb_position(self):
        if self._sincedb_path:
            try:
                with open(self._sincedb()) as sincedb_file:
                    _, fid, last_position, _ = sincedb_file.read().split()
                last_position = int(last_position)
                fid = int(fid)
            except Exception:
                logging.warning('Failed to read the sincedb file for "%s".', self._filename)
                return False
            else:
                logging.info('Resumed reading "%s" at offset %d.', self._filename, last_position)
                self._fid = fid
                self._file.seek(last_position)
                return True
        else:
            return False

    def _seek_tail(self):
        n = self.tail
        logging.debug('Seeking to %s tail lines', n)
        l = os.path.getsize(self._filename)
        s = -1024 * n
        if s * -1 >= l:
            # apparently the file is too small
            # => seek to start of file
            logging.debug('file too small')
            self._file.seek(0)
            return
        self._file.seek(s, 2)
        t = self._file.tell()
        contents = self._file.read()
        e = len(contents)
        i = 0
        while e >= 0:
            e = contents.rfind('\n', 0, e)
            if e >= 0:
                i += 1
                if i >= n:
                    self._file.seek(t + e)
                    break      

    def _seek_time(self, fd, ts):
        """try to seek to our start time"""

        s = os.path.getsize(self._filename)
        fd.seek(0)

        if s < 8192:
            # file is too small => we do not need to seek around
            return

        file_start = None
        for entry in self.parser.read(0, fd):
            file_start = entry.ts
            break

        fd.seek(-1024, 2)
        file_end = None
        for entry in self.parser.read(0, fd):
            file_end = entry.ts
            break

        if not file_start or not file_end:
            fd.seek(0)
            return

        start = parse_timestamp(file_start)
        t = parse_timestamp(ts)
        end = parse_timestamp(file_end)

        if end - start <= 0:
            fd.seek(0)
            return

        ratio = max(0, (t - start) / (end - start) - 0.2)
        fd.seek(s * ratio)

    def run(self):
        fid = self.fid
        receiver = self.receiver
        filt = self.filterdef
        self._update_file()
        self.parser.autoconfigure(self._file)
        self._update_file()
        if filt.time_from:
            self._seek_time(self._file, filt.time_from)
        while True:
            where = self._file.tell()
            had_entry = False
            for entry in self.parser.read(fid, self._file):
                if filt.matches(entry):
                    if self._first:
                        logging.debug('First entry: %s', entry.ts)
                        self._first = False
                    # print entry.ts
                    receiver.add(entry)
                # print entry.ts, entry.level, entry.thread, entry.source_class, entry.source_location, entry.message
                self._ensure_file_is_good(time.time())
                had_entry = True
            if not self.follow:
                receiver.eof(fid)
                break
            if not had_entry:
                time.sleep(0.1)
                if self._ensure_file_is_good(time.time()):
                    self._file.seek(where)

    def open(self, encoding=None):
        """Opens the file with the appropriate call"""

        logging.info('Opening %s..', self._filename)
        try:
            if self._filename.endswith('.gz'):
                _file = gzip.open(self._filename, 'rb')
            else:
                _file = io.open(self._filename, 'rb')
        except IOError:
            logging.exception('Failed to open %s', self._filename)
            raise
            # logging.warn(str(e))
            # _file = None
            # self.close()
        self._first = True

        return _file

    @staticmethod
    def get_file_id(st):
        return '%xg%x' % (st.st_dev, st.st_ino)

    def _update_file(self, seek_to_end=True):
        """Open the file for tailing"""

        try:
            self.close()
            self._file = self.open()
        except IOError:
            raise
        try:
            st = os.stat(self._filename)
        except EnvironmentError, err:
            if err.errno == errno.ENOENT:
                logging.info('file removed')
                self.close()
        self._fid = self.get_file_id(st)
        if seek_to_end and self.tail:
            self._seek_position()

    def close(self):
        """Closes all currently open file pointers"""

        self.active = False
        if self._file:
            self._file.close()

    def _sincedb(self):
        return '{}f{}'.format(self._sincedb_path, self._fhash)

    def _ensure_file_is_good(self, current_time):
        """Every N seconds, ensures that the file we are tailing is the file we expect to be tailing"""

        if self._last_file_mapping_update and current_time - self._last_file_mapping_update <= self._stat_interval:
            return True

        self._last_file_mapping_update = current_time

        try:
            st = os.stat(self._filename)
        except EnvironmentError, err:
            if err.errno == errno.ENOENT:
                logging.info('file removed')
                return

        fid = self.get_file_id(st)
        cur_pos = self._file.tell()
        if fid != self._fid:
            logging.info('file "%s" rotated', self._filename)
            self._update_file(seek_to_end=False)
            return False
        elif cur_pos > st.st_size:
            if st.st_size == 0 and self._ignore_truncate:
                logging.info('[{0}] - file size is 0 {1}. '.format(fid, self._filename)
                             + 'If you use another tool (i.e. logrotate) to truncate '
                             + 'the file, your application may continue to write to '
                             + "the offset it last wrote later. In such a case, we'd " + 'better do nothing here')
                return
            logging.info('file "%s" truncated', self._filename)
            self._update_file(seek_to_end=False)
            return False
        if self._sincedb_path and (not self._last_sincedb_write or current_time - self._last_sincedb_write
                                   > self._sincedb_write_interval):
            self._last_sincedb_write = current_time
            path = self._sincedb()
            logging.debug('Writing sincedb for %s: %s of %s (%s Bytes to go)', self._filename, cur_pos, st.st_size,
                          st.st_size - cur_pos)
            try:
                with open(path, 'wb') as fd:
                    fd.write(' '.join((self._filename, self._fid, str(cur_pos), str(st.st_size))))
            except:
                logging.exception('Failed to write to %s', path)
        return True


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


class LogAggregator(object):

    def __init__(self, file_names, sleep=0.5):
        self.file_names = file_names
        n = len(file_names)
        self.entries = []
        self.open_files = set(range(n))
        self._sleep = sleep

    def add(self, entry):
        heapq.heappush(self.entries, entry)

        # if
        # print self.entries[-10:]
        # print entry.fid, entry.ts, entry.level, entry.thread, entry.source_class, entry.source_location, entry.message

    def eof(self, fid):
        self.open_files.remove(fid)

    def get(self):
        while self.open_files or self.entries:
            try:
                entry = heapq.heappop(self.entries)
                yield entry
            except IndexError:
                if self._sleep:
                    logging.debug('Sleeping %ss..', self._sleep)
                    time.sleep(self._sleep)
                pass


class NonOrderedLogAggregator(object):

    def __init__(self, file_names, sleep=0.5):
        self.file_names = file_names
        n = len(file_names)
        self.entries = collections.deque()
        self.open_files = set(range(n))
        self._sleep = sleep

    def add(self, entry):
        self.entries.append(entry)

    def eof(self, fid):
        self.open_files.remove(fid)

    def len(self):
        return len(self.entries)

    def get(self):
        while True:
            try:
                entry = self.entries.popleft()
                yield entry
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

    def __init__(self, aggregator, host, port, namespace):
        Thread.__init__(self, name='RedisOutputThread')
        self.aggregator = aggregator
        self._redis_namespace = namespace
        import redis
        self._redis = redis.StrictRedis(host, port, socket_timeout=10)
        self._connect()

    def _connect(self):
        wait = -1
        while True:
            wait += 1
            time.sleep(wait)
            if wait == 20:
                return False

            if wait > 0:
                logging.info('Retrying connection, attempt {0}'.format(wait + 1))

            try:
                self._redis.ping()
                break
            except UserWarning:
                traceback.print_exc()
            except Exception:
                traceback.print_exc()

        self._pipeline = self._redis.pipeline(transaction=False)

    def run(self):
        file_names = self.aggregator.file_names

        total = 0
        chunk_start = time.time()
        write_interval = 1
        while True:
            time.sleep(write_interval)
            l = self.aggregator.len()
            if l > 0:
                i = 0
                for entry in self.aggregator.get():
                    d = entry.as_logstash()
                    d['logfile'] = file_names[entry.fid]
                    self._pipeline.rpush(self._redis_namespace, json.dumps(d))
                    i += 1
                    if i > l:
                        break
                total += i
                try:
                    self._pipeline.execute()
                except:
                    logging.exception('Redis connection failure')
                now = time.time()
                logging.debug('Pushed %s entries (%.1f/s), queue length %s', i, i / (now - chunk_start),
                              self.aggregator.len())
                chunk_start = now


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
            fd.write(file_names[entry.fid] + ' ')
            fd.write('\033[97m')
            # do not print year:
            fd.write(entry.ts[5:] + ' ')
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
            if entry.flowid:
                fd.write(COLORS[hash(entry.flowid) % 7])
                fd.write(' ' + entry.flowid[:2] + '-' + entry.flowid[-2:])
                fd.write('\033[0m')
            else:
                fd.write('   -  ')
            fd.write(' ' + (entry.thread or '-'))
            fd.write(' ' + entry.clazz)
            fd.write('.' + entry.method)
            fd.write(' ' + entry.file)
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


class LogFilter(object):

    def __init__(self):
        self.levels = set()
        self.grep = None
        self.time_from = None
        self.time_to = None

    def matches(self, entry):
        ok = not self.levels or entry.level in self.levels
        if ok and self.grep:
            ok = self.grep in entry.message or self.grep in entry.source_class
        if ok and self.time_from:
            ok = entry.ts >= self.time_from
        if ok and self.time_to:
            ok = entry.ts < self.time_to

        return ok


def main():
    Watcher()
    parser = ArgumentParser()
    parser.add_argument('files', nargs='+', help='use custom configuration profile (more than one profile allowed)')
    parser.add_argument('-p', '--profile', help='use custom configuration profile (more than one profile allowed)')
    parser.add_argument('-f', '--follow', action='store_true', help='keep file open reading new lines (like tail)')
    parser.add_argument('-t', '--tail', action='store_true', help='show last N lines (default 100)')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose mode')
    parser.add_argument('-n', '--lines', dest='tail_lines', default=100, type=int, metavar='N',
                        help='show last N lines (instead of default 100)')
    parser.add_argument('-c', '--collapse', action='store_true',
                        help='collapse multi-line entries (i.e. each log entry is a single line)')
    parser.add_argument('--truncate', metavar='CHARS', type=int, help='truncate log message to CHARS characters')
    parser.add_argument('-l', '--levels', help='only show log entries with log level(s)')
    parser.add_argument('-g', '--grep', metavar='PATTERN', help='only show log entries matching pattern')
    parser.add_argument('--time-from', metavar='DATETIME', help='only show log entries starting at DATETIME')
    parser.add_argument('--time-to', metavar='DATETIME', help='only show log entries until DATETIME')
    parser.add_argument('--redis-host', help='redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='redis port')
    parser.add_argument('--redis-namespace', help='redis namespace')
    parser.add_argument('--sincedb', help='sincedb path')

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
        aggregator = LogAggregator(file_names)
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
            tail=tail_lines,
            follow=args.follow,
            filterdef=filterdef,
            sincedb=args.sincedb,
        ))
        fid += 1
    for reader in readers:
        reader.start()
    if args.redis_host:
        out = RedisOutputThread(aggregator, args.redis_host, args.redis_port, args.redis_namespace)
    else:
        out = OutputThread(aggregator, collapse=args.collapse, truncate=args.truncate)
    out.start()


if __name__ == '__main__':
    main()

