#!/usr/bin/python

import collections
import heapq
import json
import os
import signal
import sys
import time
from threading import Thread
from optparse import OptionParser

class LogLevel(object):
    def __init__(self, priority, name):
        self.priority = priority
        self.name = name

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

LogEntry = collections.namedtuple('LogEntry', 'ts fid i flowid level thread source_class source_location message')

class Log4jParser(object):
    def __init__(self):
        # default pattern: %d %x %p %t %l: %m%n
        self.delimiter = ' '
        self.columns = 5
        self.col_flowid = 0
        self.col_level = 1
        self.col_thread = 2
        self.col_location = 3
        self.col_message = 4

    def auto_configure(self, fd):
        """try to auto-configure the parser"""
        try:
            for entry in self.read(0, fd):
                break
        except ValueError:
            # log4j pattern with thread name: %d [%p] %t: %l - %m%n
            self.delimiter = None
            self.columns = 4
            self.col_flowid = None
            self.col_level = 0
            self.col_thread = 1
            self.col_location = 2
            self.col_message = 3
        fd.seek(0)

        try:
            for entry in self.read(0, fd):
                break
        except ValueError:
            # log4j pattern without thread name: %d [%p] %l - %m%n
            self.columns = 3
            self.col_flowid = None
            self.col_level = 0
            self.col_thread = None
            self.col_location = 1
            self.col_message = 2
        fd.seek(0)

    def read(self, fid, fd):
        """read log4j formatted log file"""
        maxsplit = self.columns - 1
        delimiter = self.delimiter
        col_flowid = self.col_flowid
        col_level = self.col_level
        col_thread = self.col_thread
        col_location = self.col_location
        col_message = self.col_message

        lastline = None
        i = 0
        while True:
            line = lastline or fd.readline()
            if not line:
                break
            ts = line[:23]
            if ts[:2] != '20':
                continue
            cols = line[24:].split(delimiter, maxsplit)
            c = cols[col_level].strip('[]')[0]
            if c == 'T':
                level = LogLevel.TRACE
            elif c == 'D':
                level = LogLevel.DEBUG
            elif c == 'I':
                level = LogLevel.INFO
            elif c == 'W':
                level = LogLevel.WARN
            elif c == 'E':
                level = LogLevel.ERROR
            else:
                level = LogLevel.FATAL
            flowid = col_flowid is not None and cols[col_flowid] or None
            thread = col_thread is not None and cols[col_thread].rstrip(':') or None
            source_class, source_location = cols[col_location].split('(', 1)
            msg = cols[col_message]
            while True:
                lastline = fd.readline()
                if not lastline:
                    break
                if lastline[:2] == '20' and lastline[23:24] == ' ':
                    # start of new log entry
                    break
                msg += lastline
                lastline = None
            yield LogEntry(fid=fid, ts=ts, i=i, flowid=flowid, level=level, thread=thread, source_class=source_class, source_location=source_location.rstrip(':)'), message=msg.rstrip())
            i += 1

def parse_timestamp(ts):
    """takes a timestamp such as 2011-09-18 16:00:01,123"""
    if len(ts) < 19:
        ts += ':00'
    struct = time.strptime(ts[:19], '%Y-%m-%d %H:%M:%S')
    return time.mktime(struct)
    
    
class LogReader(Thread):
    def __init__(self, fid, fname, parser, receiver, tail=0, follow=False, filterdef=None):
        Thread.__init__(self, name='LogReader-%d' % (fid,))
        self.fid = fid
        self.fname = fname
        self.parser = parser
        self.receiver = receiver
        self.tail = tail
        self.follow = follow
        self.filterdef = filterdef or LogFilter()

    def _seek_tail(self, fd, n):
        """seek to start of "tail" (last n lines)"""
        l = os.path.getsize(self.fname) 
        s = -1024 * n
        if s * -1 >= l:
            # apparently the file is too small
            # => seek to start of file
            fd.seek(0)
            return
        fd.seek(s, 2)
        contents = fd.read()
        e = len(contents)
        i = 0
        while e >= 0:
            e = contents.rfind('\n', 0, e)
            if e >= 0:
                i += 1
                if i >= n:
                    fd.seek(s + e, 2)
                    break

    def _seek_time(self, fd, ts):
        """try to seek to our start time"""
        s = os.path.getsize(self.fname) 
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
        with open(self.fname, 'rb') as fd:
            self.parser.auto_configure(fd)
            if self.tail:
                self._seek_tail(fd, self.tail)
            elif filt.time_from:
                self._seek_time(fd, filt.time_from)
            while True:
                where = fd.tell()
                had_entry = False
                for entry in self.parser.read(fid, fd):
                    if filt.matches(entry):
                        receiver.add(entry)
                    #print entry.ts, entry.level, entry.thread, entry.source_class, entry.source_location, entry.message
                    had_entry = True
                if not self.follow:
                    receiver.eof(fid)
                    break
                if not had_entry:
                    time.sleep(0.5)
                    fd.seek(where)
                

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
        except OSError: pass

class LogAggregator(object):
    def __init__(self, file_names):
        self.file_names = file_names
        n = len(file_names)
        self.entries = []
        self.open_files = set(range(n))

    def add(self, entry):
        heapq.heappush(self.entries, entry)
        #if 
        #print self.entries[-10:]
        #print entry.fid, entry.ts, entry.level, entry.thread, entry.source_class, entry.source_location, entry.message

    def eof(self, fid):
        self.open_files.remove(fid)

    def get(self):
        while self.open_files or self.entries:
            try:
                entry = heapq.heappop(self.entries)
                yield entry
            except IndexError:
                time.sleep(0.5)
                pass

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
            fd.write(' ' + (entry.flowid if entry.flowid else '-'))
            fd.write(' ' + (entry.thread or '-'))
            fd.write(' ' + entry.source_class)
            fd.write(' ' + entry.source_location)
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
    parser = OptionParser(usage='Usage: %prog [OPTION]... [FILE]...')
    parser.add_option('-p', '--profile', 
                      help='use custom configuration profile (more than one profile allowed)')
    parser.add_option('-f', '--follow', action='store_true', dest='follow',
                      help='keep file open reading new lines (like tail)')
    parser.add_option('-t', '--tail', dest='tail', action='store_true',
                      help='show last N lines (default 100)')
    parser.add_option('-n', '--lines', dest='tail_lines', default=100, type='int', metavar='N',
                      help='show last N lines (instead of default 100)')
    parser.add_option('-c', '--collapse', dest='collapse', action='store_true',
                      help='collapse multi-line entries (i.e. each log entry is a single line)')
    parser.add_option('--truncate', dest='truncate', metavar='CHARS', type='int',
                      help='truncate log message to CHARS characters')
    parser.add_option('-l', '--levels', dest='levels',
                      help='only show log entries with log level(s)')
    parser.add_option('-g', '--grep', dest='grep', metavar='PATTERN',
                      help='only show log entries matching pattern')
    parser.add_option('--time-from', dest='time_from', metavar='DATETIME',
                      help='only show log entries starting at DATETIME')
    parser.add_option('--time-to', dest='time_to', metavar='DATETIME',
                      help='only show log entries until DATETIME')

    (options, args) = parser.parse_args()

    config_file = os.path.expanduser('~/.logfirerc')
    if not os.path.isfile(config_file):
        # fallback using global configuration file
        config_file = '/etc/logfirerc'
    if os.path.isfile(config_file):
        config = json.load(open(config_file, 'rb'))
        merged_config = {'options': {}, 'files': []}
        active_profiles = ['default']
        if options.profile:
            active_profiles += options.profile.split(',')
        for profile in active_profiles:
            if config.get(profile):
                merged_config['options'].update(config[profile].get('options', {}))
                if config[profile].get('files'):
                    merged_config['files'] += config[profile].get('files')

        for key, val in merged_config['options'].items():
            if not getattr(options, key, None):
                setattr(options, key, val)
        if not args:
            args = merged_config['files']
            

    filterdef = LogFilter()
    filterdef.grep = options.grep
    filterdef.time_from = options.time_from
    filterdef.time_to = options.time_to

    if options.levels:
        for lvl in options.levels.split(','):
            lo = getattr(LogLevel, lvl)
            filterdef.levels.add(lo)

    tail_lines = 0
    if options.tail:
        tail_lines = int(options.tail_lines)

    used_file_names = set()
    file_names = args
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
        file_names[fid] = name 
        used_file_names.add(name)
        parser = Log4jParser()
        readers.append(LogReader(fid, fpath, parser, aggregator, tail=tail_lines, follow=options.follow, filterdef=filterdef))
        fid += 1
    for reader in readers:
        reader.start()
    out = OutputThread(aggregator, collapse=options.collapse, truncate=options.truncate)
    out.start()


if __name__ == '__main__':
    main()

