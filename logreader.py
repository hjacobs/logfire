import errno
import gzip
import hashlib
import io
import logging
import time
import os

from threading import Thread


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
        chunk_size = 1024
        file_size = os.fstat(self._file.fileno()).st_size
        chunk_count = (file_size // chunk_size) + bool(file_size % chunk_size)

        chunk = ''
        newline_count = 0
        previous_newline_position = None

        for iteration, chunk_index in enumerate(reversed(range(chunk_count))):
            self._file.seek(chunk_size * chunk_index)
            line_tail = chunk[:previous_newline_position]
            chunk = self._file.read(chunk_size) + line_tail

            if iteration == 0:
                previous_newline_position = chunk.rfind('\n')
            else:
                previous_newline_position = None

            current_newline_position = chunk.rfind('\n', 0, previous_newline_position)

            while current_newline_position != -1:
                line = chunk[current_newline_position + 1 : previous_newline_position]

                if not self.parser.is_continuation_line(line):
                    newline_count += 1
                    if newline_count >= self.tail:
                        self._file.seek(chunk_index * chunk_size + current_newline_position + 1)
                        return    

                previous_newline_position = current_newline_position
                current_newline_position = chunk.rfind('\n', 0, previous_newline_position)
        else:
            self._file.seek(0)

    def _seek_time(self, time_string):
        """try to seek to our start time"""

        def binary_chunk_search(start_index, stop_index):
            if start_index + 1 == stop_index:
                return start_index
            else:
                pivot_index = (start_index + stop_index) // 2
                if get_first_timestamp_in_chunk(pivot_index) > time_string:
                    return binary_chunk_search(start_index, pivot_index)
                else:
                    return binary_chunk_search(pivot_index, stop_index)

        def get_first_timestamp_in_chunk(chunk_index):
            self._file.seek(chunk_size * chunk_index)
            line = self._file.readline()
            while line and self.parser.is_continuation_line(line):
                line = self._file.readline()
            if line and line[-1] == '\n':
                return self.parser.get_time_string(line)
            else:
                return 'greater than any time string'

        def seek_time_in_chunk(chunk_index):
            self._file.seek(chunk_index * chunk_size)

            while True:
                line = self._file.readline()
                if line and line[-1] == '\n':
                    if self.parser.is_continuation_line(line):
                        continue
                    else:
                        if self.parser.get_time_string(line) >= time_string:
                            self._file.seek(-len(line), os.SEEK_CUR)
                            return
                else:
                    self._file.seek(0, os.SEEK_END)
                    return

        chunk_size = 1024
        file_size = os.fstat(self._file.fileno()).st_size
        chunk_count = (file_size // chunk_size) + bool(file_size % chunk_size)

        target_chunk_index = binary_chunk_search(0, chunk_count + 1)
        seek_time_in_chunk(target_chunk_index)

    def run(self):
        fid = self.fid
        receiver = self.receiver
        filt = self.filterdef
        self._update_file()
        self.parser.autoconfigure(self._file)
        self._update_file()
        if filt.time_from:
            self._seek_time(filt.time_from)
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
