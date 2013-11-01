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
        self.parser = parser
        self.receiver = receiver
        self.tail = tail
        self.follow = follow
        self.filterdef = filterdef or LogFilter()
        self._last_file_mapping_update = None
        self._stat_interval = 2
        self._sincedb_write_interval = 5
        self._file_device_and_inode_string = None
        self._file = None
        self._first = False
        self._sincedb_path = sincedb
        self._last_sincedb_write = None

        if sincedb:
            self._full_sincedb_path = '{0}f{1}'.format(sincedb, hashlib.sha1(fname).hexdigest())
        else:
            self._full_sincedb_path = None


    def _seek_position(self):
        """seek to start of "tail" (last n lines)"""
        self._seek_sincedb_position() or self._seek_tail()

    def _seek_sincedb_position(self):
        if self._full_sincedb_path:
            try:
                _, device_and_inode_string, last_position, _ = self._load_progress()
            except Exception:
                logging.warning('Failed to read the sincedb file for "%s".', self._filename)
                return False
            else:
                logging.info('Resumed reading "%s" at offset %d.', self._filename, last_position)
                self._file_device_and_inode_string = device_and_inode_string
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
                self._do_housekeeping(time.time())
                had_entry = True
            if not self.follow:
                receiver.eof(fid)
                break
            if not had_entry:
                time.sleep(0.1)
                if self._do_housekeeping(time.time()):
                    self._file.seek(where)

    def _open_file(self):
        """
        Opens the file the LogReader is responsible for and assigns it to _file. If that file has the extension ".gz",
        it is opened as a gzip file. Errors are propagated.
        """

        try:
            if self._filename.endswith('.gz'):
                self._file = gzip.open(self._filename, 'rb')
            else:
                self._file = io.open(self._filename, 'rb')
            logging.info('Opened %s.', self._filename)
        except IOError:
            logging.exception('Failed to open %s.', self._filename)
            raise
        self._first = True

    def _close_file(self):
        """Closes the file the LogReader is responsible for and sets _file to None."""

        if self._file:
            self._file.close()
            self._file = None
            logging.info('Closed %s.' % self._filename)

    @staticmethod
    def get_device_and_inode_string(st):
        return '%xg%x' % (st.st_dev, st.st_ino)

    def _update_file(self, seek_to_end=True):
        """Open the file for tailing"""

        self._close_file()
        self._open_file()
        try:
            st = os.stat(self._filename)
        except EnvironmentError, err:
            if err.errno == errno.ENOENT:
                logging.info('file removed')
                self._close_file()
        self._file_device_and_inode_string = self.get_device_and_inode_string(st)
        if seek_to_end and self.tail:
            self._seek_position()

    ### HOUSEKEEPING ###

    def _do_housekeeping(self, current_time):
        if self._last_file_mapping_update and current_time - self._last_file_mapping_update <= self._stat_interval:
            result = True
        else:
            self._last_file_mapping_update = current_time
            result = self._ensure_file_is_good()

        if self._full_sincedb_path and (not self._last_sincedb_write or current_time - self._last_sincedb_write
                                        > self._sincedb_write_interval):
            self._last_sincedb_write = current_time
            self._save_progress()

        return result

    def _ensure_file_is_good(self):
        """
        Ensures that the file the reader is tailing is the file it is supposed to be tailing.
        If the target file has been removed, does nothing. If there is a new file in its place, stops tailing the
        current file and tails the new file instead. If the current file position lies past the file's end, resets
        it to the file's beginning.
        """
        
        try:
            st = os.stat(self._filename)
        except OSError, e:
            logging.info('The file %s has been removed.', self._filename)
            return False

        expected_device_and_inode_string = self._file_device_and_inode_string
        actual_device_and_inode_string = self.get_device_and_inode_string(st)

        if expected_device_and_inode_string != actual_device_and_inode_string:
            logging.info('The file %s has been rotated.', self._filename)
            self._update_file(seek_to_end=False)
            return False

        current_position = self._file.tell()
        file_size = st.st_size

        if current_position > file_size:
            logging.info('The file %s has been truncated.', self._filename)
            self._file.seek(0)
            return False

        return True

    ### PROGRESS ###

    def _save_progress(self):
        """
        Saves the the reader's progress infotmation to a file, so that the application can resume reading where it
        left off in case it is terminated.
        """

        progress = self._get_progress_string()
        if progress:
            logging.debug('Writing sincedb entry "%s".', progress)
            try:
                with open(self._full_sincedb_path, 'wb') as sincedb_file:
                    sincedb_file.write(progress)
            except Exception:
                logging.exception('Failed to save progress for %s.', self._filename)

    def _load_progress(self):
        """
        Loads the reader's progress information. Returns a tuple (filename, device_and_inode_string, position, size).
        """

        with open(self._full_sincedb_path, 'rb') as sincedb_file:
            filename, device_and_inode_string, position, size = sincedb_file.read().rsplit(None, 3)
            return filename, device_and_inode_string, int(position), int(size)

    def _get_progress_string(self):
        """Returns the sincedb string for the progress of the file the reader is responsible for."""

        try:
            position = self._file.tell()
            size = os.fstat(self._file.fileno()).st_size
            return '%s %s %d %d' % (self._filename, self._file_device_and_inode_string, position, size)
        except Exception:
            logging.exception('Failed to gather progress information for %s.', self._filename)
            return None


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
