import errno
import gzip
import hashlib
import io
import logging
import time
import os

from threading import Thread


class LogReader(Thread):

    NO_ENTRIES_SLEEP_INTERVAL = 0.1  # seconds
    CHUNK_SIZE = 1024  # bytes

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
        self._file_device_and_inode_string = None
        self._file = None
        self._sincedb_path = sincedb

        self._ensure_file_is_good_call_interval = 2  # seconds
        self._last_ensure_file_is_good_call_timestamp = 0
        self._save_progress_call_interval = 5  # seconds
        self._last_save_progress_call_timestamp = 0

        if sincedb:
            self._full_sincedb_path = '{0}f{1}'.format(sincedb, hashlib.sha1(fname).hexdigest())
        else:
            self._full_sincedb_path = None

    def run(self):
        """Implements the reader's main loop. Called when the thread is started."""

        self._open_file()
        self.parser.autoconfigure(self._file)
        self._seek_position()

        # Performance!
        fid = self.fid
        logfile = self._file
        receiver = self.receiver
        logfilter = self.filterdef

        while True:
            entry_count = 0
            for entry in self.parser.read(fid, logfile):
                if logfilter.matches(entry):
                    receiver.add(entry)
                entry_count += 1
                if entry_count & 1023 == 0:
                    self._maybe_do_housekeeping(time.time())

            if not self.follow:
                receiver.eof(fid)
                break
            if entry_count == 0:
                time.sleep(self.NO_ENTRIES_SLEEP_INTERVAL)
                self._maybe_do_housekeeping(time.time())

    ### FILES ###

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
        else:
            self._file_device_and_inode_string = get_device_and_inode_string(os.fstat(self._file.fileno()))

    def _close_file(self):
        """Closes the file the LogReader is responsible for and sets _file to None."""

        if self._file:
            self._file.close()
            self._file = None
            logging.info('Closed %s.' % self._filename)

    ### SEEKING ###

    def _seek_position(self):
        """
        Seeks to the start position of the file the reader is responsible for. Depending on the reader's configuration,
        dispatches to _seek_sincedb_position(), _seek_tail(), or _seek_time().
        """

        if self._full_sincedb_path:
            self._seek_sincedb_position()
        elif self.tail:
            self._seek_tail()
        elif self.filterdef.time_from:
            self._seek_time(self.filterdef.time_from)

    def _seek_sincedb_position(self):
        """Loads the last file position from the file given by _full_sincedb_path and seeks to that position."""

        try:
            _, device_and_inode_string, last_position, _ = self._load_progress()
        except Exception:
            logging.warning('Failed to read the sincedb file for "%s".', self._filename)
        else:
            logging.info('Resumed reading "%s" at offset %d.', self._filename, last_position)
            self._file_device_and_inode_string = device_and_inode_string
            self._file.seek(last_position)

    def _seek_tail(self):
        """Seeks to the beginning of the Nth entry (not line!) from the end, where N is given by tail."""

        file_size = os.fstat(self._file.fileno()).st_size
        chunk_count = (file_size // self.CHUNK_SIZE) + bool(file_size % self.CHUNK_SIZE)

        chunk = ''
        newline_count = 0
        previous_newline_position = None

        for iteration, chunk_index in enumerate(reversed(range(chunk_count))):
            self._file.seek(self.CHUNK_SIZE * chunk_index)
            line_tail = chunk[:previous_newline_position]
            chunk = self._file.read(self.CHUNK_SIZE) + line_tail

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
                        self._file.seek(chunk_index * self.CHUNK_SIZE + current_newline_position + 1)
                        return    

                previous_newline_position = current_newline_position
                current_newline_position = chunk.rfind('\n', 0, previous_newline_position)
        else:
            self._file.seek(0)

    def _seek_time(self, time_string):
        """Seeks to the beginning of the first entry with a timestamp greater than or equal to the given one."""

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
            self._file.seek(self.CHUNK_SIZE * chunk_index)
            line = self._file.readline()
            while line and self.parser.is_continuation_line(line):
                line = self._file.readline()
            if line and line[-1] == '\n':
                return self.parser.get_time_string(line)
            else:
                return 'greater than any time string'

        def seek_time_in_chunk(chunk_index):
            self._file.seek(chunk_index * self.CHUNK_SIZE)

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

        file_size = os.fstat(self._file.fileno()).st_size
        chunk_count = (file_size // self.CHUNK_SIZE) + bool(file_size % self.CHUNK_SIZE)

        target_chunk_index = binary_chunk_search(0, chunk_count + 1)
        seek_time_in_chunk(target_chunk_index)

    ### HOUSEKEEPING ###

    def _maybe_do_housekeeping(self, current_timestamp):
        """
        If more than _ensure_file_is_good_call_interval seconds have passed since _ensure_file_is_good was last called,
        calls that method. Then, if more than _save_progress_call_interval seconds have passed since _save_progress was
        last called, calls that method.
        """

        if current_timestamp - self._last_ensure_file_is_good_call_timestamp > self._ensure_file_is_good_call_interval:
            self._last_ensure_file_is_good_call_timestamp = current_timestamp
            self._ensure_file_is_good()

        if self._full_sincedb_path:
            if current_timestamp - self._last_save_progress_call_timestamp > self._save_progress_call_interval:
                self._last_save_progress_call_timestamp = current_timestamp
                self._save_progress()

    def _ensure_file_is_good(self):
        """
        Ensures that the file the reader is tailing is the file it is supposed to be tailing.
        If the target file has been removed, does nothing. If there is a new file in its place, stops tailing the
        current file and tails the new file instead. If the current file position lies past the file's end, resets
        it to the file's beginning.
        """
        
        try:
            stat_results = os.stat(self._filename)
        except OSError, e:
            logging.info('The file %s has been removed.', self._filename)
        else:
            expected_device_and_inode_string = self._file_device_and_inode_string
            actual_device_and_inode_string = get_device_and_inode_string(stat_results)
            current_position = self._file.tell()
            file_size = stat_results.st_size

            if expected_device_and_inode_string != actual_device_and_inode_string:
                logging.info('The file %s has been rotated.', self._filename)
                self._close_file()
                self._open_file()
            elif current_position > file_size:
                logging.info('The file %s has been truncated.', self._filename)
                self._file.seek(0)

    ### PROGRESS ###

    def _save_progress(self):
        """
        Saves the the reader's progress information to a file, so that the application can resume reading where it
        left off in case it is terminated.
        """

        progress = self._make_progress_string()
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

    def _make_progress_string(self):
        """Constructs a sincedb string that expresses the progress of the reader."""

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


def get_device_and_inode_string(st):
    return '%xg%x' % (st.st_dev, st.st_ino)
