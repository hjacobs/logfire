import errno
import gzip
import hashlib
import io
import logging
import threading
import time
import os


class LogReader(threading.Thread):

    NO_ENTRIES_SLEEP_INTERVAL = 0.1  # seconds
    CHUNK_SIZE = 1024  # bytes
    ENSURE_FILE_IS_GOOD_CALL_INTERVAL = 2  # seconds
    SAVE_PROGRESS_CALL_INTERVAL = 5  # seconds


    def __init__(
        self,
        reader_id,
        logfile_name,
        parser,
        receiver,
        tail_length=None,
        follow=False,
        entry_filter=None,
        progress_file_path_prefix=None,
    ):

        threading.Thread.__init__(self, name='LogReader-%d' % reader_id)

        self.reader_id = reader_id
        self.logfile_name = logfile_name
        self.parser = parser
        self.receiver = receiver
        self.tail_length = tail_length
        self.follow = follow
        self.entry_filter = entry_filter or LogFilter()

        self.logfile = None
        self.logfile_id = None
        self.last_ensure_file_is_good_call_timestamp = 0
        self.last_save_progress_call_timestamp = 0

        if progress_file_path_prefix:
            self.progress_file_path = '{0}f{1}'.format(progress_file_path_prefix, hashlib.sha1(logfile_name).hexdigest())
        else:
            self.progress_file_path = None

    def run(self):
        """Implements the reader's main loop. Called when the thread is started."""

        self._open_file()
        self.parser.autoconfigure(self.logfile)
        self._seek_position()

        # Performance!
        reader_id = self.reader_id
        logfile = self.logfile
        receiver = self.receiver
        entry_filter = self.entry_filter

        while True:
            entry_count = 0
            for entry in self.parser.read(reader_id, logfile):
                if entry_filter.matches(entry):
                    receiver.add(entry)
                entry_count += 1
                if entry_count & 1023 == 0:
                    self._maybe_do_housekeeping(time.time())

            if not self.follow:
                receiver.eof(reader_id)
                break
            if entry_count == 0:
                time.sleep(self.NO_ENTRIES_SLEEP_INTERVAL)
                self._maybe_do_housekeeping(time.time())

    ### FILES ###

    def _open_file(self):
        """
        Opens the file the LogReader is responsible for and assigns it to logfile. If that file has the extension ".gz",
        it is opened as a gzip file. Errors are propagated.
        """

        try:
            if self.logfile_name.endswith('.gz'):
                self.logfile = gzip.open(self.logfile_name, 'rb')
            else:
                self.logfile = io.open(self.logfile_name, 'rb')
            logging.info('Opened %s.', self.logfile_name)
        except IOError:
            logging.exception('Failed to open %s.', self.logfile_name)
            raise
        else:
            self.logfile_id = get_device_and_inode_string(os.fstat(self.logfile.fileno()))

    def _close_file(self):
        """Closes the file the LogReader is responsible for and sets logfile to None."""

        if self.logfile:
            self.logfile.close()
            self.logfile = None
            logging.info('Closed %s.' % self.logfile_name)

    ### SEEKING ###

    def _seek_position(self):
        """
        Seeks to the start position of the file the reader is responsible for. Depending on the reader's configuration,
        dispatches to _seek_first_unprocessed_position(), _seek_tail(), or _seek_time().
        """

        if self.progress_file_path:
            self._seek_first_unprocessed_position()
        elif self.tail_length == 0:
            self.logfile.seek(0, os.SEEK_END)
        elif self.tail_length:
            self._seek_tail()
        elif self.entry_filter.time_from:
            self._seek_time(self.entry_filter.time_from)

    def _seek_first_unprocessed_position(self):
        """Loads the last file position from the file given by progress_file_path and seeks to that position."""

        try:
            _, logfile_id, last_position, _ = self._load_progress()
        except Exception:
            logging.warning('Failed to read the progress file for "%s".', self.logfile_name)
        else:
            logging.info('Resumed reading "%s" at offset %d.', self.logfile_name, last_position)
            self.logfile_id = logfile_id
            self.logfile.seek(last_position)

    def _seek_tail(self):
        """Seeks to the beginning of the Nth entry (not line!) from the end, where N is given by tail_length."""

        file_size = os.fstat(self.logfile.fileno()).st_size
        chunk_count = (file_size // self.CHUNK_SIZE) + bool(file_size % self.CHUNK_SIZE)

        chunk = ''
        newline_count = 0
        previous_newline_position = None

        for iteration, chunk_index in enumerate(reversed(range(chunk_count))):
            self.logfile.seek(self.CHUNK_SIZE * chunk_index)
            line_tail = chunk[:previous_newline_position]
            chunk = self.logfile.read(self.CHUNK_SIZE) + line_tail

            if iteration == 0:
                previous_newline_position = chunk.rfind('\n')
            else:
                previous_newline_position = None

            current_newline_position = chunk.rfind('\n', 0, previous_newline_position)

            while current_newline_position != -1:
                line = chunk[current_newline_position + 1 : previous_newline_position]

                if not self.parser.is_continuation_line(line):
                    newline_count += 1
                    if newline_count >= self.tail_length:
                        self.logfile.seek(chunk_index * self.CHUNK_SIZE + current_newline_position + 1)
                        return    

                previous_newline_position = current_newline_position
                current_newline_position = chunk.rfind('\n', 0, previous_newline_position)
        else:
            self.logfile.seek(0)

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
            self.logfile.seek(self.CHUNK_SIZE * chunk_index)
            line = self.logfile.readline()
            while line and self.parser.is_continuation_line(line):
                line = self.logfile.readline()
            if line and line[-1] == '\n':
                return self.parser.get_time_string(line)
            else:
                return 'greater than any time string'

        def seek_time_in_chunk(chunk_index):
            self.logfile.seek(chunk_index * self.CHUNK_SIZE)

            while True:
                line = self.logfile.readline()
                if line and line[-1] == '\n':
                    if self.parser.is_continuation_line(line):
                        continue
                    else:
                        if self.parser.get_time_string(line) >= time_string:
                            self.logfile.seek(-len(line), os.SEEK_CUR)
                            return
                else:
                    self.logfile.seek(0, os.SEEK_END)
                    return

        file_size = os.fstat(self.logfile.fileno()).st_size
        chunk_count = (file_size // self.CHUNK_SIZE) + bool(file_size % self.CHUNK_SIZE)

        target_chunk_index = binary_chunk_search(0, chunk_count + 1)
        seek_time_in_chunk(target_chunk_index)

    ### HOUSEKEEPING ###

    def _maybe_do_housekeeping(self, current_timestamp):
        """
        If more than ENSURE_FILE_IS_GOOD_CALL_INTERVAL seconds have passed since _ensure_file_is_good was last called,
        calls that method. Then, if more than SAVE_PROGRESS_CALL_INTERVAL seconds have passed since _save_progress was
        last called, calls that method.
        """

        if current_timestamp - self.last_ensure_file_is_good_call_timestamp > self.ENSURE_FILE_IS_GOOD_CALL_INTERVAL:
            self.last_ensure_file_is_good_call_timestamp = current_timestamp
            self._ensure_file_is_good()

        if self.progress_file_path:
            if current_timestamp - self.last_save_progress_call_timestamp > self.SAVE_PROGRESS_CALL_INTERVAL:
                self.last_save_progress_call_timestamp = current_timestamp
                self._save_progress()

    def _ensure_file_is_good(self):
        """
        Ensures that the file the reader is tailing is the file it is supposed to be tailing.
        If the target file has been removed, does nothing. If there is a new file in its place, stops tailing the
        current file and tails the new file instead. If the current file position lies past the file's end, resets
        it to the file's beginning.
        """
        
        try:
            stat_results = os.stat(self.logfile_name)
        except OSError, e:
            logging.info('The file %s has been removed.', self.logfile_name)
        else:
            expected_logfile_id = self.logfile_id
            actual_logfile_id = get_device_and_inode_string(stat_results)
            current_position = self.logfile.tell()
            file_size = stat_results.st_size

            if expected_logfile_id != actual_logfile_id:
                logging.info('The file %s has been rotated.', self.logfile_name)
                self._close_file()
                self._open_file()
            elif current_position > file_size:
                logging.info('The file %s has been truncated.', self.logfile_name)
                self.logfile.seek(0)

    ### PROGRESS ###

    def _save_progress(self):
        """
        Saves the the reader's progress information to a file, so that the application can resume reading where it
        left off in case it is terminated.
        """

        progress = self._make_progress_string()
        if progress:
            logging.debug('Writing progress file entry "%s".', progress)
            try:
                with open(self.progress_file_path, 'wb') as progress_file:
                    progress_file.write(progress)
            except Exception:
                logging.exception('Failed to save progress for %s.', self.logfile_name)

    def _load_progress(self):
        """
        Loads the reader's progress information. Returns a tuple (filename, logfile_id, position, size).
        """

        with open(self.progress_file_path, 'rb') as progress_file:
            filename, logfile_id, position, size = progress_file.read().rsplit(None, 3)
            return filename, logfile_id, int(position), int(size)

    def _make_progress_string(self):
        """Constructs a progress string that expresses the progress of the reader."""

        try:
            position = self.logfile.tell()
            size = os.fstat(self.logfile.fileno()).st_size
            return '%s %s %d %d' % (self.logfile_name, self.logfile_id, position, size)
        except Exception:
            logging.exception('Failed to gather progress information for %s.', self.logfile_name)
            return None


class LogFilter(object):

    def __init__(self, levels=(), grep=None, time_from=None, time_to=None):
        self.levels = set(levels)
        self.grep = grep
        self.time_from = time_from
        self.time_to = time_to

    def matches(self, entry):
        ok = not self.levels or entry.level in self.levels
        if ok and self.grep:
            ok = self.grep in entry.message or self.grep in entry.class_
        if ok and self.time_from:
            ok = entry.timestamp >= self.time_from
        if ok and self.time_to:
            ok = entry.timestamp < self.time_to

        return ok


def get_device_and_inode_string(st):
    return '%xg%x' % (st.st_dev, st.st_ino)
