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

