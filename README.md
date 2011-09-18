logfire - Log File Reader
=========================

Logfire is a console based log file reader with color and tail-like "follow" support.

Currently only two log4j pattern layouts are supported.

Example:

    ./logfire.py -t -f -c --truncate=200 myapp-log4j.log mysecondapp.log

Display last 10 lines (-t) of two log files and reads new log entries as they are written (-f).
Multi-line log messages are collapsed (-c) and log messages are truncated if longer than 200 characters.
