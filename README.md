logfire - Log File Reader
=========================

Logfire is a console based log file reader with color and tail-like "follow" support.

Currently only two log4j pattern layouts are supported.

Example:

    ./logfire.py -t -f -c --truncate=200 myapp-log4j.log mysecondapp.log

Displays last 10 lines (-t) of two log files and reads new log entries as they are written (-f).
Multi-line log messages are collapsed (-c) and log messages are truncated if longer than 200 characters.

Filtering example:

	./logfire.py --time-from="2011-09-18 15:00" --time-to="2011-09-18 16:00" myapp.log

Only outputs log entries from myapp.log which are between the given two timestamps.
