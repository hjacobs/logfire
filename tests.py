from StringIO import StringIO
from unittest import TestCase

import logfire


class LogfireTests(TestCase):

    def test_regression_log4j_endless_loop(self):
        """Lines with too few columns do no longer cause an endless loop."""

        line1 = '2000-01-01 00:00:00,000 FlowID ERROR Thread C.m(C.java:23): Exception!'
        line2 = '2000-01-01 00:00:00,001 GARBAGE'
        list(logfire.Log4Jparser().read(0, StringIO(line1 + '\n' + line2)))
