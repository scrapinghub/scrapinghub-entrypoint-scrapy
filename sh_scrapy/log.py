import logging
import sys
import warnings

from scrapy import __version__
from scrapy.utils.python import to_unicode
from twisted.python import log as txlog

from sh_scrapy.writer import pipe_writer


# keep a global reference to stderr as it is redirected on log initialization
_stdout = sys.stdout
_stderr = sys.stderr


def _logfn(level, message):
    """Wraps HS job logging function."""
    try:
        pipe_writer.write_log(level=level, message=message)
    except UnicodeDecodeError:
        # workaround for messages that contain binary data
        message = repr(message)[1:-1]
        pipe_writer.write_log(level=level, message=message)


def initialize_logging():
    """Initialize logging to send messages to Hubstorage job logs

    it initializes:
    - Python logging
    - Twisted logging
    - Scrapy logging
    - Redirects standard output and stderr to job log at INFO level

    This duplicates some code with Scrapy log.start(), but it's required in
    order to avoid scrapy from starting the log twice.
    """
    # General python logging
    root = logging.getLogger()
    root.setLevel(logging.NOTSET)
    hdlr = HubstorageLogHandler()
    hdlr.setLevel(logging.INFO)
    hdlr.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    root.addHandler(hdlr)

    # Silence commonly used noisy libraries
    try:
        import boto  # boto overrides its logger at import time
    except ImportError:
        pass

    nh = logging.NullHandler()
    for ln in ('boto', 'requests', 'hubstorage'):
        lg = logging.getLogger(ln)
        lg.propagate = 0
        lg.addHandler(nh)

    # Redirect standard output and error to HS log
    sys.stdout = StdoutLogger(0, 'utf-8')
    sys.stderr = StdoutLogger(1, 'utf-8')

    # Twisted specifics (includes Scrapy)
    obs = HubstorageLogObserver(hdlr)
    _oldshowwarning = warnings.showwarning
    txlog.startLoggingWithObserver(obs.emit, setStdout=False)
    warnings.showwarning = _oldshowwarning
    return hdlr


class HubstorageLogHandler(logging.Handler):
    """Python logging handler that writes to HubStorage"""

    def emit(self, record):
        try:
            message = self.format(record)
            if message:
                _logfn(message=message, level=record.levelno)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def handleError(self, record):
        cur = sys.stderr
        try:
            sys.stderr = _stderr
            super(HubstorageLogHandler, self).handleError(record)
        finally:
            sys.stderr = cur


class HubstorageLogObserver(object):
    """Twisted log observer with Scrapy specifics that writes to HubStorage"""

    def __init__(self, loghdlr):
        self._hs_loghdlr = loghdlr

    def emit(self, ev):
        logitem = self._get_log_item(ev)
        if logitem:
            _logfn(**logitem)

    def _get_log_item(self, ev):
        """Get HubStorage log item for the given Twisted event, or None if no
        document should be inserted
        """
        if ev['system'] == 'scrapy':
            level = ev['logLevel']
        else:
            if ev['isError']:
                level = logging.ERROR
            else:
                level = logging.INFO

        # It's important to access level trough handler instance,
        # min log level can change at any moment.
        if level < self._hs_loghdlr.level:
            return

        msg = ev.get('message')
        if msg:
            msg = to_unicode(msg[0])

        failure = ev.get('failure', None)
        if failure:
            msg = failure.getTraceback()

        why = ev.get('why', None)
        if why:
            msg = "%s\n%s" % (why, msg)

        fmt = ev.get('format')
        if fmt:
            try:
                msg = fmt % ev
            except:
                msg = "UNABLE TO FORMAT LOG MESSAGE: fmt=%r ev=%r" % (fmt, ev)
                level = logging.ERROR
        # to replicate typical scrapy log appeareance
        msg = msg.replace('\n', '\n\t')
        return {'message': msg, 'level': level}


class StdoutLogger(txlog.StdioOnnaStick):
    """This works like Twisted's StdioOnnaStick but prepends standard
    output/error messages with [stdout] and [stderr]
    """

    def __init__(self, isError=0, encoding=None, loglevel=logging.INFO):
        txlog.StdioOnnaStick.__init__(self, isError, encoding)
        self.prefix = "[stderr] " if isError else "[stdout] "
        self.loglevel = loglevel

    def _logprefixed(self, msg):
        _logfn(message=self.prefix + msg, level=self.loglevel)

    def write(self, data):
        data = to_unicode(data, self.encoding)

        d = (self.buf + data).split('\n')
        self.buf = d[-1]
        messages = d[0:-1]
        for message in messages:
            self._logprefixed(message)

    def writelines(self, lines):
        for line in lines:
            line = to_unicode(line, self.encoding)
            self._logprefixed(line)
