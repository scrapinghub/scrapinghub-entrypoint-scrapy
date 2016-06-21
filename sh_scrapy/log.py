from __future__ import absolute_import
from __future__ import unicode_literals
import sys, os, logging, warnings
from twisted.python import log as txlog
from scrapy import log, __version__
from scrapy.utils.python import unicode_to_str
from sh_scrapy.hsref import hsref


# keep a global reference to stderr as it is redirected on log initialization
_stderr = sys.stderr


if sys.version_info < (3,):
    STRING_TYPE = basestring
    TEXT_TYPE = unicode
    BINARY_TYPE = str
else:
    STRING_TYPE = str
    TEXT_TYPE = str
    BINARY_TYPE = bytes


def _logfn(*args, **kwargs):
    """Wraps HS job logging function

    Prevents errors writign to a closed batchuploader writer
    It happens when the log writer is closed but batchuploader is still sending batches
    """
    logs = hsref.job.logs
    w = logs._writer
    if not (w and w.closed):
        logs.log(*args, **kwargs)


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

    # Scrapy specifics
    if 'SCRAPY_JOB' in os.environ:
        log.msg("Scrapy %s started" % __version__)
        log.start = _dummy  # ugly but needed to prevent scrapy re-opening the log

    return hdlr


def _dummy(*a, **kw):
    """Scrapy log.start dummy monkeypatch"""
    pass


class HubstorageLogHandler(logging.Handler):
    """Python logging handler that writes to HubStorage"""

    def emit(self, record):
        try:
            msg = self.format(record)
            _logfn(msg, level=record.levelno)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def handleError(self, record):
        cur = sys.stderr
        try:
            sys.stderr = _stderr
            logging.Handler.handleError(self, record)
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
            msg = unicode_to_str(msg[0])

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

        msg = msg.replace('\n', '\n\t')  # to replicate typical scrapy log appeareance
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
        data = to_native_str(data, self.encoding)

        d = (self.buf + data).split('\n')
        self.buf = d[-1]
        messages = d[0:-1]
        for message in messages:
            self._logprefixed(message)

    def writelines(self, lines):
        for line in lines:
            line = to_native_str(line, self.encoding)
            self._logprefixed(line)


def to_unicode(text, encoding=None, errors='strict'):
    """Return the unicode representation of `text`.

    If `text` is already a ``unicode`` object, return it as-is.
    If `text` is a ``bytes`` object, decode it using `encoding`.

    Otherwise, raise an error.

    """
    if isinstance(text, TEXT_TYPE):
        return text
    if not isinstance(text, BINARY_TYPE):
        raise TypeError('to_unicode must receive a bytes, str or unicode '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)


def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of `text`.

    If `text` is already a ``bytes`` object, return it as-is.
    If `text` is a ``unicode`` object, encode it using `encoding`.

    Otherwise, raise an error."""
    if isinstance(text, BINARY_TYPE):
        return text
    if not isinstance(text, TEXT_TYPE):
        raise TypeError('to_bytes must receive a unicode, str or bytes '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)


def to_native_str(text, encoding=None, errors='strict'):
    """Return ``str`` representation of `text`.

    ``str`` representation means ``bytes`` in PY2 and ``unicode`` in PY3.

    """
    if not isinstance(text, STRING_TYPE):
        return text
    if sys.version_info[0] < 3:
        return to_bytes(text, encoding, errors)
    return to_unicode(text, encoding, errors)
