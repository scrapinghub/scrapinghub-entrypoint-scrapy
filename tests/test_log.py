import os
import sys
import mock
import pytest
import logging

import scrapy.log
from sh_scrapy.log import _logfn, _dummy
from sh_scrapy.log import initialize_logging
from sh_scrapy.log import HubstorageLogHandler
from sh_scrapy.log import HubstorageLogObserver
from sh_scrapy.log import StdoutLogger


@mock.patch('sh_scrapy.hsref.hsref')
def test_logfn_writer_opened(hsref):
    args = ('test', 'logging')
    kwargs = {'x': 'parX', 'y': 'parY'}
    hsref.job.logs._writer.closed = False
    _logfn(*args, **kwargs)
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args[0] == args
    assert hsref.job.logs.log.call_args[1] == kwargs


@mock.patch('twisted.python.log.startLoggingWithObserver')
@mock.patch('sh_scrapy.log.HubstorageLogObserver')
def test_initialize_logging_dont_fail(observer, txlog_start):
    loghandler = initialize_logging()

    rootlogger = logging.getLogger()
    assert rootlogger.level == logging.NOTSET

    # check if null handler is set for libs
    for lib in ('boto', 'requests', 'hubstorage'):
        lg = logging.getLogger(lib)
        assert lg.propagate == 0
        assert any([hdl for hdl in lg.handlers
                    if isinstance(hdl, logging.NullHandler)])

    # check standard out/err redirection
    assert isinstance(sys.stdout, StdoutLogger)
    assert sys.stdout.encoding == 'utf-8'
    assert isinstance(sys.stderr, StdoutLogger)
    assert sys.stderr.encoding == 'utf-8'

    # check twisted specific
    assert observer.called
    assert observer.call_args[0] == (loghandler,)
    emit_method = observer.return_value.emit
    assert txlog_start.called
    assert txlog_start.call_args[0] == (emit_method,)
    assert txlog_start.call_args[1] == {'setStdout': False}

    # check returned handler
    assert isinstance(loghandler, HubstorageLogHandler)
    assert loghandler.level == logging.INFO
    assert loghandler.formatter._fmt == '[%(name)s] %(message)s'


@mock.patch.dict(os.environ, {'SCRAPY_JOB': '1/2/3'})
@mock.patch('sh_scrapy.hsref.hsref')
def test_initialize_logging_test_scrapy_specific(hsref):
    """Make sure we reset scrapy.log.start"""
    loghandler = initialize_logging()
    assert scrapy.log.start == _dummy


def test_dummy_doesnt_fail():
    _dummy()


@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_loghandler_emit_ok(hsref):
    hsref.job.logs._writer.closed = False
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    hdlr.emit(record)
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args[0] == ('test-record',)


@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_loghandler_emit_handle_interrupt(hsref):
    hsref.job.logs._writer.closed = False
    hsref.job.logs.log.side_effect = KeyboardInterrupt
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    with pytest.raises(KeyboardInterrupt):
        hdlr.emit(record)


@mock.patch('logging.Handler.handleError')
@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_loghandler_emit_handle_exception(hsref, handleError):
    hsref.job.logs._writer.closed = False
    hsref.job.logs.log.side_effect = ValueError
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    hdlr.emit(record)
    assert handleError.called
    assert handleError.call_args[0] == (hdlr, record)


@pytest.fixture
def hs_observer():
    hdlr = mock.Mock()
    return HubstorageLogObserver(hdlr)


def test_hs_logobserver_init(hs_observer):
    assert isinstance(hs_observer._hs_loghdlr, mock.Mock)


def test_hs_logobserver_get_log_item_low_level(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'scrapy', 'logLevel': 10}
    assert not hs_observer._get_log_item(event)


def test_hs_logobserver_get_log_item_system(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'scrapy', 'logLevel': 30, 'message': ['test']}
    assert hs_observer._get_log_item(event) == {
        'level': 30, 'message': 'test'}


def test_hs_logobserver_get_log_item_info(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'isError': False}
    assert hs_observer._get_log_item(event) == {
        'level': 20, 'message': 'test'}


def test_hs_logobserver_get_log_item_error(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'isError': True}
    assert hs_observer._get_log_item(event) == {
        'level': 40, 'message': 'test'}


def test_hs_logobserver_get_log_item_failure(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    failure = mock.Mock()
    failure.getTraceback.return_value = 'some-traceback'
    event = {'system': 'other', 'failure': failure, 'isError': False}
    assert hs_observer._get_log_item(event) == {
        'level': 20, 'message': 'some-traceback'}


def test_hs_logobserver_get_log_item_why(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'],
             'why': 'why-msg', 'isError': False}
    assert hs_observer._get_log_item(event) == {
        'level': 20, 'message': 'why-msg\n\ttest'}


def test_hs_logobserver_get_log_item_format(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'data': 'raw',
             'format': 'formatted/%(data)s', 'isError': False}
    assert hs_observer._get_log_item(event) == {
        'level': 20, 'message': 'formatted/raw'}


def test_hs_logobserver_get_log_item_format_error(hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'data': 'raw',
             'format': 'formatted/%(data)%%', 'isError': False}
    expected_template = "UNABLE TO FORMAT LOG MESSAGE: fmt=%r ev=%r"
    assert hs_observer._get_log_item(event) == {
        'level': 40, 'message': expected_template % (event['format'], event)}


@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_logobserver_emit_filter_events(hsref, hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'scrapy', 'logLevel': 10}
    hs_observer.emit(event)
    assert not hsref.job.logs.log.called


@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_logobserver_emit_logitem(hsref, hs_observer):
    hsref.job.logs._writer.closed = False
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'isError': False}
    hs_observer.emit(event)
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args[0] == ()
    assert hsref.job.logs.log.call_args[1] == {'level': 20, 'message': 'test'}


def stdout_logger_init_stdout():
    logger_out = StdoutLogger(0, 'utf-8')
    assert logger_out.prefix == '[stdout]'
    assert logger_out.loglevel == logging.INFO


def stdout_logger_init_stderr():
    logger_out = StdoutLogger(1, 'utf-8', loglevel=logging.ERROR)
    assert logger_out.prefix == '[stderr]'
    assert logger_out.loglevel == logging.ERROR


@mock.patch('sh_scrapy.hsref.hsref')
def test_stdout_logger_logprefixed(hsref):
    logger = StdoutLogger(0, 'utf-8')
    hsref.job.logs._writer.closed = False
    logger._logprefixed('message')
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args[0] == ()
    assert hsref.job.logs.log.call_args[1] == {
        'level': 20, 'message': '[stdout] message'}


@mock.patch('sh_scrapy.hsref.hsref')
def test_stdout_logger_write(hsref):
    logger = StdoutLogger(0, 'utf-8')
    hsref.job.logs._writer.closed = False
    logger.write('some-string\nother-string\nlast-string')
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args_list == [
        ({'level': 20, 'message': '[stdout] some-string'},),
        ({'level': 20, 'message': '[stdout] other-string'},)]
    assert logger.buf == 'last-string'


def test_stdout_logger_writelines_empty():
    logger = StdoutLogger(0, 'utf-8')
    logger.writelines([])


@mock.patch('sh_scrapy.hsref.hsref')
def test_stdout_logger_writelines(hsref):
    logger = StdoutLogger(0, 'utf-8')
    hsref.job.logs._writer.closed = False
    logger.writelines(['test-line'])
    assert hsref.job.logs.log.called
    assert hsref.job.logs.log.call_args[0] == ()
    assert hsref.job.logs.log.call_args[1] == {
        'level': 20, 'message': '[stdout] test-line'}
