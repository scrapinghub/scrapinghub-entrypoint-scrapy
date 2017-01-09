import json
import logging
import mock
import pytest
import sys
import zlib

import scrapy.log
from sh_scrapy.log import _dummy, _stdout, _stderr
from sh_scrapy.log import initialize_logging
from sh_scrapy.log import HubstorageLogHandler
from sh_scrapy.log import HubstorageLogObserver
from sh_scrapy.log import StdoutLogger


@pytest.fixture(autouse=True)
def reset_std_streams():
    sys.stdout = _stdout
    sys.stderr = _stderr


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
    observer.assert_called_with(loghandler)
    emit_method = observer.return_value.emit
    assert txlog_start.called
    txlog_start.assert_called_with(emit_method, setStdout=False)

    # check returned handler
    assert isinstance(loghandler, HubstorageLogHandler)
    assert loghandler.level == logging.INFO
    assert loghandler.formatter._fmt == '[%(name)s] %(message)s'


@mock.patch('sh_scrapy.log.pipe_writer')
def test_initialize_logging_test_scrapy_specific(pipe_writer):
    """Make sure we reset scrapy.log.start"""
    loghandler = initialize_logging()
    assert scrapy.log.start == _dummy
    # test it doesn't fail
    scrapy.log.start()


@mock.patch('sh_scrapy.log.pipe_writer')
def test_hs_loghandler_emit_ok(pipe_writer):
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    hdlr.emit(record)
    assert pipe_writer.write_log.called
    pipe_writer.write_log.assert_called_with(message='test-record', level=None)


@mock.patch('sh_scrapy.log.pipe_writer')
def test_hs_loghandler_emit_handle_interrupt(pipe_writer):
    pipe_writer.write_log.side_effect = KeyboardInterrupt
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    with pytest.raises(KeyboardInterrupt):
        hdlr.emit(record)


@mock.patch('logging.Handler.handleError')
@mock.patch('sh_scrapy.log.pipe_writer')
def test_hs_loghandler_emit_handle_exception(pipe_writer, handleError):
    pipe_writer.write_log.side_effect = ValueError
    hdlr = HubstorageLogHandler()
    record = logging.makeLogRecord({'msg': 'test-record'})
    hdlr.emit(record)
    assert handleError.called
    assert handleError.call_args == mock.call(record)


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


@mock.patch('sh_scrapy.log.pipe_writer')
def test_hs_logobserver_emit_filter_events(pipe_writer, hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'scrapy', 'logLevel': 10}
    hs_observer.emit(event)
    assert not pipe_writer.write_log.called


@mock.patch('sh_scrapy.log.pipe_writer')
def test_hs_logobserver_emit_logitem(pipe_writer, hs_observer):
    hs_observer._hs_loghdlr.level = 20
    event = {'system': 'other', 'message': ['test'], 'isError': False}
    hs_observer.emit(event)
    assert pipe_writer.write_log.called
    pipe_writer.write_log.assert_called_with(level=20, message='test')


def stdout_logger_init_stdout():
    logger_out = StdoutLogger(0, 'utf-8')
    assert logger_out.prefix == '[stdout]'
    assert logger_out.loglevel == logging.INFO


def stdout_logger_init_stderr():
    logger_out = StdoutLogger(1, 'utf-8', loglevel=logging.ERROR)
    assert logger_out.prefix == '[stderr]'
    assert logger_out.loglevel == logging.ERROR


@mock.patch('sh_scrapy.log.pipe_writer')
def test_stdout_logger_logprefixed(pipe_writer):
    logger = StdoutLogger(0, 'utf-8')
    logger._logprefixed('message')
    assert pipe_writer.write_log.called
    pipe_writer.write_log.assert_called_with(level=20, message='[stdout] message')


@mock.patch('sh_scrapy.log.pipe_writer')
def test_stdout_logger_write(pipe_writer):
    logger = StdoutLogger(0, 'utf-8')
    logger.write('some-string\nother-string\nlast-string')
    assert pipe_writer.write_log.called
    assert pipe_writer.write_log.call_args_list[0] == mock.call(
        level=20,
        message='[stdout] some-string'
    )
    assert pipe_writer.write_log.call_args_list[1] == mock.call(
        level=20,
        message='[stdout] other-string'
    )
    assert logger.buf == 'last-string'


def test_stdout_logger_writelines_empty():
    logger = StdoutLogger(0, 'utf-8')
    logger.writelines([])


@mock.patch('sh_scrapy.log.pipe_writer')
def test_stdout_logger_writelines(pipe_writer):
    logger = StdoutLogger(0, 'utf-8')
    logger.writelines(['test-line'])
    assert pipe_writer.write_log.called
    pipe_writer.write_log.assert_called_with(level=20, message='[stdout] test-line')


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
@mock.patch('sh_scrapy.log.pipe_writer._pipe')
def test_unicode_decode_error_handling(pipe_mock):
    hdlr = HubstorageLogHandler()
    message = 'value=%s' % zlib.compress('value')
    record = logging.makeLogRecord({'msg': message, 'levelno': 10})
    hdlr.emit(record)
    assert pipe_mock.write.called
    payload = json.loads(pipe_mock.write.call_args_list[2][0][0])
    assert isinstance(payload.pop('time'), int)
    assert payload == {
        'message': r'value=x\x9c+K\xcc)M\x05\x00\x06j\x02\x1e',
        'level': 10
    }
