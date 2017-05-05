import os
import sys
import json
import mock
import pytest
import warnings
from scrapy.settings import Settings
from scrapy.exceptions import ScrapyDeprecationWarning

import sh_scrapy.crawl
from sh_scrapy.crawl import ignore_warnings
from sh_scrapy.crawl import _fatalerror
from sh_scrapy.crawl import _get_apisettings
from sh_scrapy.crawl import _run
from sh_scrapy.crawl import _run_scrapy
from sh_scrapy.crawl import _run_pkgscript
from sh_scrapy.crawl import _run_usercode
from sh_scrapy.crawl import _launch
from sh_scrapy.crawl import list_spiders
from sh_scrapy.crawl import main
from sh_scrapy.log import HubstorageLogHandler


@mock.patch.dict(os.environ, {'HWORKER_SENTRY_DSN': 'hw-sentry-dsn',
                              'SENTRY_DSN': 'sentry-dsn'})
def test_init_module():
    assert sh_scrapy.crawl._sys_stderr == sys.stderr
    assert sh_scrapy.crawl._sys_stdout == sys.stdout
    assert sh_scrapy.crawl.socket.getdefaulttimeout() == 60.0


def test_ignore_warnings():
    with ignore_warnings(category=ScrapyDeprecationWarning):
        warnings.warn("must be suppressed", ScrapyDeprecationWarning)


@mock.patch('traceback.print_exception')
def test_fatal_error(trace_print):
    exception = ValueError('some exception')
    traceback = None
    try:
        raise exception
    except:
        # get traceback before we cleaned it with fatalerror
        traceback = sys.exc_info()[2]
        _fatalerror()
    assert trace_print.called
    trace_args = trace_print.call_args_list[0]
    assert trace_args[0][0] == ValueError
    assert trace_args[0][1] == exception
    assert trace_args[0][2] == traceback
    assert trace_args[0][3] is None
    assert trace_args[0][4] == sys.stderr


@mock.patch('traceback.print_exception')
def test_fatal_error_ignore_IOError(trace_print):
    trace_print.side_effect = IOError('some error')
    try:
        raise ValueError('some exception')
    except:
        _fatalerror()
    assert trace_print.called


@mock.patch('sh_scrapy.crawl._sentry_dsn')
def test_fatal_error_sentry_import_error(sentry_dsn):
    try:
        raise ValueError('some exception')
    except:
        _fatalerror()


@mock.patch('sh_scrapy.crawl._sentry_dsn')
def test_fatal_error_sentry_with_mock(sentry_dsn):
    raven_stub = type('raven', (object, ), {})
    raven_stub.Client = mock.Mock()
    try:
        sys.modules['raven'] = raven_stub
        raise ValueError('some exception')
    except:
        _fatalerror()
    finally:
        del sys.modules['raven']
    assert raven_stub.Client.called
    assert raven_stub.Client.call_args[0] == (sentry_dsn,)
    sentry_client = raven_stub.Client.return_value
    assert sentry_client.captureException.called


@mock.patch('sh_scrapy.crawl._sentry_dsn')
def test_fatal_error_sentry_with_mock_ignore_errors(sentry_dsn):
    raven_stub = type('raven', (object, ), {})
    raven_stub.Client = mock.Mock()
    sentry_client = raven_stub.Client.return_value
    sentry_client.captureException.side_effect = IOError('error')
    try:
        sys.modules['raven'] = raven_stub
        raise ValueError('some exception')
    except:
        _fatalerror()
    finally:
        del sys.modules['raven']


def test_get_apisettings_empty():
    assert _get_apisettings() == {}


@mock.patch.dict(os.environ, {
    'SHUB_SETTINGS': 'data:;base64,ImhlbGxvIHdvcmxkIg=='})
def test_get_apisettings_from_env():
    assert _get_apisettings() == 'hello world'


@mock.patch('sh_scrapy.crawl._run_pkgscript')
def test_run_pkg_script(run_pkg_mock):
    _run(['py:script.py'], {'SETTING': 'VALUE'})
    assert run_pkg_mock.called
    assert run_pkg_mock.call_args[0] == (['py:script.py'],)


@mock.patch('pkg_resources.WorkingSet')
def test_run_pkg_script_distribution_not_found(working_set_class):
    fake_set = mock.Mock()
    fake_set.iter_entry_points.return_value = iter(())
    working_set_class.return_value = fake_set
    with pytest.raises(ValueError):
        _run(['py:script.py'], {'SETTING': 'VALUE'})


@mock.patch('sh_scrapy.crawl._run_scrapy')
def test_run_scrapy_spider(run_scrapy_mock):
    _run(['scrapy', 'crawl', 'spider'], {'SETTING': 'VALUE'})
    assert run_scrapy_mock.called
    assert run_scrapy_mock.call_args[0] == (
        ['scrapy', 'crawl', 'spider'], {'SETTING': 'VALUE'})


@mock.patch('scrapy.cmdline.execute')
def test_run_scrapy(execute_mock):
    _run_scrapy(['scrapy', 'crawl', 'spider'], {'SETTING': 'VALUE'})
    assert execute_mock.called
    assert execute_mock.call_args == (
        {'settings': {'SETTING': 'VALUE'}},)
    assert sys.argv == ['scrapy', 'crawl', 'spider']


def get_working_set(working_set_class):
    """Helper to confugure a fake working set with ep"""
    working_set = working_set_class.return_value
    ep = mock.Mock()
    ep.name = 'settings'
    working_set.iter_entry_points.return_value = [ep]
    return working_set


@mock.patch('pkg_resources.WorkingSet')
def test_run_pkgscript_base_usage(working_set_class):
    working_set = get_working_set(working_set_class)
    _run_pkgscript(['py:script.py', 'arg1', 'arg2'])
    assert working_set.iter_entry_points.called
    assert working_set.iter_entry_points.call_args[0] == ('scrapy',)
    ep = working_set.iter_entry_points.return_value[0]
    assert ep.dist.run_script.called
    assert ep.dist.run_script.call_args[0] == (
        'script.py', {'__name__': '__main__'})
    assert sys.argv == ['script.py', 'arg1', 'arg2']


@mock.patch.dict(os.environ, {
    'SHUB_SETTINGS': '{"project_settings": {"SETTING....'})
@mock.patch('sh_scrapy.crawl._run')
def test_run_usercode_bad_settings(mocked_run):
    with pytest.raises(ValueError):
        _run_usercode('py:script.py', ['py:script.py'], _get_apisettings)
    assert not mocked_run.called


@mock.patch.dict(os.environ, {
    'SHUB_SETTINGS': '{"project_settings": {"SETTING_TEST": "VAL"}}'})
@mock.patch('sh_scrapy.crawl._run')
def test_run_usercode_run_exception(mocked_run):
    mocked_run.side_effect = AttributeError('argA is missing')
    with pytest.raises(AttributeError):
        _run_usercode('py:script.py', ['py:script.py'], _get_apisettings)
    assert mocked_run.called


@mock.patch.dict(os.environ, {
    'SHUB_SETTINGS': '{"project_settings": {"SETTING_TEST": "VAL"}}'})
@mock.patch('sh_scrapy.crawl._run')
def test_run_usercode(mocked_run):
    _run_usercode('py:script.py', ['py:script.py', 'arg1'], _get_apisettings)
    assert mocked_run.called
    assert mocked_run.call_args[0][0] == ['py:script.py', 'arg1']
    settings = mocked_run.call_args[0][1]
    assert isinstance(settings, Settings)
    assert settings['SETTING_TEST'] == 'VAL'


@mock.patch.dict(os.environ, {
    'SHUB_SETTINGS': '{"project_settings": {"LOG_LEVEL": 10}}'})
@mock.patch('sh_scrapy.crawl._run')
def test_run_usercode_with_loghandler(mocked_run):
    loghandler = mock.Mock()
    _run_usercode('py:script.py', ['py:script.py', 'arg1'],
                  _get_apisettings, loghandler)
    assert mocked_run.called
    assert loghandler.setLevel.called
    call_args = loghandler.setLevel.call_args[0]
    assert len(call_args) == 1
    assert call_args[0] == 10


SPIDER_MSG = {
    'key': '1/2/3', 'spider': 'test', 'spider_type': 'auto',
    'auth': 'auths', 'spider_args': {'arg1': 'val1', 'arg2': 'val2'},
    'settings': {'SETTING1': 'VAL1', 'SETTING2': 'VAL2'}
}


@mock.patch('sh_scrapy.crawl._fatalerror')
def test_launch_handle_fatalerror(mocked_fatalerr):
    with pytest.raises(AssertionError):
        _launch()
    assert mocked_fatalerr.called


@mock.patch.dict(os.environ, {'SHUB_JOB_DATA': json.dumps(SPIDER_MSG)})
@mock.patch('sh_scrapy.env.setup_environment')
@mock.patch('sh_scrapy.crawl._run_usercode')
def test_launch(mocked_run, mocked_setup):
    _launch()
    expected_env = {
        'SCRAPY_SPIDER': 'test', 'SHUB_JOBNAME': 'test',
        'SCRAPY_JOB': '1/2/3', 'SCRAPY_PROJECT_ID': '1',
        'SHUB_JOBKEY': '1/2/3', 'SHUB_JOB_TAGS': '',
        'SHUB_JOBAUTH': '312f322f333a6175746873',
        'SHUB_SPIDER_TYPE': 'auto'}
    for k, v in expected_env.items():
        assert os.environ.get(k) == v
    assert mocked_run.called
    run_args = mocked_run.call_args[0]
    assert run_args[0] == 'test'
    expected_args = [
        'scrapy', 'crawl', 'test', '-a', 'arg1=val1', '-a',
        'arg2=val2', '-s', 'SETTING1=VAL1', '-s', 'SETTING2=VAL2']
    assert run_args[1] == expected_args
    assert run_args[2] == _get_apisettings
    assert isinstance(run_args[3], HubstorageLogHandler)
    assert mocked_setup.called


@mock.patch('sh_scrapy.env.setup_environment')
@mock.patch('sh_scrapy.crawl._run_usercode')
def test_list_spiders(mocked_run, mocked_setup):
    list_spiders()
    assert mocked_run.called
    run_args = mocked_run.call_args[0]
    assert run_args[0] is None
    expected_args = ['scrapy', 'list']
    assert run_args[1] == expected_args
    assert run_args[2] == _get_apisettings
    assert mocked_setup.called


@mock.patch('sh_scrapy.crawl._fatalerror')
@mock.patch('sh_scrapy.env.setup_environment')
def test_list_spiders_handle_fatalerror(mocked_setup, mocked_fatalerr):
    mocked_setup.side_effect = AttributeError('some error')
    with pytest.raises(AttributeError):
        list_spiders()
    assert mocked_fatalerr.called


@mock.patch('sh_scrapy.writer.pipe_writer')
@mock.patch('sh_scrapy.crawl._launch')
def test_main(mocked_launch, pipe_writer):
    main()
    assert pipe_writer.open.called
    assert mocked_launch.called
    assert mocked_launch.call_args == ()
    assert sys.stdout == sh_scrapy.crawl._sys_stdout
    assert sys.stderr == sh_scrapy.crawl._sys_stderr
    # Pipe writer file object is closed implicitly on program exit.
    # This ensures that pipe is writable even if main program is fininshed -
    # e.g. for threads that are not closed yet.
    assert not pipe_writer.close.called
