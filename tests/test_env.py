import os
import sys
import mock
import codecs
import pytest
import tempfile

from sh_scrapy.compat import to_bytes, to_native_str

from sh_scrapy.env import _jobauth
from sh_scrapy.env import _jobname
from sh_scrapy.env import decode_uri
from sh_scrapy.env import get_args_and_env
from sh_scrapy.env import _job_args_and_env
from sh_scrapy.env import _make_scrapy_args
from sh_scrapy.env import setup_environment
from sh_scrapy.env import _scrapy_crawl_args_and_env


def test_make_scrapy_args():
    assert _make_scrapy_args('-a', {}) == []
    assert _make_scrapy_args('-a', {'test': 'val'}) == ['-a', 'test=val']
    result1 = _make_scrapy_args('-s', [('k1', 'v1'), ('k2', 'v2')])
    assert result1 == ['-s', 'k1=v1', '-s', 'k2=v2']
    result2 = _make_scrapy_args('-s', [('arg1', 'val1'), ('arg2', 'val2')])
    assert result2 == ['-s', 'arg1=val1', '-s', 'arg2=val2']
    result3 = _make_scrapy_args('-s', [('arg1', 1), ('arg2', 2)])
    assert result3 == ['-s', 'arg1=1', '-s', 'arg2=2']


def test_scrapy_crawl_args_and_env():
    # test with minimal message
    result = _scrapy_crawl_args_and_env({'key': '1/2/3', 'spider': 'test'})
    assert len(result) == 2
    assert result[0] == ['scrapy', 'crawl', 'test']
    assert result[1] == {'SCRAPY_JOB': '1/2/3', 'SCRAPY_PROJECT_ID': '1',
                         'SCRAPY_SPIDER': 'test', 'SHUB_SPIDER_TYPE': ''}
    # test with full message
    result1 = _scrapy_crawl_args_and_env(
        {'key': '1/2/3', 'spider': 'test',
         'spider_args': [('arg1', 'val1'), ('arg2', 'val2')],
         'settings': [('SETTING1', 'VAL1'), ('SETTING2', 'VAL2')],
         'spider_type': 'auto',
         'extra_args': ['some', 'extra', 'args']})
    assert result1[0] == ['scrapy', 'crawl', 'test', '-a', 'arg1=val1',
                          '-a', 'arg2=val2', '-s', 'SETTING1=VAL1',
                          '-s', 'SETTING2=VAL2']
    assert result1[1] == {'SCRAPY_JOB': '1/2/3', 'SCRAPY_PROJECT_ID': '1',
                          'SCRAPY_SPIDER': 'test', 'SHUB_SPIDER_TYPE': 'auto'}


def test_job_args_and_env():
    result = _job_args_and_env({'job_cmd': ['custom.py', 'arg1']})
    assert result == (['custom.py', 'arg1'], {})
    result1 = _job_args_and_env({'job_cmd': ['custom.py', 'arg1'],
                                 'job_env': {'some': 'env'}})
    assert result1 == (['custom.py', 'arg1'], {'some': 'env'})
    result2 = _job_args_and_env({'job_cmd': ('wrong', 'cmd', 'style')})
    assert result2 == (["('wrong', 'cmd', 'style')"], {})


def test_jobname():
    msg = {'job_name': 'jobn', 'spider': 'test',
           'job_cmd': ['custom.py', 'arg1', 'arg2']}
    assert _jobname(msg) == 'jobn'
    msg.pop('job_name')
    assert _jobname(msg) == 'test'
    msg.pop('spider')
    assert _jobname(msg) == 'custom.py'


def test_jobauth():
    msg = {'key': '1/2/3', 'auth': 'authstring'}
    expected = codecs.encode(to_bytes('1/2/3:authstring'), 'hex_codec')
    assert _jobauth(msg) == to_native_str(expected)


def test_get_args_and_env_run_spider():
    msg = {'key': '1/2/3', 'spider': 'test', 'spider_type': 'auto',
           'auth': 'auths', 'spider_args': {'arg1': 'val1', 'arg2': 'val2'},
           'settings': {'SETTING1': 'VAL1', 'SETTING2': 'VAL2'}}
    result = get_args_and_env(msg)
    expected_auth = codecs.encode(to_bytes('1/2/3:auths'), 'hex_codec')
    assert len(result) == 2
    assert result[0] == ['scrapy', 'crawl', 'test', '-a', 'arg1=val1',
                         '-a', 'arg2=val2', '-s', 'SETTING1=VAL1', '-s',
                         'SETTING2=VAL2']
    assert result[1] == {'SCRAPY_JOB': '1/2/3',
                         'SCRAPY_PROJECT_ID': '1',
                         'SCRAPY_SPIDER': 'test',
                         'SHUB_JOBAUTH': to_native_str(expected_auth),
                         'SHUB_JOBKEY': '1/2/3',
                         'SHUB_JOBNAME': 'test',
                         'SHUB_JOB_TAGS': '',
                         'SHUB_SPIDER_TYPE': 'auto'}
    add_fields = {'tags': ['tagA', 'tagB'], 'api_url': 'some-api-url'}
    msg.update(add_fields)
    result1 = get_args_and_env(msg)
    assert len(result1) == 2
    assert result1[1]['SHUB_APIURL'] == 'some-api-url'
    assert result1[1]['SHUB_JOB_TAGS'] == 'tagA,tagB'


def test_get_args_and_env_run_script():
    msg = {'key': '1/2/3', 'job_cmd': ['custom.py', 'arg1'],
           'auth': 'authstring'}
    result = get_args_and_env(msg)
    expected_auth = codecs.encode(to_bytes('1/2/3:authstring'), 'hex_codec')
    assert len(result) == 2
    assert result[0] == ['custom.py', 'arg1']
    assert result[1] == {
        'SHUB_JOBAUTH': to_native_str(expected_auth),
        'SHUB_JOBKEY': '1/2/3',
        'SHUB_JOBNAME': 'custom.py',
        'SHUB_JOB_TAGS': ''}
    add_fields = {'tags': ['tagA', 'tagB'], 'api_url': 'some-api-url'}
    msg.update(add_fields)
    result1 = get_args_and_env(msg)
    assert len(result1) == 2
    assert result1[1]['SHUB_APIURL'] == 'some-api-url'
    assert result1[1]['SHUB_JOB_TAGS'] == 'tagA,tagB'


def test_decode_uri_basic_usage():
    assert decode_uri('{"spider": "hello"}') == {'spider': 'hello'}
    str1 = 'data:application/json;charset=utf8;base64,ImhlbGxvIHdvcmxkIg=='
    assert decode_uri(str1) == u'hello world'
    assert decode_uri('data:;base64,ImhlbGxvIHdvcmxkIg==') == 'hello world'
    str2 = 'data:custom-mime;charset=utf8;base64,ImhlbGxvIHdvcmxkIg=='
    assert decode_uri(str2) == b'"hello world"'


@mock.patch.dict(os.environ, {'TEST_VAR': '{"spider": "hello"}'})
def test_decode_uri_from_env():
    assert decode_uri(None, 'TEST_VAR') == {'spider': 'hello'}


def test_decode_uri_var_or_env_is_needed():
    with pytest.raises(ValueError):
        decode_uri()


def test_decode_uri_from_file():
    with tempfile.NamedTemporaryFile() as temp:
        temp.write('{"hello":"world"}'.encode('utf-8'))
        temp.flush()
        assert decode_uri(temp.name) == {'hello': 'world'}
        assert decode_uri('file://' + temp.name) == {'hello': 'world'}


def test_setup_environment():
    builtin_mod = '__builtin__' if sys.version_info < (3,) else 'builtins'
    with mock.patch(builtin_mod + '.open') as mock_open:
        setup_environment()
        assert mock_open.called
