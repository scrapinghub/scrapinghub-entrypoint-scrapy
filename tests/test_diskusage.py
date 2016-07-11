import mock
import pytest

from sh_scrapy.diskusage import get_disk_usage
from sh_scrapy.diskusage import DiskUsage


def test_get_disk_usage_wrong_usage():
    with pytest.raises(AssertionError):
        get_disk_usage([])


@mock.patch('sh_scrapy.diskusage.Popen')
def test_get_disk_usage_base_calls_check(popen):
    assert get_disk_usage(['/tmp1', '/tmp2']) == (0, 0)
    assert popen.called
    assert popen.call_args_list[0] == (
        (['find', '/tmp1', '/tmp2', '-user', '501', '-type',
         'f', '-printf', '%s\n'],), {'stdout':-1})
    assert popen.call_args_list[1] == (
        (['awk', '{i++;s+=$1}END{print i" "s}'],),
        {'stdin': popen.return_value.stdout, 'stdout': -1})
    assert popen.return_value.stdout.close.called
    assert popen.return_value.communicate.called


@mock.patch('sh_scrapy.diskusage.Popen')
def test_get_disk_usage_mock_result(popen):
    find_result = '1234 100500\n'
    popen.return_value.communicate.return_value = (find_result, None)
    assert get_disk_usage(['/tmp']) == (1234, 100500)


@mock.patch('sh_scrapy.diskusage.Popen')
def test_get_disk_usage_swallow_exception(popen):
    popen.return_value.communicate.side_effect = ValueError('crush!')
    get_disk_usage(['/tmp']) == (0, 0)
