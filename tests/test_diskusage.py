import os
import mock
import pytest
from scrapy.crawler import Crawler
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured

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
        (['find', '/tmp1', '/tmp2', '-user', str(os.getuid()),
         '-type', 'f', '-printf', '%s\n'],), {'stdout':-1})
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


def test_diskusage_init_with_void_settings():
    crawler = get_crawler(settings_dict = {})
    with pytest.raises(NotConfigured):
        DiskUsage(crawler)


@pytest.fixture
def du_extension():
    settings = {'DISKUSAGE_ENABLED': True,
                'DISKUSAGE_SPACE_LIMIT_MB': 512,
                'DISKUSAGE_INODES_LIMIT': 80000,
                'DISKUSAGE_CHECK_INTERVAL_SECONDS': 300}
    crawler = get_crawler(settings_dict=settings)
    return DiskUsage(crawler)


def test_diskusage_init(du_extension):
    assert isinstance(du_extension.crawler, Crawler)
    assert du_extension.space_limit == 512 * 1024 * 1024
    assert du_extension.inodes_limit == 80000
    assert du_extension.check_interval == 300


@mock.patch('twisted.internet.task.LoopingCall')
@mock.patch('sh_scrapy.diskusage.get_disk_usage')
def test_diskusage_engine_started(get_disk_usage, looping_call, du_extension):
    get_disk_usage.return_value = (500, 1000)
    du_extension.engine_started()
    assert du_extension.crawler.stats.get_value(
        'diskusage/inodes/startup') == 500
    assert du_extension.crawler.stats.get_value(
        'diskusage/space/startup') == 1000
    assert looping_call.called
    assert looping_call.call_args[0] == (du_extension._task_handler,)
    assert du_extension.task
    assert du_extension.task.start.called
    assert du_extension.task.start.call_args == ((300,), {'now': True})


def test_diskusage_engine_stopped(du_extension):
    du_extension.task = mock.Mock()
    du_extension.task.running = True
    du_extension.engine_stopped()
    assert du_extension.task.stop.called


@mock.patch('sh_scrapy.diskusage.get_disk_usage')
def test_diskusage_task_handler_max_stats(get_disk_usage, du_extension):
    stats = du_extension.crawler.stats
    stats.set_value('diskusage/inodes/max', 100)
    stats.set_value('diskusage/space/max', 100000)
    get_disk_usage.return_value = (500, 90000)
    du_extension._task_handler()
    assert stats.get_value('diskusage/inodes/max') == 500
    assert stats.get_value('diskusage/space/max') == 100000


@mock.patch('sh_scrapy.diskusage.get_disk_usage')
def test_diskusage_task_handler_raise_limit(get_disk_usage, du_extension):
    for disk_usage in [(500, 1024*1024*1024), (90000, 100)]:
        get_disk_usage.return_value = disk_usage
        du_extension.task = mock.Mock()
        du_extension.task.running = True
        engine_mock = mock.Mock()
        engine_mock.open_spiders = [mock.Mock()]
        du_extension.crawler.engine = engine_mock
        du_extension.crawler.stop = mock.Mock()
        du_extension._task_handler()
        assert du_extension.crawler.stats.get_value(
            'diskusage/limit_reached') == 1
        assert engine_mock.close_spider.called
        assert engine_mock.close_spider.call_args == (
            (engine_mock.open_spiders[0], 'diskusage_exceeded'),)
        assert not du_extension.crawler.stop.called


@mock.patch('sh_scrapy.diskusage.get_disk_usage')
def test_diskusage_task_handler_no_open_spiders(get_disk_usage, du_extension):
    get_disk_usage.return_value = (500, 1024*1024*1024)
    engine_mock = mock.Mock()
    engine_mock.open_spiders = []
    du_extension.crawler.engine = engine_mock
    du_extension.crawler.stop = mock.Mock()
    du_extension._task_handler()
    assert du_extension.crawler.stop.called
