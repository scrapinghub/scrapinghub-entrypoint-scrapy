import mock
import pytest
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured

from sh_scrapy.diskquota import DiskQuota
from sh_scrapy.diskquota import DiskQuotaDownloaderMiddleware
from sh_scrapy.diskquota import DiskQuotaSpiderMiddleware


def test_disk_quota_disabled():
    crawler = get_crawler()
    with pytest.raises(NotConfigured):
        DiskQuota(crawler)


@pytest.fixture
def crawler():
    return get_crawler(settings_dict={'DISK_QUOTA_STOP_ON_ERROR': True})


def test_disk_quota_init(crawler):
    dquota = DiskQuota(crawler)
    assert dquota.crawler == crawler


def test_disk_quota_from_crawler(crawler):
    assert isinstance(DiskQuota.from_crawler(crawler), DiskQuota)


def test_disk_quota_check_error(crawler):
    dquota = DiskQuota(crawler)
    assert not dquota._is_disk_quota_error(ValueError())
    assert not dquota._is_disk_quota_error(IOError())
    valid_error = IOError()
    valid_error.errno = 122
    assert dquota._is_disk_quota_error(valid_error)


def test_downloaded_mware_process_not_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaDownloaderMiddleware(crawler)
    mware.process_exception('request', ValueError(), 'spider')
    assert not crawler.engine.close_spider.called


def test_downloaded_mware_process_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaDownloaderMiddleware(crawler)
    error = IOError()
    error.errno = 122
    mware.process_exception('request', error, 'spider')
    assert crawler.engine.close_spider.called
    assert crawler.engine.close_spider.call_args[0] == (
        'spider', 'diskusage_exceeded')


def test_spider_mware_process_not_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaSpiderMiddleware(crawler)
    mware.process_spider_exception('response', ValueError(), 'spider')
    assert not crawler.engine.close_spider.called


def test_spider_mware_process_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaSpiderMiddleware(crawler)
    error = IOError()
    error.errno = 122
    mware.process_spider_exception('response', error, 'spider')
    assert crawler.engine.close_spider.called
    assert crawler.engine.close_spider.call_args[0] == (
        'spider', 'diskusage_exceeded')
