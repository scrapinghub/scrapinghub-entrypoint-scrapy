import asyncio

import mock
import pytest
import scrapy
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured
from scrapy.utils.defer import deferred_f_from_coro_f
from twisted.internet import defer

from sh_scrapy import _SCRAPY_NO_SPIDER_ARG
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
    other_valid_error = OSError()
    other_valid_error.errno = 122
    assert dquota._is_disk_quota_error(other_valid_error)


def test_downloaded_mware_process_not_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaDownloaderMiddleware(crawler)
    if _SCRAPY_NO_SPIDER_ARG:
        result = mware.process_exception('request', ValueError())
        asyncio.run(result)  # consume the coroutine to avoid warnings
    else:
        mware.process_exception('request', ValueError(), 'spider')
    assert not crawler.engine.close_spider.called
    if _SCRAPY_NO_SPIDER_ARG:
        assert not crawler.engine.close_spider_async.called


def test_downloaded_mware_process_stopped(crawler):
    crawler.engine = mock.Mock()
    # Mock close_spider_async to return a coroutine
    async def mock_close_spider_async(**kwargs):
        pass
    crawler.engine.close_spider_async = mock.Mock(side_effect=mock_close_spider_async)

    mware = DiskQuotaDownloaderMiddleware(crawler)
    error = IOError()
    error.errno = 122
    if _SCRAPY_NO_SPIDER_ARG:
        result = mware.process_exception('request', error)
        asyncio.run(result)
        assert crawler.engine.close_spider_async.called
        assert crawler.engine.close_spider_async.call_args[1] == {'reason': 'diskusage_exceeded'}
    else:
        mware.process_exception('request', error, 'spider')
        assert crawler.engine.close_spider.called
        assert crawler.engine.close_spider.call_args[0] == ('spider', 'diskusage_exceeded')


def test_spider_mware_process_not_stopped(crawler):
    crawler.engine = mock.Mock()
    mware = DiskQuotaSpiderMiddleware(crawler)
    if _SCRAPY_NO_SPIDER_ARG:
        mware.process_spider_exception('response', ValueError())
    else:
        mware.process_spider_exception('response', ValueError(), 'spider')
    assert not crawler.engine.close_spider.called


def test_spider_mware_process_stopped(crawler):
    crawler.engine = mock.Mock()
    # Mock close_spider_async to return a coroutine
    async def mock_close_spider_async(**kwargs):
        pass
    crawler.engine.close_spider_async = mock.Mock(side_effect=mock_close_spider_async)

    mware = DiskQuotaSpiderMiddleware(crawler)
    error = IOError()
    error.errno = 122

    async def run_test():
        if _SCRAPY_NO_SPIDER_ARG:
            mware.process_spider_exception('response', error)
            # Wait a bit for the task to complete
            await asyncio.sleep(0.1)
            assert crawler.engine.close_spider_async.called
            assert crawler.engine.close_spider_async.call_args[1] == {'reason': 'diskusage_exceeded'}
        else:
            mware.process_spider_exception('response', error, 'spider')
            assert crawler.engine.close_spider.called
            assert crawler.engine.close_spider.call_args[0] == ('spider', 'diskusage_exceeded')

    asyncio.run(run_test())


@pytest.mark.parametrize("asyncio_available", [True, False])
@deferred_f_from_coro_f
async def test_spider_mware_process_stopped_integration(asyncio_available):
    """Test that disk quota error closes spider with correct reason."""
    settings = {
        "DISK_QUOTA_STOP_ON_ERROR": True,
        "SPIDER_MIDDLEWARES": {
            "sh_scrapy.diskquota.DiskQuotaSpiderMiddleware": 100,
        },
    }
    crawler = get_crawler(settings_dict=settings)

    class ErrorSpider(scrapy.Spider):
        name = "error_spider"
        start_urls = ["data:,"]

        def parse(self, response):
            error = IOError()
            error.errno = 122
            raise error

    with mock.patch("scrapy.utils.asyncio.is_asyncio_available", return_value=asyncio_available):
        yield crawler.crawl(ErrorSpider)
    assert crawler.stats.get_value("finish_reason") == "diskusage_exceeded"
