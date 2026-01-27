from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import scrapy
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured
from scrapy.utils.defer import deferred_f_from_coro_f, maybe_deferred_to_future

from sh_scrapy.diskquota import DiskQuota

if TYPE_CHECKING:
    from scrapy import Spider


def test_disk_quota_disabled():
    crawler = get_crawler()
    with pytest.raises(NotConfigured):
        DiskQuota(crawler)


@pytest.fixture
def crawler():
    return get_crawler(settings_dict={"DISK_QUOTA_STOP_ON_ERROR": True})


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


class RaiseValueErrorMiddleware:
    def process_request(self, request, spider: Spider | None = None):
        raise ValueError("Simulated ValueError")


class RaiseDiskErrorMiddleware:
    def process_request(self, request, spider: Spider | None = None):
        error = IOError()
        error.errno = 122
        raise error


@deferred_f_from_coro_f
async def test_downloaded_mware_process_not_stopped():
    settings = {
        "DISK_QUOTA_STOP_ON_ERROR": True,
        "DOWNLOADER_MIDDLEWARES": {
            "sh_scrapy.diskquota.DiskQuotaDownloaderMiddleware": 100,
            "tests.test_diskquota.RaiseValueErrorMiddleware": 200,
        },
    }

    class SimpleSpider(scrapy.Spider):
        name = "simple_spider"
        start_urls = ["data:,"]

        def parse(self, response):
            pass

    crawler = get_crawler(SimpleSpider, settings)
    await maybe_deferred_to_future(crawler.crawl())
    assert crawler.stats.get_value("finish_reason") == "finished"


@deferred_f_from_coro_f
async def test_downloaded_mware_process_stopped():
    settings = {
        "DISK_QUOTA_STOP_ON_ERROR": True,
        "DOWNLOADER_MIDDLEWARES": {
            "sh_scrapy.diskquota.DiskQuotaDownloaderMiddleware": 100,
            "tests.test_diskquota.RaiseDiskErrorMiddleware": 200,
        },
    }

    class SimpleSpider(scrapy.Spider):
        name = "simple_spider"
        start_urls = ["data:,"]

        def parse(self, response):
            pass

    crawler = get_crawler(SimpleSpider, settings)
    await maybe_deferred_to_future(crawler.crawl())
    assert crawler.stats.get_value("finish_reason") == "diskusage_exceeded"


@deferred_f_from_coro_f
async def test_spider_mware_process_not_stopped():
    settings = {
        "DISK_QUOTA_STOP_ON_ERROR": True,
        "SPIDER_MIDDLEWARES": {
            "sh_scrapy.diskquota.DiskQuotaSpiderMiddleware": 100,
        },
    }

    class ValueErrSpider(scrapy.Spider):
        name = "value_err_spider"
        start_urls = ["data:,"]

        def parse(self, response):
            raise ValueError("Simulated ValueError")

    crawler = get_crawler(ValueErrSpider, settings)
    await maybe_deferred_to_future(crawler.crawl())
    assert crawler.stats.get_value("finish_reason") == "finished"


@deferred_f_from_coro_f
async def test_spider_mware_process_stopped():
    """Test that disk quota error closes spider with correct reason."""
    settings = {
        "DISK_QUOTA_STOP_ON_ERROR": True,
        "SPIDER_MIDDLEWARES": {
            "sh_scrapy.diskquota.DiskQuotaSpiderMiddleware": 100,
        },
    }

    class ErrorSpider(scrapy.Spider):
        name = "error_spider"
        start_urls = ["data:,"]

        def parse(self, response):
            error = IOError()
            error.errno = 122
            raise error

    crawler = get_crawler(ErrorSpider, settings)
    await maybe_deferred_to_future(crawler.crawl())
    assert crawler.stats.get_value("finish_reason") == "diskusage_exceeded"
