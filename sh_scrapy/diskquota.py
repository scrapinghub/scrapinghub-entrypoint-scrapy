"""
DiskQuota downloader and spider middlewares.
The goal is to catch disk quota errors and stop spider gently.
"""

from __future__ import annotations

import asyncio

from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response
from scrapy.utils.defer import deferred_from_coro

from sh_scrapy import _SCRAPY_NO_SPIDER_ARG


class DiskQuota:
    def __init__(self, crawler: Crawler):
        if not crawler.settings.getbool("DISK_QUOTA_STOP_ON_ERROR"):
            raise NotConfigured
        self.crawler = crawler
        self._tasks: set[asyncio.Task] = set()

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> DiskQuota:
        return cls(crawler)

    def _is_disk_quota_error(self, error: Exception) -> bool:
        return isinstance(error, (OSError, IOError)) and error.errno == 122

    def _handle_exception(self, exception: Exception) -> None:
        if not self._is_disk_quota_error(exception):
            return
        if hasattr(self.crawler.engine, "close_spider_async"):
            from scrapy.utils.asyncio import is_asyncio_available

            coro = self.crawler.engine.close_spider_async(reason="diskusage_exceeded")
            if is_asyncio_available():
                task = asyncio.create_task(coro)
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            else:
                deferred_from_coro(coro)
        else:
            self.crawler.engine.close_spider(self.crawler.spider, "diskusage_exceeded")


class DiskQuotaDownloaderMiddleware(DiskQuota):
    if _SCRAPY_NO_SPIDER_ARG:

        async def process_exception(
            self, request: Request, exception: Exception
        ) -> None:
            self._handle_exception(exception)

    else:

        def process_exception(
            self, request: Request, exception: Exception, spider: Spider
        ) -> None:
            self._handle_exception(exception)


class DiskQuotaSpiderMiddleware(DiskQuota):
    if _SCRAPY_NO_SPIDER_ARG:

        def process_spider_exception(
            self, response: Response, exception: Exception
        ) -> None:
            self._handle_exception(exception)

    else:

        def process_spider_exception(
            self, response: Response, exception: Exception, spider: Spider
        ) -> None:
            self._handle_exception(exception)
