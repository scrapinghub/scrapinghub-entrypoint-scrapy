"""
DiskQuota downloader and spider middlewares.
The goal is to catch disk quota errors and stop spider gently.
"""

import asyncio

from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response

from sh_scrapy import _SCRAPY_NO_SPIDER_ARG


class DiskQuota:

    def __init__(self, crawler: Crawler) -> None:
        if not crawler.settings.getbool("DISK_QUOTA_STOP_ON_ERROR"):
            raise NotConfigured
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "DiskQuota":
        return cls(crawler)

    def _is_disk_quota_error(self, error: Exception) -> bool:
        return isinstance(error, (OSError, IOError)) and error.errno == 122


class DiskQuotaDownloaderMiddleware(DiskQuota):

    if _SCRAPY_NO_SPIDER_ARG:

        async def process_exception(self, request: Request, exception: Exception) -> None:
            if self._is_disk_quota_error(exception):
                await self.crawler.engine.close_spider_async(reason="diskusage_exceeded")

    else:

        def process_exception(
            self, request: Request, exception: Exception, spider: Spider
        ) -> None:
            if self._is_disk_quota_error(exception):
                self.crawler.engine.close_spider(spider, "diskusage_exceeded")


class DiskQuotaSpiderMiddleware(DiskQuota):

    def __init__(self, crawler: Crawler):
        super().__init__(crawler)
        self._tasks: set[asyncio.Task] = set()

    if _SCRAPY_NO_SPIDER_ARG:

        def process_spider_exception(self, response: Response, exception: Exception) -> None:
            if self._is_disk_quota_error(exception):
                coro = self.crawler.engine.close_spider_async(reason="diskusage_exceeded")
                task = asyncio.create_task(coro)
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    else:

        def process_spider_exception(
            self, response: Response, exception: Exception, spider: Spider
        ) -> None:
            if self._is_disk_quota_error(exception):
                self.crawler.engine.close_spider(spider, "diskusage_exceeded")
