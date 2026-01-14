from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.statscollectors import StatsCollector
from twisted.internet import task

from sh_scrapy import hsref, _SCRAPY_NO_SPIDER_ARG
from sh_scrapy.writer import pipe_writer


class HubStorageStatsCollector(StatsCollector):

    INTERVAL = 30

    def __init__(self, crawler: Crawler) -> None:
        super(HubStorageStatsCollector, self).__init__(crawler)
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer

    def _upload_stats(self) -> None:
        self.pipe_writer.write_stats(self._stats)

    def _setup_looping_call(self, _ignored=None, **kwargs) -> None:
        self._samplestask = task.LoopingCall(self._upload_stats)
        d = self._samplestask.start(self.INTERVAL, **kwargs)
        d.addErrback(self._setup_looping_call, now=False)

    def _close_spider(self, spider: Spider | None = None, reason: str | None = None) -> None:
        super().close_spider(spider=spider, reason=reason)
        if self._samplestask.running:
            self._samplestask.stop()
        self._upload_stats()

    if _SCRAPY_NO_SPIDER_ARG:

        def open_spider(self) -> None:
            self._setup_looping_call(now=True)

        def close_spider(self, reason: str | None = None) -> None:
            self._close_spider(reason=reason)

    else:

        def open_spider(self, spider: Spider | None = None) -> None:
            self._setup_looping_call(now=True)

        def close_spider(
            self, spider: Spider | None = None, reason: str | None = None
        ) -> None:
            self._close_spider(spider=spider, reason=reason)
