import json
import logging

from scrapinghub.hubstorage.serialization import jsondefault
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.statscollectors import StatsCollector
from twisted.internet import task

from sh_scrapy import hsref, _SCRAPY_NO_SPIDER_ARG
from sh_scrapy.writer import pipe_writer

logger = logging.getLogger(__name__)

_MAX_STATS_SIZE = 65536

_DROPPED_STATS = "cloud/dropped_stats"


def _encoded_size(stats: dict) -> int:
    encoded = json.dumps(stats, separators=(",", ":"), default=jsondefault)
    # Scrapy Cloud escapes forward slashes, which are common in stat names.
    return len(encoded) + encoded.count("/")


class HubStorageStatsCollector(StatsCollector):

    INTERVAL = 30

    def __init__(self, crawler: Crawler):
        super(HubStorageStatsCollector, self).__init__(crawler)
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer
        self._dropped_stats = set()

    def _fit_stats(self) -> dict:
        """Return the stats to upload, without as many of the largest ones as
        needed to fit into the maximum encoded size that Scrapy Cloud accepts.

        Scrapy Cloud stores all stats as a single value, and rejects that value
        as a whole when it is too large, so one oversized stat costs every stat
        of the job.

        Stats rarely shrink, so a dropped stat stays dropped even if it would
        fit again.
        """
        if not self._dropped_stats and _encoded_size(self._stats) <= _MAX_STATS_SIZE:
            return self._stats
        was_dropping = bool(self._dropped_stats)
        fitted = {
            key: value
            for key, value in self._stats.items()
            if key not in self._dropped_stats
        }
        largest_first = sorted(
            fitted, key=lambda key: _encoded_size({key: fitted[key]}), reverse=True
        )
        fitted[_DROPPED_STATS] = len(self._dropped_stats)
        for key in largest_first:
            if _encoded_size(fitted) <= _MAX_STATS_SIZE:
                break
            del fitted[key]
            self._dropped_stats.add(key)
            fitted[_DROPPED_STATS] = len(self._dropped_stats)
        if not was_dropping:
            logger.warning(
                f"Some stats exceed the maximum size that Scrapy Cloud accepts "
                f"and are being dropped. The {_DROPPED_STATS} stat counts them."
            )
        return fitted

    def _upload_stats(self) -> None:
        self.pipe_writer.write_stats(self._fit_stats())

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
