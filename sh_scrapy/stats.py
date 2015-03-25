import time
from twisted.internet import task
from scrapy.statscol import StatsCollector
from .hsref import hsref


class HubStorageStatsCollector(StatsCollector):

    # TODO: make this configurable per project
    STATS_TO_COLLECT = [
        'item_scraped_count',
        'response_received_count',
        'scheduler/enqueued',
        'scheduler/dequeued',
        'log_count/ERROR',
    ]
    INTERVAL = 30

    def _upload_stats(self):
        row = [int(time.time() * 1000)]
        row.extend(self._stats.get(x, 0) for x in self.STATS_TO_COLLECT)
        hsref.job.samples.write(row)
        hsref.job.metadata.apipost(jl={'scrapystats': self._stats})

    def open_spider(self, spider):
        self._setup_looping_call(now=True)

    def _setup_looping_call(self, _ignored=None, **kwargs):
        self._samplestask = task.LoopingCall(self._upload_stats)
        d = self._samplestask.start(self.INTERVAL, **kwargs)
        d.addErrback(self._setup_looping_call, now=False)

    def close_spider(self, spider, reason):
        super(HubStorageStatsCollector, self).close_spider(spider, reason)
        if self._samplestask.running:
            self._samplestask.stop()
        self._upload_stats()
