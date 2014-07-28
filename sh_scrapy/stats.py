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

    def open_spider(self, spider):
        self._samplestask = task.LoopingCall(self._upload_stats)
        self._samplestask.start(self.INTERVAL, now=True)

    def close_spider(self, spider, reason):
        super(HubStorageStatsCollector, self).close_spider(spider, reason)
        if self._samplestask.running:
            self._samplestask.stop()
        self._upload_stats()

    def _persist_stats(self, stats, spider):
        hsref.job.metadata['scrapystats'] = stats
        hsref.job.metadata.save()
