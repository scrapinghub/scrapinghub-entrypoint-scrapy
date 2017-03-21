from twisted.internet import task
from scrapy.statscol import StatsCollector

from sh_scrapy import hsref
from sh_scrapy.writer import pipe_writer


class HubStorageStatsCollector(StatsCollector):

    INTERVAL = 30

    def __init__(self, crawler):
        super(HubStorageStatsCollector, self).__init__(crawler)
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer

    def _upload_stats(self):
        self.pipe_writer.write_stats(self._stats)

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
