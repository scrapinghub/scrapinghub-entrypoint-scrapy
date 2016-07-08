import time
from weakref import WeakKeyDictionary
from scrapy import signals, log
from scrapy.exceptions import NotConfigured
from scrapy.exporters import PythonItemExporter
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint
from sh_scrapy import hsref


class HubstorageExtension(object):
    """Extension to write scraped items to HubStorage"""

    def __init__(self, crawler):
        self.hsref = hsref.hsref
        if not self.hsref.enabled:
            raise NotConfigured

        self.crawler = crawler
        self._write_item = self.hsref.job.items.write
        self.exporter = PythonItemExporter(binary=False)
        log.msg("HubStorage: writing items to %s" % self.hsref.job.items.url)

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler)
        crawler.signals.connect(o.item_scraped, signals.item_scraped)
        crawler.signals.connect(o.spider_closed, signals.spider_closed)
        return o

    def item_scraped(self, item, spider):
        type_ = type(item).__name__
        item = self.exporter.export_item(item)
        item.setdefault("_type", type_)
        self._write_item(item)

    def spider_closed(self, spider, reason):
        # flush item writer
        self.hsref.job.items.flush()
        # update outcome
        self.hsref.job.metadata.update(close_reason=reason)
        self.hsref.job.metadata.save()


class HubstorageMiddleware(object):

    def __init__(self):
        self._seen = WeakKeyDictionary()
        self.hsref = hsref.hsref

    def process_spider_input(self, response, spider):
        parent = response.meta.get('_hsparent')
        riq = self.hsref.job.requests.add(
            parent=parent,
            url=response.url,
            status=response.status,
            method=response.request.method,
            rs=len(response.body),
            duration=response.meta.get('download_latency', 0) * 1000,
            ts=time.time() * 1000,
            fp=request_fingerprint(response.request),
        )
        self._seen[response] = riq

    def process_spider_output(self, response, result, spider):
        parent = self._seen.pop(response)
        for x in result:
            if isinstance(x, Request):
                x.meta['_hsparent'] = parent
            yield x
