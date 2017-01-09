import itertools
import logging
from weakref import WeakKeyDictionary

from scrapy.item import BaseItem
from scrapy import signals, log
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.exporters import PythonItemExporter
from scrapy.http import Request
from scrapy.utils.request import request_fingerprint

from sh_scrapy import hsref
from sh_scrapy.compat import IS_PYTHON2
from sh_scrapy.crawl import ignore_warnings
from sh_scrapy.writer import pipe_writer


class HubstorageExtension(object):
    """Extension to write scraped items to HubStorage"""

    def __init__(self, crawler):
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer
        self.crawler = crawler
        self._write_item = self.pipe_writer.write_item
        # https://github.com/scrapy/scrapy/commit/c76190d491fca9f35b6758bdc06c34d77f5d9be9
        exporter_kwargs = {'binary': False} if not IS_PYTHON2 else {}
        with ignore_warnings(category=ScrapyDeprecationWarning):
            self.exporter = PythonItemExporter(**exporter_kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler)
        crawler.signals.connect(o.item_scraped, signals.item_scraped)
        crawler.signals.connect(o.spider_closed, signals.spider_closed)
        return o

    def item_scraped(self, item, spider):
        if not isinstance(item, (dict, BaseItem)):
            log.msg("Wrong item type: %s" % item, level=logging.ERROR)
            return
        type_ = type(item).__name__
        item = self.exporter.export_item(item)
        item.setdefault("_type", type_)
        self._write_item(item)

    def spider_closed(self, spider, reason):
        self.pipe_writer.set_outcome(reason)


class HubstorageMiddleware(object):

    def __init__(self):
        self._seen = WeakKeyDictionary()
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer
        self.request_id_sequence = itertools.count()

    def process_spider_input(self, response, spider):
        self.pipe_writer.write_request(
            url=response.url,
            status=response.status,
            method=response.request.method,
            rs=len(response.body),
            duration=response.meta.get('download_latency', 0) * 1000,
            parent=response.meta.get('_hsparent'),
            fp=request_fingerprint(response.request),
        )
        self._seen[response] = next(self.request_id_sequence)

    def process_spider_output(self, response, result, spider):
        parent = self._seen.pop(response)
        for x in result:
            if isinstance(x, Request):
                x.meta['_hsparent'] = parent
            yield x
