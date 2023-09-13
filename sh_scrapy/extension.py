import logging
from contextlib import suppress
from weakref import WeakKeyDictionary

import scrapy
from scrapy import signals
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.exporters import PythonItemExporter
from scrapy.http import Request
from scrapy.utils.deprecate import create_deprecated_class
from scrapy.utils.request import request_fingerprint

from sh_scrapy import hsref
from sh_scrapy.crawl import ignore_warnings
from sh_scrapy.exceptions import SHScrapyDeprecationWarning
from sh_scrapy.middlewares import HS_PARENT_ID_KEY, request_id_sequence
from sh_scrapy.writer import pipe_writer


try:
    from itemadapter import ItemAdapter
except ImportError:
    _base_item_cls = [dict, scrapy.item.Item]
    with suppress(AttributeError):
        _base_item_cls.append(scrapy.item.BaseItem)
    _base_item_cls = tuple(_base_item_cls)

    def is_item(item):
        return isinstance(item, _base_item_cls)
else:
    def is_item(item):
        return ItemAdapter.is_item(item)


class HubstorageExtension(object):
    """Extension to write scraped items to HubStorage"""

    def __init__(self, crawler):
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer
        self.crawler = crawler
        self.logger = logging.getLogger(__name__)
        self._write_item = self.pipe_writer.write_item
        self.exporter = PythonItemExporter()

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler)
        crawler.signals.connect(o.item_scraped, signals.item_scraped)
        crawler.signals.connect(o.spider_closed, signals.spider_closed)
        return o

    def item_scraped(self, item, spider):
        if not is_item(item):
            self.logger.error("Wrong item type: %s" % item)
            return
        type_ = type(item).__name__
        item = self.exporter.export_item(item)
        item.setdefault("_type", type_)
        self._write_item(item)

    def spider_closed(self, spider, reason):
        self.pipe_writer.set_outcome(reason)


_HUBSTORAGE_MIDDLEWARE_WARNING = """\
{cls} inherits from deprecated class {old}

sh_scrapy.extension.HubstorageMiddleware functionality is now split between two new middlewares:

- sh_scrapy.middlewares.HubstorageDownloaderMiddleware
- sh_scrapy.middlewares.HubstorageSpiderMiddleware

Please migrate to new middlewares.
"""


class HubstorageMiddleware(object):

    def __init__(self):
        self._seen = WeakKeyDictionary()
        self.hsref = hsref.hsref
        self.pipe_writer = pipe_writer
        self.request_id_sequence = request_id_sequence

    def process_spider_input(self, response, spider):
        self.pipe_writer.write_request(
            url=response.url,
            status=response.status,
            method=response.request.method,
            rs=len(response.body),
            duration=response.meta.get('download_latency', 0) * 1000,
            parent=response.meta.get(HS_PARENT_ID_KEY),
            fp=request_fingerprint(response.request),
        )
        self._seen[response] = next(self.request_id_sequence)

    def process_spider_output(self, response, result, spider):
        parent = self._seen.pop(response)
        for x in result:
            if isinstance(x, Request):
                x.meta[HS_PARENT_ID_KEY] = parent
            yield x


HubstorageMiddleware = create_deprecated_class(
    "HubstorageMiddleware",
    HubstorageMiddleware,
    warn_category=SHScrapyDeprecationWarning,
    subclass_warn_message=_HUBSTORAGE_MIDDLEWARE_WARNING
)
