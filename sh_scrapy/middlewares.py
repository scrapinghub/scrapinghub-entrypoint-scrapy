# -*- coding: utf-8 -*-
import itertools
from weakref import WeakKeyDictionary

from scrapy import Request

from sh_scrapy.writer import pipe_writer

HS_REQUEST_ID_KEY = '_hsid'
HS_PARENT_ID_KEY = '_hsparent'
request_id_sequence = itertools.count()
seen_requests = WeakKeyDictionary()


class HubstorageSpiderMiddleware(object):
    """Hubstorage spider middleware.
    
    What it does:
    
    - Sets parent request ids to the requests coming out of the spider.
    
    """

    def __init__(self):
        self._seen_requests = seen_requests

    def process_spider_output(self, response, result, spider):
        parent = self._seen_requests.pop(response.request, None)
        for x in result:
            if isinstance(x, Request):
                x.meta[HS_PARENT_ID_KEY] = parent
                # Remove request id if it was for some reason set in the request coming from Spider.
                x.meta.pop(HS_REQUEST_ID_KEY, None)
            yield x


class HubstorageDownloaderMiddleware:
    """Hubstorage dowloader middleware.
    
    What it does:
    
    - Generates request ids for all downloaded requests.
    - Sets parent request ids for requests generated in downloader middlewares.
    - Stores all downloaded requests into Hubstorage.
    
    """

    @classmethod
    def from_crawler(cls, crawler):
        try:
            result = cls(crawler)
        except TypeError:
            warn(
                (
                    "Subclasses of HubstorageDownloaderMiddleware must now "
                    "accept a crawler parameter in their __init__ method. "
                    "This will become an error in the future."
                ),
                DeprecationWarning,
            )
            result = cls()
            result._crawler = crawler
            result._load_fingerprinter()
        return result

    def __init__(self, crawler):
        self._crawler = crawler
        self._seen_requests = seen_requests
        self.pipe_writer = pipe_writer
        self.request_id_sequence = request_id_sequence
        self._load_fingerprinter()

    def _load_fingerprinter(self):
        if hasattr(self._crawler, "request_fingerprinter"):
            self._fingerprint = lambda request: self._crawler.request_fingerprinter.fingerprint(request).hex()
        else:
            from scrapy.utils.request import request_fingerprint
            self._fingerprint = request_fingerprint

    def process_request(self, request, spider):
        # Check if request id is set, which usually happens for retries or redirects because
        # those requests are usually copied from the original one.
        request_id = request.meta.pop(HS_REQUEST_ID_KEY, None)
        if request_id is not None:
            # Set original request id or None as a parent request id.
            request.meta[HS_PARENT_ID_KEY] = request_id

    def process_response(self, request, response, spider):
        # This class of response check is intended to fix the bug described here
        # https://github.com/scrapy-plugins/scrapy-zyte-api/issues/112
        if type(response).__name__ == "DummyResponse" and type(response).__module__.startswith("scrapy_poet"):
            return response

        self.pipe_writer.write_request(
            url=response.url,
            status=response.status,
            method=request.method,
            rs=len(response.body),
            duration=request.meta.get('download_latency', 0) * 1000,
            parent=request.meta.setdefault(HS_PARENT_ID_KEY),
            fp=self._fingerprint(request),
        )
        # Generate and set request id.
        request_id = next(self.request_id_sequence)
        self._seen_requests[request] = request_id
        request.meta[HS_REQUEST_ID_KEY] = request_id
        return response
