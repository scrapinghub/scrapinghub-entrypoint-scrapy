# -*- coding: utf-8 -*-
import itertools
from weakref import WeakKeyDictionary

from scrapy import Request
from scrapy.utils.request import request_fingerprint

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


class HubstorageDownloaderMiddleware(object):
    """Hubstorage dowloader middleware.
    
    What it does:
    
    - Generates request ids for all downloaded requests.
    - Sets parent request ids for requests generated in downloader middlewares.
    - Stores all downloaded requests into Hubstorage.
    
    """

    def __init__(self):
        self._seen_requests = seen_requests
        self.pipe_writer = pipe_writer
        self.request_id_sequence = request_id_sequence

    def process_request(self, request, spider):
        # Check if request id is set, which usually happens for retries or redirects because
        # those requests are usually copied from the original one.
        request_id = request.meta.pop(HS_REQUEST_ID_KEY, None)
        if request_id is not None:
            # Set original request id or None as a parent request id.
            request.meta[HS_PARENT_ID_KEY] = request_id

    def process_response(self, request, response, spider):
        self.pipe_writer.write_request(
            url=response.url,
            status=response.status,
            method=request.method,
            rs=len(response.body),
            duration=request.meta.get('download_latency', 0) * 1000,
            parent=request.meta.setdefault(HS_PARENT_ID_KEY),
            fp=request_fingerprint(request),
        )
        # Generate and set request id.
        request_id = next(self.request_id_sequence)
        self._seen_requests[request] = request_id
        request.meta[HS_REQUEST_ID_KEY] = request_id
        return response
