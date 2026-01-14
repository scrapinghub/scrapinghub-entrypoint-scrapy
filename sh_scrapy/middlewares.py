# -*- coding: utf-8 -*-
import itertools
from typing import AsyncIterable, AsyncGenerator, Iterable
from warnings import warn
from weakref import WeakKeyDictionary

from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.http import Request, Response

from sh_scrapy import _SCRAPY_NO_SPIDER_ARG
from sh_scrapy.writer import pipe_writer


HS_REQUEST_ID_KEY = '_hsid'
HS_PARENT_ID_KEY = '_hsparent'
request_id_sequence = itertools.count()
seen_requests = WeakKeyDictionary()


class HubstorageSpiderMiddleware:
    """Hubstorage spider middleware.

    What it does:

    - Sets parent request ids to the requests coming out of the spider.

    """

    def __init__(self) -> None:
        self._seen_requests = seen_requests

    if _SCRAPY_NO_SPIDER_ARG:

        def process_spider_output(self, response: Response, result: Iterable) -> Iterable:
            return self._process_spider_output(response, result)

        async def process_spider_output_async(
            self, response: Response, result: Iterable
        ) -> AsyncGenerator:
            async for x in self._process_spider_output_async(response, result):
                yield x

    else:

        def process_spider_output(
            self, response: Response, result: Iterable, spider: Spider
        ) -> Iterable:
            return self._process_spider_output(response, result)

        async def process_spider_output_async(
            self, response: Response, result: Iterable, spider: Spider
        ) -> AsyncGenerator:
            async for x in self._process_spider_output_async(response, result):
                yield x

    def _process_spider_output(self, response: Response, result: Iterable) -> Iterable:
        parent = self._seen_requests.pop(response.request, None)
        for x in result:
            if isinstance(x, Request):
                self._process_request(x, parent)
            yield x

    async def _process_spider_output_async(
        self, response: Response, result: AsyncIterable
    ) -> AsyncGenerator:
        parent = self._seen_requests.pop(response.request, None)
        async for x in result:
            if isinstance(x, Request):
                self._process_request(x, parent)
            yield x

    def _process_request(self, request: Request, parent: int | None) -> None:
        request.meta[HS_PARENT_ID_KEY] = parent
        # Remove request id if it was for some reason set in the request coming from Spider.
        request.meta.pop(HS_REQUEST_ID_KEY, None)


class HubstorageDownloaderMiddleware:
    """Hubstorage dowloader middleware.

    What it does:

    - Generates request ids for all downloaded requests.
    - Sets parent request ids for requests generated in downloader middlewares.
    - Stores all downloaded requests into Hubstorage.

    """

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "HubstorageDownloaderMiddleware":
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

    def __init__(self, crawler: Crawler):
        self._crawler = crawler
        self._seen_requests = seen_requests
        self.pipe_writer = pipe_writer
        self.request_id_sequence = request_id_sequence
        self._load_fingerprinter()

    def _load_fingerprinter(self) -> None:
        if hasattr(self._crawler, "request_fingerprinter"):
            self._fingerprint = lambda request: self._crawler.request_fingerprinter.fingerprint(request).hex()
        else:
            from scrapy.utils.request import request_fingerprint
            self._fingerprint = request_fingerprint

    if _SCRAPY_NO_SPIDER_ARG:

        def process_request(self, request: Request) -> None:
            return self._process_request(request)

        def process_response(self, request: Request, response: Response) -> Response:
            return self._process_response(request, response)

    else:

        def process_request(self, request: Request, spider: Spider) -> None:
            return self._process_request(request)

        def process_response(self, request: Request, response: Response, spider: Spider) -> Response:
            return self._process_response(request, response)

    def _process_request(self, request: Request) -> None:
        # Check if request id is set, which usually happens for retries or redirects because
        # those requests are usually copied from the original one.
        request_id = request.meta.pop(HS_REQUEST_ID_KEY, None)
        if request_id is not None:
            # Set original request id or None as a parent request id.
            request.meta[HS_PARENT_ID_KEY] = request_id

    def _process_response(self, request: Request, response: Response) -> Response:
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
