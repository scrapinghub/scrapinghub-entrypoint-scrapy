# -*- coding: utf-8 -*-
from weakref import WeakKeyDictionary
import itertools
import pytest
import sys
from scrapy import Spider, Request, Item
from scrapy.http import Response
from typing import Optional

from sh_scrapy.middlewares import (
    HubstorageSpiderMiddleware, HubstorageDownloaderMiddleware,
    HS_REQUEST_ID_KEY, HS_PARENT_ID_KEY
)


@pytest.fixture()
def monkeypatch_globals(monkeypatch):
    monkeypatch.setattr('sh_scrapy.middlewares.request_id_sequence', itertools.count())
    monkeypatch.setattr('sh_scrapy.middlewares.seen_requests', WeakKeyDictionary())


@pytest.fixture()
def hs_spider_middleware(monkeypatch_globals):
    return HubstorageSpiderMiddleware()


@pytest.fixture()
def hs_downloader_middleware(monkeypatch_globals):
    return HubstorageDownloaderMiddleware()


def test_hs_middlewares(hs_downloader_middleware, hs_spider_middleware):
    assert hs_spider_middleware._seen_requests == WeakKeyDictionary()
    assert hs_downloader_middleware._seen_requests == WeakKeyDictionary()
    assert hs_spider_middleware._seen_requests is hs_downloader_middleware._seen_requests

    spider = Spider('test')
    url = 'http://resp-url'
    request_0 = Request(url)
    response_0 = Response(url)

    hs_downloader_middleware.process_request(request_0, spider)

    assert HS_REQUEST_ID_KEY not in request_0.meta
    assert HS_PARENT_ID_KEY not in request_0.meta
    assert len(hs_spider_middleware._seen_requests) == 0
    assert len(hs_downloader_middleware._seen_requests) == 0

    hs_downloader_middleware.process_response(request_0, response_0, spider)

    assert request_0.meta[HS_REQUEST_ID_KEY] == 0
    assert request_0.meta[HS_PARENT_ID_KEY] is None
    assert hs_spider_middleware._seen_requests[request_0] == 0

    response_0.request = request_0
    request_1 = Request(url)
    request_2 = Request(url)
    item1 = {}
    item2 = Item()
    output = [request_1, request_2, item1, item2]
    processed_output = list(hs_spider_middleware.process_spider_output(response_0, output, spider))

    assert processed_output[0] is request_1
    assert request_1.meta[HS_PARENT_ID_KEY] == 0
    assert processed_output[1] is request_2
    assert request_2.meta[HS_PARENT_ID_KEY] == 0
    assert processed_output[2] is item1
    assert processed_output[3] is item2

    response_1 = Response(url)
    hs_downloader_middleware.process_request(request_1, spider)
    hs_downloader_middleware.process_response(request_1, response_1, spider)
    assert request_1.meta[HS_REQUEST_ID_KEY] == 1
    assert request_1.meta[HS_PARENT_ID_KEY] == 0

    response_2 = Response(url)
    hs_downloader_middleware.process_request(request_2, spider)
    hs_downloader_middleware.process_response(request_2, response_2, spider)
    assert request_2.meta[HS_REQUEST_ID_KEY] == 2
    assert request_2.meta[HS_PARENT_ID_KEY] == 0


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7")
def test_hs_middlewares_dummy_response(hs_downloader_middleware, hs_spider_middleware):
    from dataclasses import dataclass

    @dataclass(unsafe_hash=True)
    class DummyResponse(Response):
        __module__: str = "scrapy_poet.api"

        def __init__(self, url: str, request: Optional[Request] = None):
            super().__init__(url=url, request=request)

    spider = Spider('test')
    url = 'http://resp-url'

    # cleaning log file
    hs_downloader_middleware.pipe_writer.open()

    request = Request(url)
    response_1 = DummyResponse(url, request)
    response_2 = Response(url)
    hs_downloader_middleware.process_request(request, spider)
    hs_downloader_middleware.process_response(request, response_1, spider)

    with open(hs_downloader_middleware.pipe_writer.path, 'r') as tmp_file:
        assert tmp_file.readline() == ""
    assert request.meta == {}

    hs_downloader_middleware.process_response(request, response_2, spider)
    with open(hs_downloader_middleware.pipe_writer.path, 'r') as tmp_file:
        assert tmp_file.readline().startswith('REQ')

    assert request.meta[HS_REQUEST_ID_KEY] == 0
    assert request.meta[HS_PARENT_ID_KEY] is None


@pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7")
def test_hs_middlewares_retry(hs_downloader_middleware, hs_spider_middleware):
    from dataclasses import dataclass

    @dataclass(unsafe_hash=True)
    class DummyResponse(Response):
        __module__: str = "scrapy_poet.api"

        def __init__(self, url: str, request: Optional[Request] = None):
            super().__init__(url=url, request=request)

    spider = Spider('test')
    url = 'http://resp-url'
    request_0 = Request(url)
    response_0 = Response(url)

    hs_downloader_middleware.process_request(request_0, spider)

    assert HS_REQUEST_ID_KEY not in request_0.meta
    assert HS_PARENT_ID_KEY not in request_0.meta
    assert len(hs_spider_middleware._seen_requests) == 0
    assert len(hs_downloader_middleware._seen_requests) == 0

    hs_downloader_middleware.process_response(request_0, response_0, spider)

    assert request_0.meta[HS_REQUEST_ID_KEY] == 0
    assert request_0.meta[HS_PARENT_ID_KEY] is None
    assert hs_spider_middleware._seen_requests[request_0] == 0

    request_1 = request_0.copy()
    response_1 = Response(url)
    assert request_1.meta[HS_REQUEST_ID_KEY] == 0
    assert request_1.meta[HS_PARENT_ID_KEY] is None

    hs_downloader_middleware.process_request(request_1, spider)

    assert HS_REQUEST_ID_KEY not in request_1.meta
    assert request_1.meta[HS_PARENT_ID_KEY] == 0

    hs_downloader_middleware.process_response(request_1, response_1, spider)

    assert request_1.meta[HS_REQUEST_ID_KEY] == 1
    assert request_1.meta[HS_PARENT_ID_KEY] == 0

    request_2 = request_1.copy()
    response_2_1 = DummyResponse(url, request_2)
    response_2_2 = Response(url)

    hs_downloader_middleware.process_response(request_2, response_2_1, spider)

    assert request_2.meta[HS_REQUEST_ID_KEY] == 1
    assert request_2.meta[HS_PARENT_ID_KEY] == 0

    hs_downloader_middleware.process_response(request_2, response_2_2, spider)

    assert request_2.meta[HS_REQUEST_ID_KEY] == 2
    assert request_2.meta[HS_PARENT_ID_KEY] == 0
