import sys
from weakref import WeakKeyDictionary

import mock
import pytest
import scrapy
from packaging import version
from pytest import warns
from scrapy import Spider, version_info as SCRAPY_VERSION_INFO
from scrapy.exporters import PythonItemExporter
from scrapy.http import Request, Response
from scrapy.item import Item
from scrapy.utils.test import get_crawler

from sh_scrapy.extension import HubstorageExtension, HubstorageMiddleware
from sh_scrapy.middlewares import HS_PARENT_ID_KEY


@pytest.fixture
def hs_ext(monkeypatch):
    monkeypatch.setattr('sh_scrapy.extension.pipe_writer', mock.Mock())
    monkeypatch.setattr('sh_scrapy.extension.hsref', mock.Mock())
    crawler = get_crawler(Spider)
    return HubstorageExtension.from_crawler(crawler)


def test_hs_ext_init(hs_ext):
    assert hs_ext.crawler
    assert hs_ext._write_item == hs_ext.pipe_writer.write_item
    assert isinstance(hs_ext.exporter, PythonItemExporter)
    assert hs_ext.exporter.export_item({"a": "b"}) == {"a": "b"}


@pytest.mark.skipif(SCRAPY_VERSION_INFO < (2, 2, 0), reason="Requires Scrapy>=2.2")
def test_hs_ext_dataclass_item_scraped(hs_ext):
    from dataclasses import dataclass

    @dataclass
    class DataclassItem:
        pass

    hs_ext._write_item = mock.Mock()
    item = DataclassItem()
    spider = Spider('test')
    hs_ext.item_scraped(item, spider)
    assert hs_ext._write_item.call_count == 1
    assert hs_ext._write_item.call_args[0] == ({'_type': 'DataclassItem'},)


@pytest.mark.skipif(SCRAPY_VERSION_INFO < (2, 2, 0), reason="Requires Scrapy>=2.2")
def test_hs_ext_attrs_item_scraped(hs_ext):
    try:
        import attr
        import iteamadapter
    except ImportError:
        pytest.skip('attrs not installed')
        return

    @attr.s
    class AttrsItem(object):
        pass

    hs_ext._write_item = mock.Mock()
    item = AttrsItem()
    spider = Spider('test')
    hs_ext.item_scraped(item, spider)
    assert hs_ext._write_item.call_count == 1
    assert hs_ext._write_item.call_args[0] == ({'_type': 'AttrsItem'},)


def test_hs_ext_item_scraped(hs_ext):
    hs_ext._write_item = mock.Mock()
    item = Item()
    spider = Spider('test')
    hs_ext.item_scraped(item, spider)
    assert hs_ext._write_item.call_count == 1
    assert hs_ext._write_item.call_args[0] == ({'_type': 'Item'},)


def test_hs_ext_item_scraped_skip_wrong_type(hs_ext):
    hs_ext._write_item = mock.Mock()
    spider = Spider('test')
    for item in [None, [], 123]:
        hs_ext.item_scraped(item, spider)
        assert hs_ext._write_item.call_count == 0


def test_hs_ext_spider_closed(hs_ext):
    spider = Spider('test')
    hs_ext.spider_closed(spider, 'killed')
    assert hs_ext.pipe_writer.set_outcome.called
    assert hs_ext.pipe_writer.set_outcome.call_args == mock.call('killed')


@pytest.fixture
def hs_mware(monkeypatch):
    monkeypatch.setattr('sh_scrapy.extension.pipe_writer', mock.Mock())
    crawler = get_crawler()
    return HubstorageMiddleware.from_crawler(crawler)


def test_hs_mware_init(hs_mware):
    assert hs_mware._seen == {}
    assert hs_mware.hsref


def test_hs_mware_process_spider_input(hs_mware):
    response = Response('http://resp-url')
    response.request = Request('http://req-url')
    hs_mware.process_spider_input(response, Spider('test'))
    assert hs_mware.pipe_writer.write_request.call_count == 1
    args = hs_mware.pipe_writer.write_request.call_args[1]
    if hasattr(hs_mware._crawler, "request_fingerprinter"):
        fp = "1c735665b072000e11b0169081bce5bbaeac09a7"
    else:
        fp = "a001a1eb4537acdc8525edf1250065cab2657152"
    assert args == {
        'duration': 0,
        'fp': fp,
        'method': 'GET',
        'parent': None,
        'rs': 0,
        'status': 200,
        'url': 'http://resp-url'
    }
    assert hs_mware._seen == WeakKeyDictionary({response: 0})


def test_hs_mware_process_spider_output_void_result(hs_mware):
    response = Response('http://resp-url')
    hs_mware._seen = WeakKeyDictionary({response: 'riq'})
    assert list(hs_mware.process_spider_output(
        response, [], Spider('test'))) == []


def test_hs_mware_process_spider_output_filter_request(hs_mware):
    response = Response('http://resp-url')
    # provide a response and a new request in result
    child_response = Response('http://resp-url-child')
    child_response.request = Request('http://resp-url-child-req')
    child_request = Request('http://req-url-child')
    hs_mware._seen = WeakKeyDictionary({response: 'riq'})
    result = list(hs_mware.process_spider_output(
        response, [child_response, child_request], Spider('test')))
    assert len(result) == 2
    # make sure that we update hsparent meta only for requests
    assert result[0].meta.get(HS_PARENT_ID_KEY) is None
    assert result[1].meta[HS_PARENT_ID_KEY] == 'riq'


@pytest.mark.skipif(
    version.parse(scrapy.__version__) < version.parse("2.7"),
    reason="Only Scrapy 2.7 and higher support centralized request fingerprints."
)
def test_custom_fingerprinter(monkeypatch):
    monkeypatch.setattr('sh_scrapy.extension.pipe_writer', mock.Mock())

    class CustomFingerprinter:
        def fingerprint(self, request):
            return b"foo"

    crawler = get_crawler(settings_dict={"REQUEST_FINGERPRINTER_CLASS": CustomFingerprinter})
    mw = HubstorageMiddleware.from_crawler(crawler)

    response = Response('http://resp-url')
    response.request = Request('http://req-url')
    mw.process_spider_input(response, Spider('test'))
    assert mw.pipe_writer.write_request.call_args[1]["fp"] == b"foo".hex()


def test_subclassing():
    class CustomHubstorageMiddleware(HubstorageMiddleware):
        def __init__(self):
            super().__init__()
            self.foo = "bar"

    crawler = get_crawler()
    with warns(
        DeprecationWarning,
        match="must now accept a crawler parameter in their __init__ method",
    ):
        mw = CustomHubstorageMiddleware.from_crawler(crawler)

    assert mw.foo == "bar"
    assert hasattr(mw, "_fingerprint")
