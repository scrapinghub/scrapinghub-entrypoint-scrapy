import mock
import pytest
from weakref import WeakKeyDictionary
from scrapy import signals
from scrapy.http import Request, Response
from scrapy.item import Item
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler
from scrapy.exceptions import NotConfigured
from scrapy.exporters import PythonItemExporter
from scrapy.utils.request import request_fingerprint

from sh_scrapy.extension import HubstorageExtension
from sh_scrapy.extension import HubstorageMiddleware


@mock.patch('sh_scrapy.hsref.hsref')
def test_hs_ext_fail_if_disabled_hsref(hsref):
    hsref.enabled = False
    crawler = get_crawler(Spider)
    with pytest.raises(NotConfigured):
        return HubstorageExtension.from_crawler(crawler)


@pytest.fixture
@mock.patch('sh_scrapy.hsref.hsref')
def hs_ext(hsref):
    hsref.enabled = True
    crawler = get_crawler(Spider)
    return HubstorageExtension.from_crawler(crawler)


def test_hs_ext_init(hs_ext):
    assert hs_ext.crawler
    assert hs_ext._write_item == hs_ext.hsref.job.items.write
    assert isinstance(hs_ext.exporter, PythonItemExporter)
    assert not hs_ext.exporter.binary


def test_hs_ext_item_scraped(hs_ext):
    hs_ext._write_item = mock.Mock()
    item = Item()
    spider = Spider('test')
    hs_ext.item_scraped(item, spider)
    assert hs_ext._write_item.call_count == 1
    assert hs_ext._write_item.call_args[0] == ({'_type': 'Item'},)


def test_hs_ext_spider_closed(hs_ext):
    spider = Spider('test')
    hs_ext.spider_closed(spider, 'killed')
    assert hs_ext.hsref.job.items.flush.called
    assert hs_ext.hsref.job.metadata.update.called
    assert hs_ext.hsref.job.metadata.update.call_args == (
        {'close_reason': 'killed'},)
    assert hs_ext.hsref.job.metadata.save.called


@pytest.fixture
@mock.patch('sh_scrapy.hsref.hsref')
def hs_mware(hsref):
    return HubstorageMiddleware()


def test_hs_mware_init(hs_mware):
    assert hs_mware._seen == {}
    assert hs_mware.hsref


def test_hs_mware_process_spider_input(hs_mware):
    response = Response('http://resp-url')
    response.request = Request('http://req-url')
    hs_mware.hsref.job.requests.add.return_value = 'riq'
    hs_mware.process_spider_input(response, Spider('test'))
    assert hs_mware.hsref.job.requests.add.call_count == 1
    args = hs_mware.hsref.job.requests.add.call_args[1]
    ts = args.pop('ts', None)
    assert isinstance(ts, float)
    assert args == {
        'duration': 0,
        'fp': request_fingerprint(response.request),
        'method': 'GET',
        'parent': None,
        'rs': 0,
        'status': 200,
        'url': 'http://resp-url'}
    assert hs_mware._seen == WeakKeyDictionary({response: 'riq'})


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
    assert result[0].meta.get('_hsparent') is None
    assert result[1].meta['_hsparent'] == 'riq'
