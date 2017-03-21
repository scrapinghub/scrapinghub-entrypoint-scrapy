import mock
import pytest

from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from sh_scrapy import stats


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr('sh_scrapy.stats.pipe_writer', mock.Mock())
    crawler = get_crawler(Spider)
    return stats.HubStorageStatsCollector(crawler)


def test_collector_class_vars(collector):
    assert collector.INTERVAL == 30


def test_collector_upload_stats(collector):
    stats = {'item_scraped_count': 10, 'scheduler/enqueued': 20}
    collector.set_stats(stats.copy())
    collector._upload_stats()
    assert collector.pipe_writer.write_stats.call_count == 1
    collector.pipe_writer.write_stats.assert_called_with(stats.copy())


@mock.patch('twisted.internet.task.LoopingCall')
def test_collector_open_spider(lcall, collector):
    collector.open_spider('spider')
    lcall.assert_called_with(collector._upload_stats)
    lcall.return_value.start.assert_called_with(collector.INTERVAL, now=True)
    dcall = lcall.return_value.start.return_value
    dcall.addErrback.assert_called_with(
        collector._setup_looping_call, now=False)


def test_collector_close_spider(collector):
    collector._samplestask = mock.Mock()
    collector._samplestask.running = True
    stats = {'item_scraped_count': 10}
    collector.set_stats(stats.copy())
    collector.close_spider('spider', 'reason')
    assert collector._samplestask.stop.called
    collector.pipe_writer.write_stats.assert_called_with(stats.copy())
