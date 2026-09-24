import mock
import pytest

from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from sh_scrapy import stats, _SCRAPY_NO_SPIDER_ARG


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


def test_collector_upload_stats_drops_oversized_value(collector):
    collector.set_stats({
        'item_scraped_count': 10,
        'huge': 'x' * stats._MAX_STATS_SIZE,
    })
    collector._upload_stats()
    uploaded = collector.pipe_writer.write_stats.call_args[0][0]
    assert uploaded == {'item_scraped_count': 10, 'cloud/dropped_stats': 1}
    assert stats._encoded_size(uploaded) <= stats._MAX_STATS_SIZE


def test_collector_upload_stats_drops_until_it_fits(collector):
    collector.set_stats({f'chunk/{i}': 'x' * 5000 for i in range(40)})
    collector._upload_stats()
    uploaded = collector.pipe_writer.write_stats.call_args[0][0]
    assert stats._encoded_size(uploaded) <= stats._MAX_STATS_SIZE
    assert uploaded['cloud/dropped_stats'] + len(uploaded) - 1 == 40


def test_collector_upload_stats_keeps_dropping_and_warns_once(collector, caplog):
    collector.set_stats({'huge': 'x' * stats._MAX_STATS_SIZE, 'kept': 1})
    collector._upload_stats()
    collector.set_stats({'huge': 'x', 'kept': 1, 'later': 2})
    collector._upload_stats()
    uploaded = collector.pipe_writer.write_stats.call_args[0][0]
    assert uploaded == {'kept': 1, 'later': 2, 'cloud/dropped_stats': 1}
    assert len([r for r in caplog.records if r.levelname == 'WARNING']) == 1


@mock.patch('twisted.internet.task.LoopingCall')
def test_collector_open_spider(lcall, collector):
    if _SCRAPY_NO_SPIDER_ARG:
        collector.open_spider()
    else:
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
    if _SCRAPY_NO_SPIDER_ARG:
        collector.close_spider(reason='reason')
    else:
        collector.close_spider('spider', 'reason')
    assert collector._samplestask.stop.called
    collector.pipe_writer.write_stats.assert_called_with(stats.copy())
