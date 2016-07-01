import mock
import pytest

from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler

from sh_scrapy import stats


@pytest.fixture
@mock.patch('sh_scrapy.hsref.hsref')
def collector(hsref):
    crawler = get_crawler(Spider)
    return stats.HubStorageStatsCollector(crawler)


def test_collector_class_vars(collector):
    assert collector.STATS_TO_COLLECT == [
        'item_scraped_count',
        'response_received_count',
        'scheduler/enqueued',
        'scheduler/dequeued',
        'log_count/ERROR',
    ]
    assert collector.INTERVAL == 30


def test_collector_upload_stats(collector):
    collector.set_stats({'item_scraped_count': 10, 'scheduler/enqueued': 20})
    collector._upload_stats()
    assert collector.hsref.job.samples.write.call_count == 1
    samples_args = collector.hsref.job.samples.write.call_args[0][0]
    assert isinstance(samples_args[0], int)
    assert samples_args[1:] == [10, 0, 20, 0, 0]
    collector.hsref.job.metadata.apipost.assert_called_with(
        jl={'scrapystats': {'item_scraped_count': 10,
                            'scheduler/enqueued': 20}})


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
    collector.set_stats({'item_scraped_count': 10})
    collector.close_spider('spider', 'reason')
    assert collector._samplestask.stop.called
    collector.hsref.job.metadata.apipost.assert_called_with(
        jl={'scrapystats': {'item_scraped_count': 10}})
