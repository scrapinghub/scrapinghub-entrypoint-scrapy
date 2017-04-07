"""
DiskQuota downloader and spider middlewares.
The goal is to catch disk quota errors and stop spider gently.
"""

from scrapy.exceptions import NotConfigured


class DiskQuota(object):

    def __init__(self, crawler):
        if not crawler.settings.getbool('DISK_QUOTA_STOP_ON_ERROR'):
            raise NotConfigured
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def _is_disk_quota_error(self, error):
        return isinstance(error, (OSError, IOError)) and error.errno == 122


class DiskQuotaDownloaderMiddleware(DiskQuota):

    def process_exception(self, request, exception, spider):
        if self._is_disk_quota_error(exception):
            self.crawler.engine.close_spider(spider, 'diskusage_exceeded')


class DiskQuotaSpiderMiddleware(DiskQuota):

    def process_spider_exception(self, response, exception, spider):
        if self._is_disk_quota_error(exception):
            self.crawler.engine.close_spider(spider, 'diskusage_exceeded')
