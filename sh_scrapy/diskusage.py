"""
DiskUsage extension.
"""
import os
import logging
from threading import Timer
from subprocess import Popen, PIPE

from twisted.internet import task

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.mail import MailSender
from scrapy.utils.engine import get_engine_status


logger = logging.getLogger(__name__)


def get_disk_usage(folders):
    """ Get disk usage for current user.

    :param folders: Folders to calculate disk usage for.
    :type folders: a list of str

    :return: ``(inodes_count, space_bytes)`` tuple
    """
    assert folders, 'There must be some folders to calculate disk usage'
    inodes = space = 0
    find_process = Popen(['find'] + folders + ['-user', str(os.getuid()),
                         '-type', 'f', '-printf', '%s\n'], stdout=PIPE)
    awk_process = Popen(['awk', "{i++;s+=$1}END{print i\" \"s}"],
                        stdin=find_process.stdout, stdout=PIPE)
    try:
        find_process.stdout.close()
        result = awk_process.communicate()[0]
        logger.debug('Find result: %s', result)
        if result and result.strip():
            inodes, space = [int(val) for val in result.strip().split(' ')]
    except Exception as exc:
        logger.warning("Find exception: %s", exc)
    return inodes, space


class DiskUsage(object):

    FOLDERS = ['/scrapy', '/tmp']

    def __init__(self, crawler):
        if not crawler.settings.getbool('DISKUSAGE_ENABLED'):
            raise NotConfigured

        self.crawler = crawler

        self.space_limit = crawler.settings.getint('DISKUSAGE_SPACE_LIMIT_MB')*1024*1024
        self.inodes_limit = crawler.settings.getint('DISKUSAGE_INODES_LIMIT')
        self.check_interval = crawler.settings.getfloat('DISKUSAGE_CHECK_INTERVAL_SECONDS')

        crawler.signals.connect(self.engine_started, signal=signals.engine_started)
        crawler.signals.connect(self.engine_stopped, signal=signals.engine_stopped)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def engine_started(self):
        inodes, space = get_disk_usage(self.FOLDERS)
        self.crawler.stats.set_value('diskusage/inodes/startup', inodes)
        self.crawler.stats.set_value('diskusage/space/startup', space)
        self.task = task.LoopingCall(self._task_handler)
        self.task.start(self.check_interval, now=True)

    def engine_stopped(self):
        if self.task.running:
            self.task.stop()

    def _task_handler(self):
        inodes, space = get_disk_usage(self.FOLDERS)
        self.crawler.stats.max_value('diskusage/inodes/max', inodes)
        self.crawler.stats.max_value('diskusage/space/max', space)
        msg = None
        if self.inodes_limit and inodes > self.inodes_limit:
            msg = 'inodes limit ({} > {})'.format(inodes, self.inodes_limit)
        elif self.space_limit and space > self.space_limit:
            msg = 'space limit ({}M > {}M)'.format(space, self.space_limit)
        if msg:
            self.crawler.stats.set_value('diskusage/limit_reached', 1)
            logger.error("Disk usage exceeded: %s. Shutting down Scrapy...",
                         msg, extra={'crawler': self.crawler})
            open_spiders = self.crawler.engine.open_spiders
            if open_spiders:
                for spider in open_spiders:
                    self.crawler.engine.close_spider(spider, 'diskusage_exceeded')
            else:
                self.crawler.stop()
