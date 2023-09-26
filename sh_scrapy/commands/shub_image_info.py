# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import subprocess
import sys

from scrapy.commands import ScrapyCommand


class Command(ScrapyCommand):
    requires_project = True
    default_settings = {'LOG_ENABLED': False}

    IMAGE_INFO_CMD = ' && '.join([
        "printf 'Linux packages:\n'", "dpkg -l",
        "printf '\nPython packages:\n'", "pip freeze",
    ])

    def short_desc(self):
        return "Print JSON-encoded project metadata."

    def add_options(self, parser):
        super(Command, self).add_options(parser)
        # backward compatibility for optparse/argparse
        try:
            add_argument = parser.add_argument
        except AttributeError:
            add_argument = parser.add_option
        add_argument(
            "--debug",
            action="store_true",
            help="add debugging information such as list of "
                 "installed Debian packages and Python packages.",
        )

    def run(self, args, opts):
        result = {
            'project_type': 'scrapy',
            'spiders': sorted(self.crawler_process.spider_loader.list()),
        }
        try:
            from scrapy_spider_metadata import get_metadata_for_spider
            result['metadata'] = {}
            for spider_name in result['spiders']:
                spider_cls = self.crawler_process.spider_loader.load(spider_name)
                metadata_dict = get_metadata_for_spider(spider_cls)
                try:
                    # make sure it's serializable
                    json.dumps(metadata_dict)
                except (TypeError, ValueError):
                    continue
                result['metadata'][spider_name] = metadata_dict
        except ImportError:
            pass
        if opts.debug:
            output = subprocess.check_output(
                ['bash', '-c', self.IMAGE_INFO_CMD],
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            result['debug'] = output
        print(json.dumps(result))
