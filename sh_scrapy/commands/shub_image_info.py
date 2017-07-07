# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import subprocess

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
        parser.add_option("--debug", action="store_true",
                          help="add debugging information such as "
                               "list of installed Debian packages "
                               "and Python packages.")

    def run(self, args, opts):
        result = {
            'project_type': 'scrapy',
            'spiders': sorted(self.crawler_process.spider_loader.list())
        }
        if opts.debug:
            output = subprocess.check_output(
                ['bash', '-c', self.IMAGE_INFO_CMD],
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            result['debug'] = output
        print(json.dumps(result))
