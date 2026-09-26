
from argparse import ArgumentParser

import pytest
import scrapy

from sh_scrapy.commands.shub_image_info import Command


@pytest.fixture
def command():
    command = Command()
    command.settings = scrapy.settings.Settings()
    return command


def test_argparse(command):
    parser = ArgumentParser()
    command.add_options(parser)
    options = parser.parse_args(["--debug"])
    assert options.debug
