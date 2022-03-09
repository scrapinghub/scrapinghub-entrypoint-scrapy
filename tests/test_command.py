
from argparse import ArgumentParser
from optparse import OptionParser

import pytest
import scrapy
from packaging import version

from sh_scrapy.commands.shub_image_info import Command


@pytest.fixture
def command():
    command = Command()
    command.settings = scrapy.settings.Settings()
    return command


@pytest.mark.skipif(
    version.parse(scrapy.__version__) >= version.parse("2.6"),
    reason="Scrapy>=2.6 uses argparse"
)
def test_optparse(command):
    parser = OptionParser()
    command.add_options(parser)
    options = parser.parse_args(["--debug"])
    assert options[0].debug


@pytest.mark.skipif(
    version.parse(scrapy.__version__) < version.parse("2.6"),
    reason="Scrapy<2.6 uses optparse"
)
def test_argparse(command):
    parser = ArgumentParser()
    command.add_options(parser)
    options = parser.parse_args(["--debug"])
    assert options.debug
