# -*- coding: utf-8 -*-
import codecs
import os
import shutil
import tempfile

import pytest
from scrapy.utils.python import to_unicode, to_bytes

TEMP_DIR = tempfile.mkdtemp()
SHUB_FIFO_PATH = os.path.join(TEMP_DIR, 'scrapinghub')
os.environ['SHUB_FIFO_PATH'] = SHUB_FIFO_PATH

from sh_scrapy.writer import pipe_writer  # should go after setting SHUB_FIFO_PATH


TEST_AUTH = to_unicode(codecs.encode(to_bytes('1/2/3:authstr'), 'hex_codec'))


@pytest.fixture(scope='session', autouse=True)
def clean_shub_fifo_path():
    global TEMP_DIR
    pipe_writer.open()
    try:
        yield
    finally:
        shutil.rmtree(TEMP_DIR)


@pytest.fixture(autouse=True)
def set_jobkeyenvironment(monkeypatch):
    monkeypatch.setenv('SHUB_JOBKEY', '1/2/3')
    monkeypatch.setenv('SCRAPY_JOB', '1/2/3')
    monkeypatch.setenv('SHUB_JOBAUTH', TEST_AUTH)
    monkeypatch.setenv('SHUB_STORAGE', 'storage-url')
