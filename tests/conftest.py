import os
import shutil
import tempfile
from pathlib import Path

import pytest

TEMP_DIR = Path(tempfile.mkdtemp())
SHUB_FIFO_PATH = str(TEMP_DIR / "scrapinghub")
os.environ["SHUB_FIFO_PATH"] = SHUB_FIFO_PATH

from sh_scrapy.writer import pipe_writer  # should go after setting SHUB_FIFO_PATH

TEST_AUTH = "312f322f333a61757468737472"  # 1/2/3:authstr


@pytest.fixture(scope="session", autouse=True)
def clean_shub_fifo_path():
    pipe_writer.open()
    try:
        yield
    finally:
        shutil.rmtree(TEMP_DIR)


@pytest.fixture(autouse=True)
def set_jobkeyenvironment(monkeypatch):
    monkeypatch.setenv("SHUB_JOBKEY", "1/2/3")
    monkeypatch.setenv("SCRAPY_JOB", "1/2/3")
    monkeypatch.setenv("SHUB_JOBAUTH", TEST_AUTH)
    monkeypatch.setenv("SHUB_STORAGE", "storage-url")
