import os
import subprocess
import sys
from pathlib import Path


def call_command(cwd: str | os.PathLike, *args: str) -> tuple[str, str]:
    result = subprocess.run(
        args,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout, result.stderr


def call_scrapy_command(cwd: str | os.PathLike, *args: str) -> tuple[str, str]:
    args = (sys.executable, "-m", "scrapy.cmdline", *args)
    return call_command(cwd, *args)


def create_project(topdir: Path, spider_text: str | None = None) -> Path:
    project_name = "foo"
    cwd = topdir
    call_scrapy_command(str(cwd), "startproject", project_name)
    cwd /= project_name
    (cwd / project_name / "spiders" / "spider.py").write_text(
        spider_text
        or """
from scrapy import Spider

class MySpider(Spider):
    name = "myspider"
"""
    )
    return cwd
