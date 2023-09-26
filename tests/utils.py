import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple, Optional, Union


def call_command(cwd: Union[str, os.PathLike], *args: str) -> Tuple[str, str]:
    result = subprocess.run(
        args,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout, result.stderr


def call_scrapy_command(cwd: Union[str, os.PathLike], *args: str) -> Tuple[str, str]:
    args = (sys.executable, "-m", "scrapy.cmdline") + args
    return call_command(cwd, *args)


def create_project(topdir: Path, spider_text: Optional[str] = None) -> Path:
    project_name = "foo"
    cwd = topdir
    call_scrapy_command(str(cwd), "startproject", project_name)
    cwd /= project_name
    (cwd / project_name / "spiders" / "spider.py").write_text(spider_text or """
from scrapy import Spider

class MySpider(Spider):
    name = "myspider"
""")
    return cwd
