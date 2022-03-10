
# scrapinghub-entrypoint-scrapy

[![version](https://img.shields.io/pypi/v/scrapinghub-entrypoint-scrapy.svg)](https://pypi.python.org/pypi/scrapinghub-entrypoint-scrapy)
[![pyversions](https://img.shields.io/pypi/pyversions/scrapinghub-entrypoint-scrapy.svg)](https://pypi.python.org/pypi/scrapinghub-entrypoint-scrapy)
[![actions](https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/workflows/Tests/badge.svg)](https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/actions)
[![codecov](https://codecov.io/gh/scrapinghub/scrapinghub-entrypoint-scrapy/branch/master/graph/badge.svg)](https://codecov.io/gh/scrapinghub/scrapinghub-entrypoint-scrapy)

Scrapy entrypoint for Scrapinghub job runner.

The package implements a base wrapper layer to extract job data from
environment, parse/prepare it properly and execute job using Scrapy
or custom executor.


## Features

- parsing job data from environment
- processing job args and settings
- running a job with Scrapy
- collecting stats
- advanced logging & error handling
- full hubstorage support
- custom scripts support


## Install

    pip install scrapinghub-entrypoint-scrapy
