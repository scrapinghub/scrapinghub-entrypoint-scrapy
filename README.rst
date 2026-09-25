=============================
scrapinghub-entrypoint-scrapy
=============================

.. image:: https://img.shields.io/pypi/v/scrapinghub-entrypoint-scrapy.svg
   :target: https://pypi.python.org/pypi/scrapinghub-entrypoint-scrapy
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/scrapinghub-entrypoint-scrapy.svg
   :target: https://pypi.python.org/pypi/scrapinghub-entrypoint-scrapy
   :alt: Supported Python Versions

.. image:: https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/workflows/Tests/badge.svg
   :target: https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/actions
   :alt: Build Status

.. image:: https://codecov.io/gh/scrapinghub/scrapinghub-entrypoint-scrapy/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/scrapinghub/scrapinghub-entrypoint-scrapy
   :alt: Coverage report

Scrapy entrypoint for Scrapinghub job runner.

The package implements a base wrapper layer to extract job data from
environment, parse/prepare it properly and execute job using Scrapy
or custom executor.

Features
========

-   parsing job data from environment
-   processing job args and settings
-   running a job with Scrapy
-   collecting stats
-   advanced logging & error handling
-   full hubstorage support
-   custom scripts support

Install
=======

.. code-block:: shell

    pip install scrapinghub-entrypoint-scrapy
