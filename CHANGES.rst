=======
Changes
=======

0.18.1 (unreleased)
===================

-   Fixed ``DiskQuotaDownloaderMiddleware`` and ``DiskQuotaSpiderMiddleware``
    raising ``RuntimeError: no running event loop`` when not using an asyncio
    Twisted reactor.

0.18.0 (2026-01-15)
===================

-   Added support for Scrapy 2.14.

0.17.7 (2025-07-18)
===================

-   Support scripts installed with ``pip install``.

0.17.6 (2025-07-08)
===================

-   Made Python 3.13 support official, and dropped support for Python 3.6 and
    3.7.

-   ``HubstorageDownloaderMiddleware`` is now an `universal spider middleware
    <https://docs.scrapy.org/en/latest/topics/coroutines.html#universal-spider-middleware>`__.

0.17.5 (2024-11-28)
===================

-   Added support for Scrapy 2.12.

-   Not accepting a ``crawler`` in the ``__init__`` method of
    ``HubstorageDownloaderMiddleware`` is now deprecated.

0.17.4 (2024-07-08)
===================

-   Fixed an exception when running scripts with importlib_ installed,
    introduced in 0.17.3.

0.17.3 (2024-06-17)
===================

-   Replaced a use of the deprecated pkg_resources_ module with importlib_.

    .. _pkg_resources: https://setuptools.pypa.io/en/latest/pkg_resources.html
    .. _importlib: https://docs.python.org/3/library/importlib.html

0.17.2 (2024-02-20)
===================

-   Added official support for Python 3.11 and 3.12.

-   Added support for centralized request fingerprints on Scrapy 2.7 and
    higher.

-   Started this change log. Check `GitHub releases`_ for older releases until
    0.12.0, and the `commit history`_ for the complete history.

    .. _commit history: https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/commits/master/
    .. _GitHub releases: https://github.com/scrapinghub/scrapinghub-entrypoint-scrapy/releases
