=======
Changes
=======

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
