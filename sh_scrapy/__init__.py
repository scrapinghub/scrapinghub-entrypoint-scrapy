__version__ = "0.18.1"


from scrapy import version_info as scrapy_version_info


# Flag to check if Scrapy version requires spider argument in some core components.
# Also indicates whether or not async versions of some methods are supported.
_SCRAPY_NO_SPIDER_ARG = scrapy_version_info >= (2, 14, 0)
