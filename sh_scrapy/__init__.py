__version__ = "0.18.0"


from scrapy import version_info as scrapy_version_info


_SCRAPY_NO_SPIDER_ARG = scrapy_version_info >= (2, 14, 0)
