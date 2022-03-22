import warnings

from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.utils.decorators import deprecated
from scrapy.utils.python import (
    to_bytes as scrapy_to_bytes,
    to_unicode as scrapy_to_unicode,
)


IS_PYTHON2 = False
STRING_TYPE = str
TEXT_TYPE = str
BINARY_TYPE = bytes


warnings.warn(
    "The sh_scrapy.compat module is deprecated, use the functions in scrapy.utils.python instead",
    category=ScrapyDeprecationWarning,
    stacklevel=2,
)


def is_string(var):
    warnings.warn(
        "is_string(var) is deprecated, please use isinstance(var, str) instead",
        category=ScrapyDeprecationWarning,
        stacklevel=2,
    )
    return isinstance(var, str)


@deprecated("scrapy.utils.python.to_bytes")
def to_bytes(text, encoding=None, errors='strict'):
    return scrapy_to_bytes(text, encoding, errors)


@deprecated("scrapy.utils.python.to_unicode")
def to_native_str(text, encoding=None, errors='strict'):
    return scrapy_to_unicode(text, encoding, errors)


@deprecated("scrapy.utils.python.to_unicode")
def to_unicode(text, encoding=None, errors='strict'):
    return scrapy_to_unicode(text, encoding, errors)
