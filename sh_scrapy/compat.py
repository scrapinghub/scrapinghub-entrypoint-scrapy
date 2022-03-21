import warnings

from scrapy.exceptions import ScrapyDeprecationWarning


def to_str(text, encoding=None, errors='strict'):
    """Return the unicode representation of `text`.

    If `text` is already a ``str`` object, return it as-is.
    If `text` is a ``bytes`` object, decode it using `encoding`.

    Otherwise, raise an error.
    """
    if isinstance(text, str):
        return text
    if not isinstance(text, bytes):
        raise TypeError('to_str must receive a bytes or str '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.decode(encoding, errors)


def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of `text`.

    If `text` is already a ``bytes`` object, return it as-is.
    If `text` is a ``unicode`` object, encode it using `encoding`.

    Otherwise, raise an error.
    """
    if isinstance(text, bytes):
        return text
    if not isinstance(text, str):
        raise TypeError('to_bytes must receive a str or bytes '
                        'object, got %s' % type(text).__name__)
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)


def is_string(var):
    warnings.warn(
        f"{_qualname(is_string)} is deprecated, please use isinstance(<var>, str) instead",
        category=ScrapyDeprecationWarning,
        stacklevel=2,
    )
    return isinstance(var, str)


def to_native_str(text, encoding=None, errors='strict'):
    warnings.warn(
        f"{_qualname(to_native_str)} is deprecated, please use {_qualname(to_str)} instead",
        category=ScrapyDeprecationWarning,
        stacklevel=2,
    )
    return to_str(text, encoding, errors)


def to_unicode(text, encoding=None, errors='strict'):
    warnings.warn(
        f"{_qualname(to_unicode)} is deprecated, please use {_qualname(to_str)} instead",
        category=ScrapyDeprecationWarning,
        stacklevel=2,
    )
    return to_str(text, encoding, errors)


def _qualname(obj) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"
