import warnings

import pytest
from scrapy.exceptions import ScrapyDeprecationWarning

from sh_scrapy.compat import is_string, to_bytes, to_unicode, to_native_str


# test deprecation messages

def test_deprecated_is_string():
    with warnings.catch_warnings(record=True) as caught:
        assert is_string("foo")
        assert not is_string(b"foo")
        assert not is_string(1)
        assert (
            "is_string(var) is deprecated, please use isinstance(var, str) instead"
            == str(caught[0].message)
        )
        assert caught[0].category is ScrapyDeprecationWarning


def test_deprecated_to_unicode():
    with warnings.catch_warnings(record=True) as caught:
        assert to_unicode("foo") == "foo"
        assert to_unicode(b"foo") == "foo"
        assert (
            "Call to deprecated function to_unicode. Use scrapy.utils.python.to_unicode instead."
            == str(caught[0].message)
        )
        assert caught[0].category is ScrapyDeprecationWarning


def test_deprecated_to_native_str():
    with warnings.catch_warnings(record=True) as caught:
        assert to_native_str("foo") == "foo"
        assert to_native_str(b"foo") == "foo"
        assert (
            "Call to deprecated function to_native_str. Use scrapy.utils.python.to_unicode instead."
            == str(caught[0].message)
        )
        assert caught[0].category is ScrapyDeprecationWarning


def test_deprecated_to_bytes():
    with warnings.catch_warnings(record=True) as caught:
        assert to_bytes("foo") == b"foo"
        assert to_bytes(b"foo") == b"foo"
        assert (
            "Call to deprecated function to_bytes. Use scrapy.utils.python.to_bytes instead."
            == str(caught[0].message)
        )
        assert caught[0].category is ScrapyDeprecationWarning


# Testing to_unicode conversion

def test_to_str_an_utf8_encoded_string_to_str():
    assert to_unicode(b'lel\xc3\xb1e') == u'lel\xf1e'


def test_to_str_a_latin_1_encoded_string_to_str():
    assert to_unicode(b'lel\xf1e', 'latin-1') == u'lel\xf1e'


def test_to_str_a_unicode_to_str_should_return_the_same_object():
    assert to_unicode(u'\xf1e\xf1e\xf1e') == u'\xf1e\xf1e\xf1e'


def test_to_str_a_strange_object_should_raise_TypeError():
    with pytest.raises(TypeError) as excinfo:
        to_unicode(123)


def test_to_str_errors_argument():
    assert to_unicode(b'a\xedb', 'utf-8', errors='replace') == u'a\ufffdb'


# Testing to_bytes conversion

def test_to_bytes_a_unicode_object_to_an_utf_8_encoded_string():
    assert to_bytes(u'\xa3 49') == b'\xc2\xa3 49'


def test_to_bytes_a_unicode_object_to_a_latin_1_encoded_string():
    assert to_bytes(u'\xa3 49', 'latin-1') == b'\xa3 49'


def test_to_bytes_a_regular_bytes_to_bytes_should_return_the_same_object():
    assert to_bytes(b'lel\xf1e') == b'lel\xf1e'


def test_to_bytes_a_strange_object_should_raise_TypeError():
    with pytest.raises(TypeError):
        to_bytes(pytest)


def test_to_bytes_errors_argument():
    assert to_bytes(u'a\ufffdb', 'latin-1', errors='replace') == b'a?b'
