import sys
import pytest
from sh_scrapy.compat import to_bytes
from sh_scrapy.compat import to_native_str
from sh_scrapy.compat import to_unicode


# Testing to_unicode conversion

def test_to_unicode_an_utf8_encoded_string_to_unicode():
    assert to_unicode(b'lel\xc3\xb1e') == u'lel\xf1e'


def test_to_unicode_a_latin_1_encoded_string_to_unicode():
    assert to_unicode(b'lel\xf1e', 'latin-1') == u'lel\xf1e'


def test_to_unicode_a_unicode_to_unicode_should_return_the_same_object():
    assert to_unicode(u'\xf1e\xf1e\xf1e') == u'\xf1e\xf1e\xf1e'


def test_to_unicode_a_strange_object_should_raise_TypeError():
    with pytest.raises(TypeError) as excinfo:
        to_unicode(123)


def test_to_unicode_errors_argument():
    assert to_unicode(b'a\xedb', 'utf-8', errors='replace') == u'a\ufffdb'

# Testing to_unicode conversion

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
