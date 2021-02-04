from os import environ
from mock import patch

from pytest import raises
from scrapy.settings import Settings

from sh_scrapy.utils import get_project_settings


def test_get_project_settings_class():
    settings = get_project_settings()
    assert isinstance(settings, Settings)


def test_get_project_settings_default():
    settings = get_project_settings()
    assert settings['TELNETCONSOLE_HOST'] == '0.0.0.0'


@patch.dict(
    environ,
    {
        'SHUB_SETTINGS': '{"project_settings": {"SETTING_TEST": "VAL"}}',
    }
)
def test_get_project_settings_setting():
    settings = get_project_settings()
    assert settings['SETTING_TEST'] == 'VAL'


@patch.dict(
    environ,
    {
        'SHUB_SETTINGS': '{"project_settings": {"SETTING....',
    }
)
def test_get_project_settings_bad_setting():
    with raises(ValueError):
        get_project_settings()
