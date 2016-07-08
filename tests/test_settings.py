
import os
import sys
import mock
import pytest
from scrapy.settings import Settings, SettingsAttribute
from sh_scrapy.settings import _update_settings
from sh_scrapy.settings import _load_autoscraping_settings
from sh_scrapy.settings import _maybe_load_autoscraping_project
from sh_scrapy.settings import _get_component_base
from sh_scrapy.settings import _load_addons
from sh_scrapy.settings import _populate_settings_base
from sh_scrapy.settings import _load_default_settings
from sh_scrapy.settings import populate_settings
from sh_scrapy.settings import REPLACE_ADDONS_PATHS

from sh_scrapy.compat import to_native_str


TEST_ADDON = {
    'addon_id': 'test_addon',
    'name': 'Fake test addon',
    'description': 'Some description',
    'settings': ('TEST_SETTING_A', 'TEST_SETTING_B'),
    'default_settings': {},
    'type': 'SPIDER_MIDDLEWARES',
    'order': 10,
    'path': 'some.addon.path',
    'builtin': False,
    'needs_aws': False,
}


def test_update_settings_void_dictionaries():
    test = {}
    _update_settings(test, {})
    assert test == {}


def test_update_settings_base_test():
    test = {}
    _update_settings(test, {'a': 'b'})
    assert test == {'a': 'b'}


def test_update_settings_base_test2():
    test = {}
    _update_settings(test, {'a': 'b', 'c': 'd'})
    assert test == {'a': 'b', 'c': 'd'}


def test_update_settings_dont_fail_on_non_string():
    test = {}
    _update_settings(test, {'a': 3})
    assert test == {'a': 3}


def test_update_settings_update_existing_value():
    test = {'a': 'b', 'c': 'd'}
    _update_settings(test, {'c': 'e', 'f': 'g'})
    assert test == {'a': 'b', 'c': 'e', 'f': 'g'}


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = {}
    _update_settings(test, {'\xf1e\xf1e\xf1e': 'test'})
    assert test == {'\xf1e\xf1e\xf1e': 'test',
                    to_native_str('\xf1e\xf1e\xf1e'): 'test'}


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key_value():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = {}
    _update_settings(test, {'\xf1e\xf1e\xf1e': '\xf1e\xf1e'})
    assert test == {
        '\xf1e\xf1e\xf1e': '\xf1e\xf1e',
        to_native_str('\xf1e\xf1e\xf1e'): to_native_str('\xf1e\xf1e')}


@pytest.mark.skipif(sys.version_info < (3,), reason="requires python3")
def test_update_settings_check_unicode_in_py3():
    test = {}
    _update_settings(test, {'\xf1e\xf1e\xf1e': 'test'})
    assert test == {'\xf1e\xf1e\xf1e': 'test'}


def test_load_autoscraping_settings_void_settings():
    settings = {}
    _load_autoscraping_settings({}, settings)
    assert settings == {
        'ITEM_PIPELINES': {'slybot.dupefilter.DupeFilterPipeline': 0},
        'SLYCLOSE_SPIDER_ENABLED': True,
        'SLYDUPEFILTER_ENABLED': True,
        'SPIDER_MANAGER_CLASS':
            'slybot.spidermanager.ZipfileSlybotSpiderManager'}


def test_load_autoscraping_settings_skip_existing():
    settings = {'SPIDER_MANAGER_CLASS': 'some.class',
                'SLYDUPEFILTER_ENABLED': False}
    _load_autoscraping_settings({}, settings)
    assert settings == {
        'ITEM_PIPELINES': {'slybot.dupefilter.DupeFilterPipeline': 0},
        'SLYCLOSE_SPIDER_ENABLED': True,
        'SLYDUPEFILTER_ENABLED': False,
        'SPIDER_MANAGER_CLASS': 'some.class'}


def test_maybe_load_autoscraping_project_no_spider_type_env():
    result = {}
    _maybe_load_autoscraping_project({}, result)
    assert result == {}


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'custom'})
def test_maybe_load_autoscraping_project_custom_type():
    result = {}
    _maybe_load_autoscraping_project({}, result)
    assert result == {}


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'auto'})
def test_maybe_load_autoscraping_project_ok():
    result = {'SPIDER_MANAGER_CLASS': 'test.class'}
    _maybe_load_autoscraping_project({}, result)
    assert result == {
        'ITEM_PIPELINES': {'slybot.dupefilter.DupeFilterPipeline': 0},
        'PROJECT_ZIPFILE': 'project-slybot.zip',
        'SLYCLOSE_SPIDER_ENABLED': True,
        'SLYDUPEFILTER_ENABLED': True,
        'SPIDER_MANAGER_CLASS': 'test.class'}


def test_get_component_base():
    assert _get_component_base({}, 'TEST') == 'TEST'
    assert _get_component_base({'SOME_SETTING': 'VAL'}, 'TEST') == 'TEST'
    assert _get_component_base({'TEST_BASE': 'VAL'}, 'TEST') == 'TEST_BASE'


def test_load_addons_void():
    addons = []
    settings = o = {}
    _load_addons(addons, settings, o)
    assert addons == []
    assert settings == o == {}


def test_load_addons_no_spider_mwares_setting():
    addons = [TEST_ADDON]
    settings = o = {}
    with pytest.raises(KeyError) as excinfo:
        _load_addons(addons, settings, o)
    assert 'SPIDER_MIDDLEWARES' in str(excinfo.value)


def test_load_addons_basic_usage():
    addons = [TEST_ADDON]
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = {}
    _load_addons(addons, settings, o)
    assert settings == o == {'SPIDER_MIDDLEWARES': {'some.addon.path': 10}}


def test_load_addons_basic_with_defaults():
    addons = [TEST_ADDON.copy()]
    addons[0]['default_settings'] = {'TEST_SETTING_A': 'TEST'}
    settings = {'SPIDER_MIDDLEWARES_BASE': {
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500}}
    o = {}
    _load_addons(addons, settings, o)
    assert settings == {'SPIDER_MIDDLEWARES_BASE': {
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500,
        'some.addon.path': 10}}
    expected_o = settings.copy()
    expected_o['TEST_SETTING_A'] = 'TEST'
    assert o == expected_o


def test_load_addons_hworker_import_ignore():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = {}
    _load_addons(addons, settings, o)
    assert settings == {'SPIDER_MIDDLEWARES': {}}
    assert o == {}


def test_load_addons_hworker_import_replace():
    for addon_path, replace_path in REPLACE_ADDONS_PATHS.items():
        addons = [TEST_ADDON.copy()]
        addons[0]['path'] = addon_path
        settings = {'SPIDER_MIDDLEWARES': {}}
        o = {}
        _load_addons(addons, settings, o)
        assert settings == o == {'SPIDER_MIDDLEWARES': {replace_path: 10}}


def test_populate_settings_dont_fail():
    result = _populate_settings_base({}, lambda x: x)
    assert isinstance(result, Settings)


def test_populate_settings_with_default():
    def default_test(s):
        s['TEST_SETTING_A'] = 'test'
    result = _populate_settings_base({}, default_test)
    assert result
    assert result.get('TEST_SETTING_A') == 'test'


def test_populate_settings_addons():
    addon = TEST_ADDON.copy()
    addon['default_settings'] = {'TEST_SETTING_A': 'by_addon'}
    msg = {'enabled_addons': [addon]}
    result = _populate_settings_base(msg, lambda x: x)
    assert result
    assert result.get('TEST_SETTING_A') == 'by_addon'


def test_populate_settings_override_settings():
    msg = {}
    for section in ['project_settings',
                    'organization_settings',
                    'job_settings']:
        msg[section] = {'TEST_SETTING_A': 'from_' + section}
        result = _populate_settings_base(msg, lambda x: x)
        assert result
        assert result.get('TEST_SETTING_A') == 'from_' + section


def test_populate_settings_with_spider():
    msg = {'project_settings': {'JOBDIR': 'by_project'},
           'spider_settings': {'TEST_SETTING_A': 'test'}}
    result = _populate_settings_base(msg, lambda x: x, spider=True)
    assert result
    assert result.get('TEST_SETTING_A', 'test')
    assert result.get('JOBDIR').split('/')[-1].startswith('jobdata-')
    assert not result.get('PROJECT_ZIPFILE')


def test_populate_settings_with_spider_override():
    msg = {'job_settings': {'JOBDIR': 'by_job'}}
    result = _populate_settings_base(msg, lambda x: x, spider=True)
    assert result
    assert result.get('JOBDIR') == 'by_job'


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'portia'})
def test_populate_settings_with_spider_autoscraping():
    result = _populate_settings_base({}, lambda x: x, spider=True)
    assert result
    assert result.get('PROJECT_ZIPFILE') == 'project-slybot.zip'


def test_load_default_settings():
    result = Settings({'EXTENSIONS_BASE': {
        'sh_scrapy.extension.HubstorageExtension': 50},
                       'SPIDER_MIDDLEWARES_BASE': {}})
    _load_default_settings(result)
    extensions = result['EXTENSIONS_BASE']
    assert extensions['scrapy.extensions.debug.StackTraceDump'] == 0
    assert extensions['sh_scrapy.extension.HubstorageExtension'] == 100
    assert 'slybot.closespider.SlybotCloseSpider' not in extensions
    mwares = result['SPIDER_MIDDLEWARES_BASE']
    assert 'sh_scrapy.extension.HubstorageMiddleware' in mwares
    assert result.get('SHUB_JOB_MEMORY_LIMIT', 950)


@mock.patch.dict(os.environ, {'SHUB_JOB_MEMORY_LIMIT': '200'})
def test_load_default_settings_mem_limit():
    result = Settings({'EXTENSIONS_BASE': {},
                       'SPIDER_MIDDLEWARES_BASE': {}})
    _load_default_settings(result)
    assert result.get('SHUB_JOB_MEMORY_LIMIT', 200)


def test_populate_settings_dont_fail():
    result = populate_settings({})
    assert isinstance(result, Settings)
    # check one of the settings provided by default by sh_scrapy
    assert result.get('TELNETCONSOLE_HOST') == '0.0.0.0'


def test_populate_settings_dont_fail_with_spider():
    result = populate_settings({}, True)
    assert isinstance(result, Settings)
    # check one of the settings provided by default by sh_scrapy
    assert result.get('TELNETCONSOLE_HOST') == '0.0.0.0'
