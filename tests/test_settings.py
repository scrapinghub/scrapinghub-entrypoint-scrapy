
import os
import sys
import mock
import pytest
from scrapy.settings import BaseSettings, Settings, SettingsAttribute
from sh_scrapy.settings import BaseSettingsWithNativeStrings
from sh_scrapy.settings import _maybe_load_autoscraping_project
from sh_scrapy.settings import _get_component_base
from sh_scrapy.settings import _get_action_on_missing_addons
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
    'path': 'scrapy.utils.misc.load_object',
    'builtin': False,
    'needs_aws': False,
}


def test_update_settings_void_dictionaries():
    test = BaseSettingsWithNativeStrings()
    test.update( {}, 10)
    assert test.copy_to_dict() == {}


def test_update_settings_base_test():
    test = BaseSettingsWithNativeStrings()
    test.update({'a': 'b'}, 10)
    assert test == {'a': 'b'}


def test_update_settings_base_test2():
    test = BaseSettingsWithNativeStrings()
    test.update({'a': 'b', 'c': 'd'}, 10)
    assert test == {'a': 'b', 'c': 'd'}


def test_update_settings_dont_fail_on_non_string():
    test = BaseSettingsWithNativeStrings()
    test.update({'a': 3}, 10)
    assert test == {'a': 3}


def test_update_settings_update_existing_value():
    test = BaseSettingsWithNativeStrings({'a': 'b', 'c': 'd'}, priority=10)
    test.update({'c': 'e', 'f': 'g'}, 10)
    assert test.copy_to_dict() == {'a': 'b', 'c': 'e', 'f': 'g'}


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = BaseSettingsWithNativeStrings({})
    test.update({'\xf1e\xf1e\xf1e': 'test'}, 10)
    assert test == {'\xf1e\xf1e\xf1e': 'test',
                    to_native_str('\xf1e\xf1e\xf1e'): 'test'}


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key_value():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = BaseSettingsWithNativeStrings({})
    test.update({'\xf1e\xf1e\xf1e': '\xf1e\xf1e'}, 10)
    assert test == {
        '\xf1e\xf1e\xf1e': '\xf1e\xf1e',
        to_native_str('\xf1e\xf1e\xf1e'): to_native_str('\xf1e\xf1e')}


@pytest.mark.skipif(sys.version_info < (3,), reason="requires python3")
def test_update_settings_check_unicode_in_py3():
    test = BaseSettingsWithNativeStrings({})
    test.update({'\xf1e\xf1e\xf1e': 'test'}, 10)
    assert test == {'\xf1e\xf1e\xf1e': 'test'}


def test_maybe_load_autoscraping_project_no_spider_type_env():
    result = {}
    _maybe_load_autoscraping_project(result)
    assert result == {}


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'custom'})
def test_maybe_load_autoscraping_project_custom_type():
    result = {}
    _maybe_load_autoscraping_project(result)
    assert result == {}


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'auto'})
def test_maybe_load_autoscraping_project_ok():
    result = BaseSettingsWithNativeStrings(
        {'SPIDER_MANAGER_CLASS': 'test.class'})
    _maybe_load_autoscraping_project(result)
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


def test_get_action_on_missing_addons_default():
    o = BaseSettings()
    assert _get_action_on_missing_addons(o) == 'warn'


def test_get_action_on_missing_addons_base():
    o = BaseSettings({'ON_MISSING_ADDONS': 'fail'})
    assert _get_action_on_missing_addons(o) == 'fail'


def test_get_action_on_missing_addons_warn_if_wrong_value():
    o = BaseSettings({'ON_MISSING_ADDONS': 'wrong'})
    assert _get_action_on_missing_addons(o) == 'warn'


def test_load_addons_void():
    addons = []
    settings, o = BaseSettings(), BaseSettingsWithNativeStrings()
    _load_addons(addons, settings, o)
    assert addons == []
    assert settings == o == {}


def test_load_addons_basic_usage():
    addons = [TEST_ADDON]
    settings = BaseSettings({'SPIDER_MIDDLEWARES': {}})
    o = BaseSettingsWithNativeStrings()
    _load_addons(addons, settings, o)
    assert settings.copy_to_dict() == {'SPIDER_MIDDLEWARES': {
            TEST_ADDON['path']: 10}}
    assert o.copy_to_dict() == {
        'SPIDER_MIDDLEWARES': {TEST_ADDON['path']: 10}}


def test_load_addons_basic_with_defaults():
    addons = [TEST_ADDON.copy()]
    addons[0]['default_settings'] = {'TEST_SETTING_A': 'TEST'}
    settings = {'SPIDER_MIDDLEWARES_BASE': {
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500}}
    o = BaseSettingsWithNativeStrings({'ON_MISSING_ADDONS': 'warn'})
    _load_addons(addons, settings, o)
    assert settings == {'SPIDER_MIDDLEWARES_BASE': {
        TEST_ADDON['path']: 10,
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500
    }}
    expected_o = settings.copy()
    expected_o['TEST_SETTING_A'] = 'TEST'
    expected_o['ON_MISSING_ADDONS'] = 'warn'
    assert o.copy_to_dict() == expected_o


def test_load_addons_hworker_fail_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = BaseSettings({'SPIDER_MIDDLEWARES': {}})
    o = BaseSettingsWithNativeStrings({'ON_MISSING_ADDONS': 'fail'})
    with pytest.raises(ImportError):
        _load_addons(addons, settings, o)


def test_load_addons_hworker_error_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = BaseSettingsWithNativeStrings({'ON_MISSING_ADDONS': 'error'})
    _load_addons(addons, settings, o)
    assert o.copy_to_dict() == {'ON_MISSING_ADDONS': 'error'}
    assert settings == {'SPIDER_MIDDLEWARES': {}}


def test_load_addons_hworker_warning_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = BaseSettingsWithNativeStrings({'ON_MISSING_ADDONS': 'warn'})
    _load_addons(addons, settings, o)
    assert o.copy_to_dict() == {'ON_MISSING_ADDONS': 'warn'}
    assert settings == {'SPIDER_MIDDLEWARES': {}}


@mock.patch.dict('sh_scrapy.settings.REPLACE_ADDONS_PATHS',
                 {TEST_ADDON['path']: 'scrapy.utils.misc.arg_to_iter'})
def test_load_addons_hworker_import_replace():
    addons = [TEST_ADDON]
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = BaseSettingsWithNativeStrings()
    _load_addons(addons, settings, o)
    assert o.copy_to_dict() == {'SPIDER_MIDDLEWARES': {
        'scrapy.utils.misc.arg_to_iter': 10}}


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
