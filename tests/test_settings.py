
import os
import sys
import mock
import pytest
from scrapy import version_info as scrapy_version
from scrapy.settings import Settings
from sh_scrapy.settings import EntrypointSettings
from sh_scrapy.settings import _maybe_load_autoscraping_project
from sh_scrapy.settings import _get_component_base
from sh_scrapy.settings import _get_action_on_missing_addons
from sh_scrapy.settings import _load_addons
from sh_scrapy.settings import _populate_settings_base
from sh_scrapy.settings import _load_default_settings
from sh_scrapy.settings import _update_old_classpaths
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
    test = EntrypointSettings()
    test.setdict({}, 10)
    assert len(test.attributes) == 0


def test_update_settings_base_test():
    test = EntrypointSettings()
    test.setdict({'a': 'b'}, 10)
    assert test['a'] == 'b'


def test_update_settings_base_test2():
    test = EntrypointSettings()
    test.setdict({'a': 'b', 'c': 'd'}, 10)
    assert len(test.attributes) == 2


def test_update_settings_dont_fail_on_non_string():
    test = EntrypointSettings()
    test.setdict({'a': 3}, 10)
    assert test['a'] == 3


def test_update_settings_update_existing_value():
    test = EntrypointSettings()
    test.setdict({'a': 'b', 'c': 'd'}, priority=10)
    test.setdict({'c': 'e', 'f': 'g'}, 10)
    assert len(test.attributes) == 3
    assert test['a'] == 'b'
    assert test['c'] == 'e'
    assert test['f'] == 'g'


def test_update_settings_per_key_priorities_old_behavior():
    test = EntrypointSettings()
    test.set('ITEM_PIPELINES', {'path.one': 100})
    test.set('ITEM_PIPELINES', {'path.two': 200})
    assert test['ITEM_PIPELINES'] == {'path.two': 200}


@pytest.mark.skipif(scrapy_version < (1, 1), reason="requires Scrapy>=1.1")
def test_update_settings_per_key_priorities_new_behaviour():
    from scrapy.settings import BaseSettings
    test = EntrypointSettings()
    test.set('ITEM_PIPELINES', BaseSettings())
    test['ITEM_PIPELINES'].update({'test.path1': 100})
    test['ITEM_PIPELINES'].update({'test.path2': 200})
    assert dict(test['ITEM_PIPELINES']) == {
        'test.path1': 100, 'test.path2': 200}


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = EntrypointSettings()
    test.setdict({'\xf1e\xf1e\xf1e': 'test'}, 10)
    assert test['\xf1e\xf1e\xf1e'] == 'test'
    assert test[to_native_str('\xf1e\xf1e\xf1e')] == 'test'


@pytest.mark.skipif(sys.version_info[0] == 3, reason="requires python2")
def test_update_settings_check_unicode_in_py2_key_value():
    # a dict entry is duplicated as unicode doesn't match native str value
    test = EntrypointSettings()
    test.setdict({'\xf1e\xf1e\xf1e': '\xf1e\xf1e'}, 10)
    assert test['\xf1e\xf1e\xf1e'] == '\xf1e\xf1e'
    native_key = to_native_str('\xf1e\xf1e\xf1e')
    assert test[native_key] == to_native_str('\xf1e\xf1e')


@pytest.mark.skipif(sys.version_info < (3,), reason="requires python3")
def test_update_settings_check_unicode_in_py3():
    test = EntrypointSettings()
    test.setdict({'\xf1e\xf1e\xf1e': 'test'}, 10)
    assert test['\xf1e\xf1e\xf1e'] == 'test'


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
    result = EntrypointSettings()
    result.setdict({'SPIDER_MANAGER_CLASS': 'test.class'})
    _maybe_load_autoscraping_project(result)
    assert result['ITEM_PIPELINES'] == {
        'slybot.dupefilter.DupeFilterPipeline': 0}
    assert result['PROJECT_ZIPFILE'] == 'project-slybot.zip'
    assert result['SLYCLOSE_SPIDER_ENABLED']
    assert result['SLYDUPEFILTER_ENABLED']
    assert result['SPIDER_MANAGER_CLASS'] == 'test.class'


def test_get_component_base():
    assert _get_component_base({}, 'TEST') == 'TEST'
    assert _get_component_base({'SOME_SETTING': 'VAL'}, 'TEST') == 'TEST'
    assert _get_component_base({'TEST_BASE': 'VAL'}, 'TEST') == 'TEST_BASE'


def test_get_action_on_missing_addons_default():
    o = EntrypointSettings()
    assert _get_action_on_missing_addons(o) == 'warn'


def test_get_action_on_missing_addons_base():
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'fail'})
    assert _get_action_on_missing_addons(o) == 'fail'


def test_get_action_on_missing_addons_warn_if_wrong_value():
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'wrong'})
    assert _get_action_on_missing_addons(o) == 'warn'


def test_load_addons_void():
    addons = []
    settings, o = EntrypointSettings(), EntrypointSettings()
    _load_addons(addons, settings, o)
    assert addons == []
    assert settings.attributes == o.attributes == {}


def test_load_addons_basic_usage():
    addons = [TEST_ADDON]
    settings = EntrypointSettings()
    settings.setdict({'SPIDER_MIDDLEWARES': {}})
    o = EntrypointSettings()
    _load_addons(addons, settings, o)
    assert settings['SPIDER_MIDDLEWARES'] == {TEST_ADDON['path']: 10}
    assert o['SPIDER_MIDDLEWARES'] == {TEST_ADDON['path']: 10}


def test_load_addons_basic_with_defaults():
    addons = [TEST_ADDON.copy()]
    addons[0]['default_settings'] = {'TEST_SETTING_A': 'TEST'}
    settings = {'SPIDER_MIDDLEWARES_BASE': {
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500}}
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'warn'})
    _load_addons(addons, settings, o)
    assert settings == {'SPIDER_MIDDLEWARES_BASE': {
        TEST_ADDON['path']: 10,
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': 500
    }}
    assert len(o.attributes) == 3
    assert o['TEST_SETTING_A'] == 'TEST'
    assert o['ON_MISSING_ADDONS'] == 'warn'
    assert len(o['SPIDER_MIDDLEWARES_BASE']) == 3


def test_load_addons_hworker_fail_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = EntrypointSettings()
    settings.setdict({'SPIDER_MIDDLEWARES': {}})
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'fail'})
    with pytest.raises(ImportError):
        _load_addons(addons, settings, o)


def test_load_addons_hworker_error_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'error'})
    _load_addons(addons, settings, o)
    assert len(o.attributes) == 1
    assert o['ON_MISSING_ADDONS'] == 'error'
    assert settings == {'SPIDER_MIDDLEWARES': {}}


def test_load_addons_hworker_warning_on_import():
    addons = [TEST_ADDON.copy()]
    addons[0]['path'] = 'hworker.some.module'
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = EntrypointSettings()
    o.setdict({'ON_MISSING_ADDONS': 'warn'})
    _load_addons(addons, settings, o)
    assert len(o.attributes) == 1
    assert o['ON_MISSING_ADDONS'] == 'warn'
    assert settings == {'SPIDER_MIDDLEWARES': {}}


@mock.patch.dict('sh_scrapy.settings.REPLACE_ADDONS_PATHS',
                 {TEST_ADDON['path']: 'scrapy.utils.misc.arg_to_iter'})
def test_load_addons_hworker_import_replace():
    addons = [TEST_ADDON]
    settings = {'SPIDER_MIDDLEWARES': {}}
    o = EntrypointSettings()
    _load_addons(addons, settings, o)
    assert len(o.attributes) == 1
    assert o['SPIDER_MIDDLEWARES'] == {'scrapy.utils.misc.arg_to_iter': 10}


def test_populate_settings_dont_fail():
    result = _populate_settings_base({}, lambda x: x)
    assert isinstance(result, Settings)


def test_populate_settings_with_default():
    def default_test(s):
        s.set('TEST_SETTING_A', 'test')
    result = _populate_settings_base({}, default_test)
    assert result
    assert result['TEST_SETTING_A'] == 'test'


def test_populate_settings_addons():
    addon = TEST_ADDON.copy()
    addon['default_settings'] = {'TEST_SETTING_A': 'by_addon'}
    msg = {'enabled_addons': [addon]}
    result = _populate_settings_base(msg, lambda x: x)
    assert result
    assert result['TEST_SETTING_A'] == 'by_addon'


def test_populate_settings_override_settings():
    msg = {}
    for section in ['project_settings',
                    'organization_settings',
                    'job_settings']:
        msg[section] = {'TEST_SETTING_A': 'from_' + section}
        result = _populate_settings_base(msg, lambda x: x)
        assert result
        assert result['TEST_SETTING_A'] == 'from_' + section


def test_populate_settings_with_spider():
    msg = {'project_settings': {'JOBDIR': 'by_project'},
           'spider_settings': {'TEST_SETTING_A': 'test'}}
    result = _populate_settings_base(msg, lambda x: x, spider=True)
    assert result
    assert result['TEST_SETTING_A'] == 'test'
    assert result['JOBDIR'].split('/')[-1].startswith('jobdata-')
    assert not result.get('PROJECT_ZIPFILE')


def test_populate_settings_with_spider_override():
    msg = {'job_settings': {'JOBDIR': 'by_job'}}
    result = _populate_settings_base(msg, lambda x: x, spider=True)
    assert result
    assert result['JOBDIR'] == 'by_job'


@mock.patch.dict(os.environ, {'SHUB_SPIDER_TYPE': 'portia'})
def test_populate_settings_with_spider_autoscraping():
    result = _populate_settings_base({}, lambda x: x, spider=True)
    assert result
    assert result['PROJECT_ZIPFILE'] == 'project-slybot.zip'


@mock.patch('sh_scrapy.settings.get_project_settings')
def test_populate_settings_keep_user_priorities(get_settings_mock):
    get_settings_mock.return_value = Settings({
        'EXTENSIONS_BASE': {
            'sh_scrapy.extension.HubstorageExtension': None,
            'scrapy.spidermiddlewares.depth.DepthMiddleware': 10},
        'SPIDER_MIDDLEWARES_BASE': {'scrapy.utils.misc.load_object': 1}})
    addon = TEST_ADDON.copy()
    api_settings = {
        'project_settings': {
            'EXTENSIONS_BASE': {'sh_scrapy.extension.HubstorageExtension': 300,
                                'scrapy.contrib.throttle.AutoThrottle': 5}},
        'enabled_addons': [addon]}
    result = _populate_settings_base(api_settings, lambda x: x, spider=True)
    assert result.getdict('EXTENSIONS_BASE')[
        'sh_scrapy.extension.HubstorageExtension'] is None
    assert result.getdict('EXTENSIONS_BASE').get(
        'scrapy.contrib.throttle.AutoThrottle') is None
    assert result.getdict('EXTENSIONS_BASE')[
        'scrapy.extensions.throttle.AutoThrottle'] == 5
    assert result.getdict('SPIDER_MIDDLEWARES_BASE')[
        'scrapy.utils.misc.load_object'] == 1


def test_populate_settings_unique_update_dict():
    monitoring_dict = {u'SPIDER_OPENED': {u'failed_actions': []}}
    msg = {'spider_settings': {'DASH_MONITORING': monitoring_dict}}
    result = _populate_settings_base(msg, lambda x: x, spider=True)
    assert result['DASH_MONITORING'] == monitoring_dict


@mock.patch('sh_scrapy.settings.get_project_settings')
def test_populate_settings_keep_user_priorities_oldpath(get_settings_mock):
    get_settings_mock.return_value = Settings({
        'EXTENSIONS_BASE': {'scrapy.contrib.throttle.AutoThrottle': 0}})
    api_settings = {
        'project_settings': {
            'EXTENSIONS_BASE': {'scrapy.contrib.throttle.AutoThrottle': 5}}}
    result = _populate_settings_base(api_settings, lambda x: x, spider=True)
    autothrottles = [k for k in result.getdict('EXTENSIONS_BASE')
                     if 'AutoThrottle' in k]
    assert len(autothrottles) == 1
    assert result.getdict('EXTENSIONS_BASE')[
        'scrapy.extensions.throttle.AutoThrottle'] is 0


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
    assert result['MEMUSAGE_LIMIT_MB'] == 950


@mock.patch.dict(os.environ, {'SHUB_JOB_MEMORY_LIMIT': '200'})
def test_load_default_settings_mem_limit():
    result = Settings({'EXTENSIONS_BASE': {},
                       'SPIDER_MIDDLEWARES_BASE': {}})
    _load_default_settings(result)
    assert result['MEMUSAGE_LIMIT_MB'] == 200


def test_populate_settings_dont_fail():
    result = populate_settings({})
    assert isinstance(result, Settings)
    # check one of the settings provided by default by sh_scrapy
    assert result['TELNETCONSOLE_HOST'] == '0.0.0.0'


def test_populate_settings_dont_fail_with_spider():
    result = populate_settings({}, True)
    assert isinstance(result, Settings)
    # check one of the settings provided by default by sh_scrapy
    assert result['TELNETCONSOLE_HOST'] == '0.0.0.0'


def test_update_old_classpaths_not_string():

    class CustomObject(object):
        pass

    test_value = {'scrapy.contrib.exporter.CustomExporter': 1,
                  123: 2, CustomObject: 3}
    test_settings = Settings({'SOME_SETTING': test_value})
    _update_old_classpaths(test_settings)
    expected = test_settings['SOME_SETTING'].keys()
    assert len(expected) == 3
    assert 123 in expected
    assert CustomObject in expected
    assert 'scrapy.exporters.CustomExporter' in expected
