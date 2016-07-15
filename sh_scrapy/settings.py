from __future__ import print_function
import warnings
import sys, os, tempfile
from sh_scrapy.compat import to_native_str, is_string
from scrapy.utils.project import get_project_settings


REPLACE_ADDONS_PATHS = {
    "hworker.bot.ext.page.PageStorageMiddleware":
        "scrapy_pagestorage.PageStorageMiddleware",
    "hworker.bot.ext.persistence.DotScrapyPersistence":
        "scrapy_dotpersistence.DotScrapyPersistence",
}

try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x


def _update_settings(o, d):
    # We need to convert settings to string since the S3 download handler
    # doesn't work if the AWS keys are passed as unicode. Other code may also
    # depend on settings being str. TODO: we should test this
    for k, v in d.items():
        d[to_native_str(k)] = to_native_str(v) if is_string(v) else v
    o.update(d)


def _load_autoscraping_settings(s, o):
    o.setdefault('SPIDER_MANAGER_CLASS', 'slybot.spidermanager.ZipfileSlybotSpiderManager')
    o.setdefault('SLYCLOSE_SPIDER_ENABLED', True)
    o.setdefault('ITEM_PIPELINES', {})['slybot.dupefilter.DupeFilterPipeline'] = 0
    o.setdefault('SLYDUPEFILTER_ENABLED', True)


def _maybe_load_autoscraping_project(s, o):
    if os.environ.get('SHUB_SPIDER_TYPE') in ('auto', 'portia'):
        _load_autoscraping_settings(s, o)
        o["PROJECT_ZIPFILE"] = 'project-slybot.zip'


def _get_component_base(s, compkey):
    if s.get(compkey + '_BASE') is not None:
        return compkey + '_BASE'
    return compkey


def _get_action_on_missing_addons(settings):
    for section in settings:
        if 'ON_MISSING_ADDONS' in section:
            level = section['ON_MISSING_ADDONS']
            if level not in ['fail', 'error', 'warn']:
                warnings.warn("Wrong value for ON_MISSING_ADDONS: "
                              "should be one of [fail,error,warn]."
                              "Fallback to default 'warn' value")
                level = 'warn'
            return level
    return 'warn'


def _load_addons(addons, s, o, on_missing_addons):
    for addon in addons:
        if addon['path'] in REPLACE_ADDONS_PATHS:
            addon['path'] = REPLACE_ADDONS_PATHS[addon['path']]
        module = addon['path'].rsplit('.', 1)[0]
        try:
            __import__(module)
        except ImportError:
            if on_missing_addons == 'warn':
                warnings.warn("Addon's module %s not found" % module, Warning)
                continue
            elif on_missing_addons == 'error':
                print("Addon's module %s not found" % module, file=sys.stderr)
                continue
            raise
        skey = _get_component_base(s, addon['type'])
        components = s[skey]
        path = update_classpath(addon['path'])
        components[path] = addon['order']
        o[skey] = components
        _update_settings(o, addon['default_settings'])


def _populate_settings_base(apisettings, defaults_func, spider=None):
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    s = get_project_settings()
    o = {}

    enabled_addons = apisettings.setdefault('enabled_addons', [])
    project_settings = apisettings.setdefault('project_settings', {})
    organization_settings = apisettings.setdefault('organization_settings', {})
    spider_settings = apisettings.setdefault('spider_settings', {})
    job_settings = apisettings.setdefault('job_settings', {})

    defaults_func(s)
    on_missing_addons = _get_action_on_missing_addons([
        job_settings, spider_settings,
        organization_settings, project_settings])
    _load_addons(enabled_addons, s, o, on_missing_addons)
    _update_settings(o, project_settings)
    _update_settings(o, organization_settings)
    if spider:
        _update_settings(o, spider_settings)
        _maybe_load_autoscraping_project(s, o)
        o['JOBDIR'] = tempfile.mkdtemp(prefix='jobdata-')
    _update_settings(o, job_settings)
    s.setdict(o, priority='cmdline')
    return s


def _load_default_settings(s):
    downloader_middlewares = {
        'sh_scrapy.diskquota.DiskQuotaDownloaderMiddleware': 0,
    }
    spider_middlewares = {
        'sh_scrapy.extension.HubstorageMiddleware': 0,
        'sh_scrapy.diskquota.DiskQuotaSpiderMiddleware': 0,
    }
    extensions = {
        'scrapy.extensions.debug.StackTraceDump': 0,
        'sh_scrapy.extension.HubstorageExtension': 100,
    }

    try:
        import slybot
    except ImportError:
        pass
    else:
        extensions['slybot.closespider.SlybotCloseSpider'] = 0

    s.get('DOWNLOADER_MIDDLEWARES_BASE').update(downloader_middlewares)
    s.get('EXTENSIONS_BASE').update(extensions)
    s.get('SPIDER_MIDDLEWARES_BASE').update(spider_middlewares)
    memory_limit = int(os.environ.get('SHUB_JOB_MEMORY_LIMIT', 950))
    s.setdict({
        'STATS_CLASS': 'sh_scrapy.stats.HubStorageStatsCollector',
        'MEMUSAGE_ENABLED': True,
        'MEMUSAGE_LIMIT_MB': memory_limit,
        'DISK_QUOTA_STOP_ON_ERROR': True,
        'WEBSERVICE_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'TELNETCONSOLE_HOST': '0.0.0.0',  # to access telnet console from host
    }, priority='cmdline')


def populate_settings(apisettings, spider=None):
    return _populate_settings_base(apisettings, _load_default_settings, spider)
