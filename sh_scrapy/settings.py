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


def _load_addons(addons, s, o):
    for addon in addons:
        if addon['path'].startswith('hworker'):
            try:
                import hworker
            except ImportError:
                if addon['path'] in REPLACE_ADDONS_PATHS:
                    addon['path'] = REPLACE_ADDONS_PATHS[addon['path']]
                else:
                    continue  # ignore missing module

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
    _load_addons(enabled_addons, s, o)
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
        'LOG_ENABLED': False,
        'TELNETCONSOLE_HOST': '0.0.0.0',  # to access telnet console from host
    }, priority='cmdline')


def populate_settings(apisettings, spider=None):
    return _populate_settings_base(apisettings, _load_default_settings, spider)
