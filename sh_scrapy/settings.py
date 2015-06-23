import sys, os, tempfile
from scrapy.utils.project import get_project_settings
from scrapy.settings.default_settings import EXTENSIONS_BASE, SPIDER_MIDDLEWARES_BASE

try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x

from .env import decode_uri


def _update_settings(o, d):
    # We need to convert settings to string since the S3 download handler
    # doesn't work if the AWS keys are passed as unicode. Other code may also
    # depend on settings being str. TODO: we should test this
    for k, v in d.items():
        d[k.encode('utf-8')] = v.encode('utf-8') if isinstance(v, unicode) else v
    o.update(d)


def _load_autoscraping_settings(s, o):
    o.setdefault('SPIDER_MANAGER_CLASS', 'slybot.spidermanager.ZipfileSlybotSpiderManager')
    o.setdefault('SLYCLOSE_SPIDER_ENABLED', True)
    o.setdefault('ITEM_PIPELINES', {})['slybot.dupefilter.DupeFilterPipeline'] = 0
    o.setdefault('SLYDUPEFILTER_ENABLED', True)


def _maybe_load_autoscraping_project(s, o):
    if os.environ.get('SHUB_SPIDER_TYPE') == 'auto':
        _load_autoscraping_settings(s, o)
        o["PROJECT_ZIPFILE"] = 'project-slybot.zip'


def _get_component_base(s, compkey):
    if s.get(compkey + '_BASE') is not None:
        return compkey + '_BASE'
    return compkey


def _load_addons(addons, s, o):
    for addon in addons:
        if addon['path'].startswith('hworker'):
            continue  # ignore missing module

        skey = _get_component_base(s, addon['type'])
        components = s[skey]
        path = update_classpath(addon['path'])
        components[path] = addon['order']
        o[skey] = components
        _update_settings(o, addon['default_settings'])


def _populate_settings_base(defaults_func, spider=None):
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    s = get_project_settings()
    o = {}

    apisettings = decode_uri(envvar='JOB_SETTINGS') or {}
    enabled_addons = apisettings.setdefault('enabled_addons', {})
    project_settings = apisettings.setdefault('project_settings', {})
    spider_settings = apisettings.setdefault('spider_settings', {})

    defaults_func(o)
    _load_addons(enabled_addons, s, o)
    _update_settings(o, project_settings)
    if spider:
        _update_settings(o, spider_settings)
        _maybe_load_autoscraping_project(s, o)
        o['JOBDIR'] = tempfile.mkdtemp(prefix='jobdata-')
    s.setdict(o, priority='cmdline')
    return s


def _load_default_settings(o):
    SPIDER_MIDDLEWARES_BASE.update({
        'sh_scrapy.extension.HubstorageMiddleware': 0,
    })
    EXTENSIONS_BASE.update({
        'scrapy.contrib.debug.StackTraceDump': 0,
        #'slybot.closespider.SlybotCloseSpider': 0,
        'sh_scrapy.extension.HubstorageExtension': 100,
    })
    o.update({
        'EXTENSIONS_BASE': EXTENSIONS_BASE,
        'STATS_CLASS': 'sh_scrapy.stats.HubStorageStatsCollector',
        'MEMUSAGE_ENABLED': True,
        'MEMUSAGE_LIMIT_MB': 512,
        'WEBSERVICE_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': 'scrapy.log',
        'TELNETCONSOLE_HOST': '0.0.0.0',  # to access telnet console from host
    })


def populate_settings(spider=None):
    return _populate_settings_base(_load_default_settings, spider)
