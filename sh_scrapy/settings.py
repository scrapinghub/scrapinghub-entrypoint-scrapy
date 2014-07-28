import sys, os, tempfile, json
from scrapy.utils.project import get_project_settings
from scrapy.settings.default_settings import EXTENSIONS_BASE, SPIDER_MIDDLEWARES_BASE


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


def _load_addons(addons, s, o):
    for addon in addons:
        if addon['path'].startswith('hworker'):
            continue  # ignore missing module

        skey = addon['type']
        components = s[skey]
        components[addon['path']] = addon['order']
        o[skey] = components
        _update_settings(o, addon['default_settings'])


def _populate_settings_base(defaults_func, spider=None):
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    settings = get_project_settings()
    s, o = settings, settings.overrides

    apisettings = json.load(open(os.getenv('JOB_SETTINGS'), 'rb'))

    defaults_func(o)
    _load_addons(apisettings['enabled_addons'], s, o)
    _update_settings(o, apisettings['project_settings'])
    if spider:
        _update_settings(o, apisettings['spider_settings'])
        _maybe_load_autoscraping_project(s, o)
        settings.overrides['JOBDIR'] = tempfile.mkdtemp(prefix='jobdata-')
    return settings


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
    })


def populate_settings(spider=None):
    return _populate_settings_base(_load_default_settings, spider)
