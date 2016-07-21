import logging
import sys, os, tempfile
from sh_scrapy.compat import to_native_str, is_string
from scrapy.settings import BaseSettings
from scrapy.utils.misc import load_object
from scrapy.utils.project import get_project_settings


logger = logging.getLogger(__name__)
REPLACE_ADDONS_PATHS = {
    "hworker.bot.ext.page.PageStorageMiddleware":
        "scrapy_pagestorage.PageStorageMiddleware",
    "hworker.bot.ext.persistence.DotScrapyPersistence":
        "scrapy_dotpersistence.DotScrapyPersistence",
}
SLYBOT_SPIDER_MANAGER = 'slybot.spidermanager.ZipfileSlybotSpiderManager'

try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x


def _update_settings(o, d, priority='default'):
    """
    We need to convert settings to string since the S3 download handler
    doesn't work if the AWS keys are passed as unicode. Other code may also
    depend on settings being str.

    :param o: auxiliary BaseSettings object to merge provided settings
    :type o: scrapy.settings.BaseSettings instance

    :param d: final Settings object to run a job
    :type d: scrapy.settings.Settings instance
    """
    for k, v in list(d.items()):
        d[to_native_str(k)] = to_native_str(v) if is_string(v) else v
    o.update(d, priority=priority)


def _maybe_load_autoscraping_project(s, o, priority=0):
    if os.environ.get('SHUB_SPIDER_TYPE') in ('auto', 'portia'):
        settings = {'ITEM_PIPELINES': {},
                    'SLYDUPEFILTER_ENABLED': True,
                    'SLYCLOSE_SPIDER_ENABLED': True,
                    'SPIDER_MANAGER_CLASS': SLYBOT_SPIDER_MANAGER}
        _update_settings(o, settings, priority=priority)
        o['ITEM_PIPELINES']['slybot.dupefilter.DupeFilterPipeline'] = 0
        o["PROJECT_ZIPFILE"] = 'project-slybot.zip'


def _get_component_base(s, compkey):
    if s.get(compkey + '_BASE') is not None:
        return compkey + '_BASE'
    return compkey


def _get_action_on_missing_addons(o):
    on_missing_addons = o.get('ON_MISSING_ADDONS', 'warn')
    if on_missing_addons not in ['fail', 'error', 'warn']:
        logger.warning(
            "Wrong value for ON_MISSING_ADDONS: should be one of "
            "[fail,error,warn]. Set default 'warn' value.")
        on_missing_addons = 'warn'
    return on_missing_addons


def _load_addons(addons, s, o, priority=0):
    on_missing_addons = _get_action_on_missing_addons(o)
    for addon in addons:
        addon_path = addon['path']
        if addon_path in REPLACE_ADDONS_PATHS:
            addon_path = REPLACE_ADDONS_PATHS[addon_path]
        try:
            load_object(addon_path)
        except (ImportError, NameError, ValueError) as exc:
            message = "Addon import error {}:\n {}".format(addon_path, exc)
            if on_missing_addons == 'warn':
                logger.warning(message)
                continue
            elif on_missing_addons == 'error':
                logger.error(message)
                continue
            raise
        skey = _get_component_base(s, addon['type'])
        components = s[skey]
        path = update_classpath(addon_path)
        components[path] = addon['order']
        o[skey] = components
        _update_settings(o, addon['default_settings'], priority)


def _populate_settings_base(apisettings, defaults_func, spider=None):
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    s = get_project_settings()
    o = BaseSettings()

    enabled_addons = apisettings.setdefault('enabled_addons', [])
    project_settings = apisettings.setdefault('project_settings', {})
    organization_settings = apisettings.setdefault('organization_settings', {})
    spider_settings = apisettings.setdefault('spider_settings', {})
    job_settings = apisettings.setdefault('job_settings', {})

    defaults_func(s)
    _update_settings(o, project_settings, priority=10)
    _update_settings(o, organization_settings, priority=20)
    if spider:
        _update_settings(o, spider_settings, priority=30)
        _maybe_load_autoscraping_project(s, o, priority=0)
        o['JOBDIR'] = tempfile.mkdtemp(prefix='jobdata-')
    _update_settings(o, job_settings, priority=40)
    # Load addons only after we gather all settings
    _load_addons(enabled_addons, s, o, priority=0)
    s.setdict(o.copy_to_dict(), priority='cmdline')
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
