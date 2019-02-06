import os
import sys
import logging
import tempfile
from sh_scrapy.compat import to_native_str, is_string
from scrapy.settings import Settings
from scrapy.utils.misc import load_object
from scrapy.utils.project import get_project_settings


logger = logging.getLogger(__name__)
REPLACE_ADDONS_PATHS = {
    "hworker.bot.ext.page.PageStorageMiddleware":
        "scrapy_pagestorage.PageStorageMiddleware",
    "hworker.bot.ext.persistence.DotScrapyPersistence":
        "scrapy_dotpersistence.DotScrapyPersistence",
    "scrapylib.deltafetch.DeltaFetch":
        "scrapy_deltafetch.DeltaFetch",
    "scrapylib.magicfields.MagicFieldsMiddleware":
        "scrapy_magicfields.MagicFieldsMiddleware",
    "scrapylib.querycleaner.QueryCleanerMiddleware":
        "scrapy_querycleaner.QueryCleanerMiddleware",
    "scrapylib.splitvariants.SplitVariantsMiddleware":
        "scrapy_splitvariants.SplitVariantsMiddleware",
    "scrapy.contrib.throttle.AutoThrottle":
        "scrapy.extensions.throttle.AutoThrottle",
}
SLYBOT_SPIDER_MANAGER = 'slybot.spidermanager.ZipfileSlybotSpiderManager'
SLYBOT_DUPE_FILTER = 'slybot.dupefilter.DupeFilterPipeline'
SETTINGS_ORDERED_DICTS = [
    "DOWNLOADER_MIDDLEWARES", "DOWNLOADER_MIDDLEWARES_BASE",
    "EXTENSIONS", "EXTENSIONS_BASE",
    "ITEM_PIPELINES", "ITEM_PIPELINES_BASE",
    "SPIDER_CONTRACTS", "SPIDER_CONTRACTS_BASE",
    "SPIDER_MIDDLEWARES", "SPIDER_MIDDLEWARES_BASE"
]

try:
    from scrapy.utils.deprecate import update_classpath
except ImportError:
    update_classpath = lambda x: x


class EntrypointSettings(Settings):
    """
    We need to convert settings to string since the S3 download handler
    doesn't work if the AWS keys are passed as unicode. Other code may
    also depend on settings being str.
    """

    def __init__(self):
        super(EntrypointSettings, self).__init__()
        self.attributes = {}

    def set(self, name, value, priority='project'):
        super(EntrypointSettings, self).set(
            to_native_str(name),
            to_native_str(value) if is_string(value) else value,
            priority=priority)

    def copy_to_dict(self):
        if hasattr(super(EntrypointSettings, self), 'copy_to_dict'):
            return super(EntrypointSettings, self).copy_to_dict()
        # Backward compatibility with older Scrapy versions w/o copy_to_dict
        settings = self.copy()
        return {key: settings[key] for key in settings.attributes}


def _maybe_load_autoscraping_project(settings, priority=0):
    if os.environ.get('SHUB_SPIDER_TYPE') in ('auto', 'portia'):
        slybot_settings = {'ITEM_PIPELINES': {},
                           'SLYDUPEFILTER_ENABLED': True,
                           'SLYCLOSE_SPIDER_ENABLED': True,
                           'SPIDER_MANAGER_CLASS': SLYBOT_SPIDER_MANAGER}
        settings.setdict(slybot_settings, priority=priority)
        settings['ITEM_PIPELINES'][SLYBOT_DUPE_FILTER] = 0
        settings.set("PROJECT_ZIPFILE", 'project-slybot.zip')


def _get_component_base(settings, compkey):
    if settings.get(compkey + '_BASE') is not None:
        return compkey + '_BASE'
    return compkey


def _get_action_on_missing_addons(settings):
    on_missing_addons = settings.get('ON_MISSING_ADDONS', 'warn')
    if on_missing_addons not in ['fail', 'error', 'warn']:
        logger.warning(
            "Wrong value for ON_MISSING_ADDONS: should be one of "
            "[fail,error,warn]. Set default 'warn' value.")
        on_missing_addons = 'warn'
    return on_missing_addons


def _update_old_classpaths(settings):
    """Update user's project settings with proper class paths.

    Note that the method updates only settings with dicts as values:
    it's needed for proper dicts merge to avoid duplicates in paths.
    For all other cases Scrapy will handle it by itself.
    """
    for setting_key in settings.attributes.keys():
        setting_value = settings[setting_key]
        # A workaround to make it work for:
        # - Scrapy==1.0.5 with dicts as values
        # - Scrapy>=1.1.0 with BaseSettings as values
        if hasattr(setting_value, 'copy_to_dict'):
            setting_value = setting_value.copy_to_dict()
        elif not isinstance(setting_value, dict):
            continue
        for path in setting_value.keys():
            if not is_string(path):
                continue
            updated_path = update_classpath(path)
            if updated_path != path:
                order = settings[setting_key].pop(path)
                settings[setting_key][updated_path] = order


def _update_component_order(components, path, order):
    """Update component order only if it's not set yet"""
    updated_path = update_classpath(path)
    if updated_path not in components:
        components[updated_path] = order


def _load_addons(addons, settings, merged_settings, priority=0):
    on_missing_addons = _get_action_on_missing_addons(merged_settings)
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
        skey = _get_component_base(settings, addon['type'])
        components = settings[skey]
        _update_component_order(components, addon_path, addon['order'])
        merged_settings.set(skey, components)
        merged_settings.setdict(addon['default_settings'], priority)


def _merge_with_keeping_order(settings, updates):
    for setting_key, value in updates.items():
        if not isinstance(value, dict):
            settings.set(setting_key, value, priority='cmdline')
            continue
        if setting_key in SETTINGS_ORDERED_DICTS:
            components = settings[setting_key]
            for path, order in value.items():
                _update_component_order(components, path, order)
        else:
            settings.set(setting_key, value)


def _populate_settings_base(apisettings, defaults_func, spider=None):
    """Populate and merge project settings with other ones.

    Important note: Scrapy doesn't really copy values on set/setdict methods,
    changing a dict in merged settings means mutating it in original settings.
    """
    assert 'scrapy.conf' not in sys.modules, "Scrapy settings already loaded"
    settings = get_project_settings().copy()
    _update_old_classpaths(settings)
    merged_settings = EntrypointSettings()

    enabled_addons = apisettings.setdefault('enabled_addons', [])
    project_settings = apisettings.setdefault('project_settings', {})
    organization_settings = apisettings.setdefault('organization_settings', {})
    spider_settings = apisettings.setdefault('spider_settings', {})
    job_settings = apisettings.setdefault('job_settings', {})

    defaults_func(settings)
    merged_settings.setdict(project_settings, priority=10)
    merged_settings.setdict(organization_settings, priority=20)
    if spider:
        merged_settings.setdict(spider_settings, priority=30)
        _maybe_load_autoscraping_project(merged_settings, priority=0)
        merged_settings.set('JOBDIR', tempfile.mkdtemp(prefix='jobdata-'),
                            priority=40)
    merged_settings.setdict(job_settings, priority=40)
    # Load addons only after we gather all settings
    _load_addons(enabled_addons, settings, merged_settings, priority=0)
    _merge_with_keeping_order(settings, merged_settings.copy_to_dict())
    _enforce_required_settings(settings)
    return settings


def _load_default_settings(settings):
    downloader_middlewares = {
        'sh_scrapy.diskquota.DiskQuotaDownloaderMiddleware': -10000,  # closest to the engine
        'sh_scrapy.middlewares.HubstorageDownloaderMiddleware': 10000,  # closest to the downloader
    }
    spider_middlewares = {
        'sh_scrapy.diskquota.DiskQuotaSpiderMiddleware': -10001,  # closest to the engine
        'sh_scrapy.middlewares.HubstorageSpiderMiddleware': -10000,  # right after disk quota middleware
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

    settings.get('DOWNLOADER_MIDDLEWARES_BASE').update(downloader_middlewares)
    settings.get('EXTENSIONS_BASE').update(extensions)
    settings.get('SPIDER_MIDDLEWARES_BASE').update(spider_middlewares)
    memory_limit = int(os.environ.get('SHUB_JOB_MEMORY_LIMIT', 950))
    settings.setdict({
        'STATS_CLASS': 'sh_scrapy.stats.HubStorageStatsCollector',
        'MEMUSAGE_ENABLED': True,
        'MEMUSAGE_LIMIT_MB': memory_limit,
        'DISK_QUOTA_STOP_ON_ERROR': True,
        'WEBSERVICE_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'LOG_ENABLED': False,
        'TELNETCONSOLE_HOST': '0.0.0.0',  # to access telnet console from host
    }, priority='cmdline')


def _enforce_required_settings(settings):
    settings.setdict({
        # breaks logging and useless in scrapy cloud
        'LOG_STDOUT': False,
    }, priority='cmdline')


def populate_settings(apisettings, spider=None):
    return _populate_settings_base(apisettings, _load_default_settings, spider)
