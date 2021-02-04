from sh_scrapy.settings import populate_settings
from sh_scrapy.crawl import _get_apisettings


def get_project_settings():
    return populate_settings(_get_apisettings())
