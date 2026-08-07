from sh_scrapy.crawl import _get_apisettings
from sh_scrapy.settings import populate_settings


def get_project_settings():
    return populate_settings(_get_apisettings())
