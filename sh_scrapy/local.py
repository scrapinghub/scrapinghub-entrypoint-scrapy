import os
import logging
import warnings

from sh_scrapy.crawl import (
    _fatalerror, ignore_warnings, _get_apisettings, _run,
)
from sh_scrapy.env import _job_args_and_env, _make_scrapy_args


def get_args_and_env(msg):
    # must: spider; optional: job_cmd, job_env, spider_args, settings
    if 'job_cmd' in msg:
        return _job_args_and_env(msg)
    args = ['scrapy', 'crawl', str(msg['spider'])] + \
        _make_scrapy_args('-a', msg.get('spider_args')) + \
        _make_scrapy_args('-s', msg.get('settings'))
    env = msg['job_env'] if isinstance(msg.get('job_env'), dict) else {}
    return args, env


def _run_usercode(spider, args, apisettings_func):
    try:
        from scrapy.exceptions import ScrapyDeprecationWarning
        from sh_scrapy.settings import populate_base_settings

        with ignore_warnings(category=ScrapyDeprecationWarning):
            settings = populate_base_settings(apisettings_func(), spider)
    except Exception:
        logging.exception('Settings initialization failed')
        raise
    try:
        _run(args, settings)
    except Exception:
        logging.exception('Job runtime exception')
        raise


def test_crawl():
    try:
        from scrapy.exceptions import ScrapyDeprecationWarning
        warnings.filterwarnings(
            'ignore', category=ScrapyDeprecationWarning, module='^sh_scrapy')
        from sh_scrapy.env import decode_uri
        job = decode_uri(envvar='SHUB_JOB_DATA')
        assert job, 'SHUB_JOB_DATA must be set'

        args, env = get_args_and_env(job)
        os.environ.update(env)

        from sh_scrapy.env import setup_environment
        setup_environment()
    except:
        _fatalerror()
        raise

    _run_usercode(job['spider'], args, _get_apisettings)
