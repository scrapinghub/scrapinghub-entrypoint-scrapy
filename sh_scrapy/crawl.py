#!/usr/bin/env python
# --------------------- DO NOT ADD IMPORTS HERE -------------------------
# Add them below so that any import errors are caught and sent to sentry
# -----------------------------------------------------------------------
import os
import sys
import socket
import logging
import datetime
import warnings
from contextlib import contextmanager
# XXX: Do not use atexit to close Hubstorage client!
# why: functions registed with atexit are called when run_script() finishes,
# and at that point main() function doesn't completed leading to lost log
# messages.

# Keep a reference to standard output/error as they are redirected
# at log initialization
_sys_stderr = sys.stderr  # stderr and stoud are redirected to HS later
_sys_stdout = sys.stdout
# Sentry DSN ins passed by environment variable
_hworker_sentry_dsn = os.environ.pop('HWORKER_SENTRY_DSN', None)
_sentry_dsn = os.environ.pop('SENTRY_DSN', _hworker_sentry_dsn)

# Set default socket timeout for code that doesn't
socket.setdefaulttimeout(60.0)


@contextmanager
def ignore_warnings(**kwargs):
    """Context manager that creates a temporary filter to ignore warnings.

    This context manager behaves similarly to warnings.catch_warnings though
    filtered warnings aren't recorded and you can ignore them by some criteria
    matching warnings.simplefilter arguments.

    As warnings.catch_warnings, this context manager is not thread-safe.
    """
    _filters = warnings.filters[:]
    warnings.filterwarnings('ignore', **kwargs)
    yield
    warnings.filters = _filters


def _fatalerror():
    # Log error to hworker slotN.out
    # Inspired by logging.Handler.handleError()
    #
    # Capture exc_info early on, so that an error in the handler doesn't
    # overwrite it.
    import traceback
    ei = sys.exc_info()

    if _sentry_dsn:
        try:
            from raven import Client
        except ImportError:
            # Do not fail here, previous error is more important
            print >>_sys_stderr, 'HWORKER_SENTRY_DSN is set but python-raven '\
                                 'is not installed'
        else:
            try:
                Client(_sentry_dsn).captureException()
            except Exception as err:
                print >>_sys_stderr, datetime.datetime.utcnow().isoformat(), \
                    "Error when sending fatal error to sentry:", err

    # Log error to hworker slotN.out
    # Inspired by logging.Handler.handleError()
    try:
        print >>_sys_stderr, datetime.datetime.utcnow().isoformat(),
        traceback.print_exception(ei[0], ei[1], ei[2], None, _sys_stderr)
    except IOError:
        pass
    finally:
        del ei


def _get_apisettings():
    from sh_scrapy.env import decode_uri
    return decode_uri(envvar='JOB_SETTINGS') or {}


def _run(args, settings):
    # SCRAPY_PROJECT_ID is set in both scrapy jobs and scrapy list (deploys)
    if 'SCRAPY_PROJECT_ID' in os.environ:
        _run_scrapy(args, settings)
    else:
        _run_pkgscript(args)


def _run_scrapy(argv, settings):
    from scrapy.cmdline import execute
    sys.argv = argv
    execute(settings=settings)


def _run_pkgscript(argv):
    import pkg_resources
    if argv[0].startswith('py:'):
        argv[0] = argv[0][3:]
    scriptname = argv[0]
    sys.argv = argv

    def get_distribution():
        for ep in pkg_resources.WorkingSet().iter_entry_points('scrapy'):
            if ep.name == 'settings':
                return ep.dist
    d = get_distribution()
    d.run_script(scriptname, {'__name__': '__main__'})


def _run_usercode(spider, args, apisettings_func, log_handler=None):
    try:
        from scrapy.exceptions import ScrapyDeprecationWarning
        from sh_scrapy.settings import populate_settings

        with ignore_warnings(category=ScrapyDeprecationWarning):
            settings = populate_settings(apisettings_func(), spider)
        if log_handler is not None:
            log_handler.setLevel(settings['LOG_LEVEL'])
    except Exception:
        logging.exception('Settings initialization failed')
        raise

    try:
        _run(args, settings)
    except Exception:
        logging.exception('Script initialization failed')
        raise


def _launch():
    try:
        from scrapy.exceptions import ScrapyDeprecationWarning
        warnings.filterwarnings(
            'ignore', category=ScrapyDeprecationWarning, module='^sh_scrapy')
        from sh_scrapy.env import get_args_and_env, decode_uri
        job = decode_uri(envvar='JOB_DATA')
        assert job, 'JOB_DATA must be set'
        args, env = get_args_and_env(job)
        os.environ.update(env)

        print args, env

        from sh_scrapy.log import initialize_logging
        from sh_scrapy.settings import populate_settings  # NOQA
        from sh_scrapy.env import setup_environment
        loghdlr = initialize_logging()
        setup_environment()
    except:
        _fatalerror()
        raise

    _run_usercode(job['spider'], args, _get_apisettings, loghdlr)


def list_spiders():
    """ An entrypoint for list-spiders."""
    try:
        from scrapy.exceptions import ScrapyDeprecationWarning
        warnings.filterwarnings(
            'ignore', category=ScrapyDeprecationWarning, module='^sh_scrapy')
        from sh_scrapy.env import setup_environment
        setup_environment()
    except:
        _fatalerror()
        raise

    _run_usercode(None, ['scrapy', 'list'], _get_apisettings)


def main():
    try:
        _launch()
    finally:
        sys.stderr = _sys_stderr
        sys.stdout = _sys_stdout
        try:
            from sh_scrapy.hsref import hsref
            hsref.close()
        except Exception:
            _fatalerror()


if __name__ == '__main__':
    sys.exit(main())
