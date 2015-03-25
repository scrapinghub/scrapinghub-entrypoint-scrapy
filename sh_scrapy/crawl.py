#!/usr/bin/env python
# --------------------- DO NOT ADD IMPORTS HERE -------------------------
# Add them below so that any import errors are caught and sent to sentry
# -----------------------------------------------------------------------
import os, sys, logging, json
# XXX: Do not use atexit to close Hubstorage client!
# why: functions registed with atexit are called when run_script() finishes,
# and at that point main() function doesn't completed leading to lost log
# messages.

# Keep a reference to standard output/error as they are redirected
# at log initialization
_sys_stderr = sys.stderr  # stderr and stoud are redirected to HS later
_sys_stdout = sys.stdout
# Sentry DSN ins passed by environment variable
_sentry_dsn = os.environ.pop('SENTRY_DSN', None)


def _fatalerror():
    if _sentry_dsn:
        try:
            from raven import Client
        except ImportError:
            # Do not fail here, previous error is more important
            print >>_sys_stderr, 'HWORKER_SENTRY_DSN is set but python-raven is not installed'
        else:
            Client(_sentry_dsn).captureException()

    # Log error to hworker slotN.out
    # Inspired by logging.Handler.handleError()
    import traceback
    ei = sys.exc_info()
    try:
        traceback.print_exception(ei[0], ei[1], ei[2], None, _sys_stderr)
    except IOError:
        pass
    finally:
        del ei


def main():
    try:
        from sh_scrapy.env import get_args_and_env, decode_uri
        job = decode_uri(envvar='JOB_DATA')
        args, env = get_args_and_env(job)
        os.environ.update(env)

        from sh_scrapy.log import initialize_logging
        from sh_scrapy.settings import populate_settings
        loghdlr = initialize_logging()
    except:
        _fatalerror()
        raise

    # user code will be imported beyond this point --------------
    try:
        settings = populate_settings(job['spider'])
        loghdlr.setLevel(settings['LOG_LEVEL'])
    except Exception:
        logging.exception('Settings initialization failed')
        raise

    try:
        _run(args, settings)
    except Exception:
        logging.exception('Script initialization failed')
        raise


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
    import pkg_resources, sys
    scriptname = argv[0]
    sys.argv = argv

    def get_distribution():
        for ep in pkg_resources.WorkingSet().iter_entry_points('scrapy'):
            if ep.name == 'settings':
                return ep.dist
    d = get_distribution()
    d.run_script(scriptname, {'__name__': '__main__'})


if __name__ == '__main__':
    try:
        main()
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
