import os
import json
from base64 import b64decode
from scrapy.utils.python import stringify_dict


def _make_scrapy_args(arg, args_dict):
    if not args_dict:
        return []
    args = []
    for kv in stringify_dict(args_dict, keys_only=False).items():
        args += [arg, "%s=%s" % kv]
    return args


def _scrapy_crawl_args_and_env(msg):
    args = ['scrapy', 'crawl', str(msg['spider'])] + \
        _make_scrapy_args('-a', msg.get('spider_args')) + \
        _make_scrapy_args('-s', msg.get('settings'))
    env = {
        'SCRAPY_JOB': msg['key'],
        'SCRAPY_SPIDER': msg['spider'],
        'SCRAPY_PROJECT_ID': msg['key'].split('/')[0],
        # the following should be considered deprecated
        'SHUB_SPIDER_TYPE': msg.get('spider_type', '')
    }
    return args, env


def _job_args_and_env(msg):
    env = msg.get('job_env')
    if not isinstance(env, dict):
        env = {}
    cmd = msg.get('job_cmd')
    if not isinstance(cmd, list):
        cmd = [str(cmd)]
    return cmd, stringify_dict(env, keys_only=False)


def _jobname(msg):
    if 'job_name' in msg:
        return msg['job_name']
    elif 'spider' in msg:
        return msg['spider']
    else:
        return msg['job_cmd'][0]


def _jobauth(msg):
    return '{0[key]}:{0[auth]}'.format(msg).encode('hex')


def get_args_and_env(msg):
    envf = _job_args_and_env if 'job_cmd' in msg else _scrapy_crawl_args_and_env
    args, env = envf(msg)
    if 'api_url' in msg:
        env['SHUB_APIURL'] = msg.get('api_url')

    env.update({
        'SHUB_JOBKEY': msg['key'],
        'SHUB_JOBAUTH': _jobauth(msg),
        'SHUB_JOBNAME': _jobname(msg),
        'SHUB_JOB_TAGS': ','.join(msg.get('tags') or ()),  # DEPRECATED?
    })
    return args, env


def decode_uri(uri=None, envvar=None):
    """Return content for a data: or file: URI

    >>> decode_uri('data:application/json;charset=utf8;base64,ImhlbGxvIHdvcmxkIg==')
    u'hello world'
    >>> decode_uri('data:;base64,ImhlbGxvIHdvcmxkIg==')
    u'hello world'
    >>> decode_uri('{"spider": "hello"}')
    {u'spider': u'hello'}

    """
    if envvar is not None:
        uri = os.getenv(envvar, '')
    elif uri is None:
        raise ValueError('An uri or envvar is required')

    mime_type = 'application/json'

    # data:[<MIME-type>][;charset=<encoding>][;base64],<data>
    if uri.startswith('data:'):
        prefix, _, data = uri.rpartition(',')
        mods = {}
        for idx, value in enumerate(prefix[5:].split(';')):
            if idx == 0:
                mime_type = value or mime_type
            elif '=' in value:
                k, _, v = value.partition('=')
                mods[k] = v
            else:
                mods[value] = None

        if 'base64' in mods:
            data = b64decode(data)
        if mime_type == 'application/json':
            data = data.decode(mods.get('charset', 'utf-8'))
            return json.loads(data)
        else:
            return data

    if uri.startswith('{'):
        return json.loads(uri)

    if uri.startswith('/'):
        uri = 'file://' + uri
    if uri.startswith('file://'):
        return json.load(open(uri[7:], 'rb'))


def setup_environment():
    # scrapy.cfg is required by scrapy.utils.project.data_path
    # FIXME: drop this requirement
    if not os.path.exists('scrapy.cfg'):
        open('scrapy.cfg', 'w').close()
