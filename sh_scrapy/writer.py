# -*- coding: utf-8 -*-
import json
import os
import threading

from scrapinghub.hubstorage.serialization import jsondefault
from scrapinghub.hubstorage.utils import millitime


def _not_configured(*args, **kwargs):
    raise RuntimeError("Pipe writer is misconfigured, named pipe path is not set")


class _PipeWriter(object):
    """Writer for the Scrapinghub named pipe.

    It's not safe to instantiate and use multiple writers, only one writer
    should be instantiated and used, otherwise data may be corrupted.

    The object is thread safe.

    :ivar path: Named pipe path

    """

    def __init__(self, path):
        self.path = path or ''
        self._lock = threading.Lock()
        self._pipe = None
        if not self.path:
            self._write = _not_configured
            self.open = _not_configured
            self.close = _not_configured

    def open(self):
        with self._lock:
            self._pipe = open(self.path, 'wb')

    def _write(self, command, payload):
        # binary command
        command = command.encode('utf-8')
        # binary payload
        encoded_payload = json.dumps(
            payload,
            separators=(',', ':'),
            default=jsondefault
        ).encode('utf-8')
        # write needs to be locked because write can be called from multiple threads
        with self._lock:
            self._pipe.write(command)
            self._pipe.write(b' ')
            self._pipe.write(encoded_payload)
            self._pipe.write(b'\n')
            self._pipe.flush()

    def write_log(self, level, message):
        log = {
            'time': millitime(),
            'level': level,
            'message': message
        }
        self._write('LOG', log)

    def write_request(self, url, status, method, rs, duration, parent, fp):
        request = {
            'url': url,
            'status': int(status),
            'method': method,
            'rs': int(rs),
            'duration': int(duration),
            'parent': parent,
            'time': millitime(),
            'fp': fp,
        }
        self._write('REQ', request)

    def write_item(self, item):
        self._write('ITM', item)

    def write_stats(self, stats):
        self._write('STA', {'time': millitime(), 'stats': stats})

    def set_outcome(self, outcome):
        self._write('FIN', {'outcome': outcome})

    def close(self):
        with self._lock:
            self._pipe.close()


pipe_writer = _PipeWriter(os.environ.get('SHUB_FIFO_PATH', ''))
