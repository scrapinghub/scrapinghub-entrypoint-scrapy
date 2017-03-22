# -*- coding: utf-8 -*-
import json
import os
import threading
import warnings

from scrapinghub.hubstorage.serialization import jsondefault
from scrapinghub.hubstorage.utils import millitime
from six import StringIO


class _PipeWriter(object):
    """Writer for the Scrapinghub named pipe.

    It's not safe to instantiate and use multiple writers, only one writer
    should be instantiated and used, otherwise data may be corrupted.

    The object is thread safe.

    :ivar path: Named pipe path

    """

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        if path:
            self._pipe = open(self.path, 'w')
        else:
            warnings.warn("Pipe writer is instantiated without a pipe path, "
                          "pipe is replaced with a StringIO object.",
                          stacklevel=2)
            self._pipe = StringIO()

    def _write(self, command, payload):
        # write needs to be locked because write can be called from multiple threads
        encoded_payload = json.dumps(payload,
                                     separators=(',', ':'),
                                     default=jsondefault)
        with self._lock:
            self._pipe.write(command)
            self._pipe.write(' ')
            self._pipe.write(encoded_payload)
            self._pipe.write('\n')
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
