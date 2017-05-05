# -*- coding: utf-8 -*-
import json
import logging
import os
import threading

from six.moves.queue import Queue
import pytest

from sh_scrapy.writer import _PipeWriter


@pytest.fixture
def fifo(tmpdir):
    path = os.path.join(str(tmpdir.mkdir('fifo')), 'scrapinghub')
    os.mkfifo(path)
    return path


@pytest.fixture
def queue():
    return Queue()


@pytest.fixture
def reader(fifo, queue):
    def read_from_fifo():
        with open(fifo) as f:
            for line in iter(f.readline, ''):
                queue.put(line)

    reader_thread = threading.Thread(target=read_from_fifo)
    reader_thread.start()
    try:
        yield reader_thread
    finally:
        reader_thread.join(timeout=1)


@pytest.fixture
def writer(fifo, reader):
    w = _PipeWriter(fifo)
    w.open()
    try:
        yield w
    finally:
        w.close()


def test_close(writer):
    assert writer._pipe.closed is False
    writer.close()
    assert writer._pipe.closed is True


def _parse_data_line(msg):
    assert msg.endswith('\n')
    cmd, _, payload = msg.strip().partition(' ')
    return cmd, json.loads(payload)


def test_write_item(writer, queue):
    writer.write_item({'foo': 'bar'})
    line = queue.get(timeout=1)
    assert queue.empty()
    cmd, payload = _parse_data_line(line)
    assert cmd == 'ITM'
    assert payload == {'foo': 'bar'}


def test_write_request(writer, queue):
    writer.write_request(
        url='http://example.com/',
        status=200,
        method='GET',
        rs=1024,
        duration=102,
        parent=None,
        fp='fingerprint',
    )
    line = queue.get(timeout=1)
    assert queue.empty()
    cmd, payload = _parse_data_line(line)
    assert cmd == 'REQ'
    assert isinstance(payload.pop('time'), int)
    assert payload == {
        'url': 'http://example.com/',
        'status': 200,
        'method': 'GET',
        'rs': 1024,
        'duration': 102,
        'parent': None,
        'fp': 'fingerprint',
    }


def test_write_log(writer, queue):
    writer.write_log(
        level=logging.INFO,
        message='text',
    )
    line = queue.get(timeout=1)
    assert queue.empty()
    cmd, payload = _parse_data_line(line)
    assert cmd == 'LOG'
    assert isinstance(payload.pop('time'), int)
    assert payload == {
        'message': 'text',
        'level': logging.INFO
    }


def test_write_stats(writer, queue):
    stats = {'item_scraped_count': 10, 'scheduler/enqueued': 20}
    writer.write_stats(stats.copy())
    line = queue.get(timeout=1)
    assert queue.empty()
    cmd, payload = _parse_data_line(line)
    assert cmd == 'STA'
    assert isinstance(payload.pop('time'), int)
    assert payload == {
        'stats': stats.copy()
    }


def test_set_outcome(writer, queue):
    outcome = 'custom_outcome'
    writer.set_outcome(outcome)
    line = queue.get(timeout=1)
    assert queue.empty()
    cmd, payload = _parse_data_line(line)
    assert cmd == 'FIN'
    assert payload == {
        'outcome': outcome
    }


def test_writer_raises_runtime_error_if_not_configured():
    error_msg = "Pipe writer is misconfigured, named pipe path is not set"
    w = _PipeWriter('')
    with pytest.raises(RuntimeError) as exc_info:
        w.write_log(10, 'message')
    assert exc_info.value.args[0] == error_msg
    with pytest.raises(RuntimeError) as exc_info:
        w.close()
    assert exc_info.value.args[0] == error_msg
