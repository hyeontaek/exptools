import asyncio
import concurrent
import os
import re
import tempfile

import pytest

from exptools.run_server import run_server
from exptools.run_client import run_client

@pytest.fixture
async def loop(event_loop):
  '''Provide an alias for event_loop fixture.'''
  yield event_loop
  event_loop.stop()

@pytest.fixture
async def server(unused_tcp_port, loop):
  '''Run a exptools server.'''
  port = unused_tcp_port
  ready_event = asyncio.Event()

  with tempfile.TemporaryDirectory() as tmp_dir:
    server_args = [
        f'--host=127.0.0.1',
        f'--port={port}',
        f'--secret-file={tmp_dir}/secret.json',
        f'--scheduler-file={tmp_dir}/sched_conf.json',
        f'--history-file={tmp_dir}/history.json',
        f'--queue-file={tmp_dir}/queue.json',
        f'--output-dir={tmp_dir}/output',
        ]

    task = asyncio.ensure_future(
        run_server(server_args, ready_event=ready_event, loop=loop), loop=loop)
    await ready_event.wait()

    yield {
        'client_args': [
            f'--host=127.0.0.1',
            f'--port={port}',
            f'--secret-file={tmp_dir}/secret.json',
            ]
        }

    task.cancel()
    try:
      await task
    except concurrent.futures.CancelledError:
      pass

async def run(server, args, stdin=None, loop=None):
  '''Run a exptools client'''
  args = server['client_args'] + list(args)
  return await run_client(args, loop=loop)

@pytest.mark.asyncio
async def test_s(capsys, loop, server):
  await run(server, ['--color=no', 's'], loop=loop)
  stdout, stderr = capsys.readouterr()
  # pytest -s
  #print(stdout)
  assert re.search(
      r'^S:0 F:0 A:0 Q:0  Remaining 0s  Finish by .*  Concurrency 1\.0$',
      stdout) is not None
