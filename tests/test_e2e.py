import asyncio
import concurrent
import json
import os
import re
import tempfile

import pytest

from exptools.server_main import server_main
from exptools.client_main import client_main


@pytest.fixture
def loop(event_loop):
  """Provide an alias for event_loop fixture."""
  yield event_loop
  # Do not close event_loop; it will be done by the event_loop fixture


@pytest.fixture
async def server(unused_tcp_port, loop):
  """Run a exptools server."""
  port = unused_tcp_port
  ready_event = asyncio.Event()

  with tempfile.TemporaryDirectory() as tmp_dir:
    server_args = [
      f'--host=127.0.0.1',
      f'--port={port}',
      f'--secret-file={tmp_dir}/secret.json',
      f'--scheduler-file={tmp_dir}/sched_conf.json',
      f'--registry-file={tmp_dir}/registry.json',
      f'--history-file={tmp_dir}/history.json',
      f'--queue-file={tmp_dir}/queue.json',
      f'--output-dir={tmp_dir}/output',
      # f'--verbose',
      ]

    task = asyncio.ensure_future(
      server_main(server_args, ready_event=ready_event, loop=loop), loop=loop)

    try:
      await ready_event.wait()

      yield {
        'server_task': task,
        'client_args': [
          f'--host=127.0.0.1',
          f'--port={port}',
          f'--secret-file={tmp_dir}/secret.json',
          # f'--verbose',
          ],
        'tmp_dir': tmp_dir,
        }
    finally:
      task.cancel()
      try:
        await task
      except concurrent.futures.CancelledError:
        pass


async def run(server, args, loop):
  """Run a exptools client"""
  args = server['client_args'] + list(args)
  return await client_main(args, loop=loop)


def create_params_file(server, params):
  """Create a params file."""
  params_json_path = os.path.join(server['tmp_dir'], 'params.json')

  with open(params_json_path, 'w') as file:
    json.dump(params, file)

  return params_json_path


async def add_params(server, paramset, params, loop):
  """Add params to a new paramset."""
  params_json_path = create_params_file(server, params)
  await run(server, ['add', '-c', '-f', params_json_path, paramset], loop=loop)


@pytest.mark.asyncio
async def test_s(capsys, server, loop):
  await run(server, ['--color=no', 's'], loop=loop)
  stdout, stderr = capsys.readouterr()
  # pytest -s
  # print(stdout)
  # print(stderr)
  assert re.search(
    r'^S:0 F:0 A:0 Q:0  Remaining 0s  Finish by .*  Concurrency 1\.0$',
    stdout, re.MULTILINE) is not None


@pytest.mark.asyncio
async def test_add(capsys, server, loop):
  paramset = 'params'
  params = [
    {
      'command': 'ls',
    },
  ]
  await add_params(server, paramset, params, loop=loop)
  stdout, stderr = capsys.readouterr()
  # pytest -s
  # print(stdout)
  # print(stderr)
  assert re.search(r'^Added: %s$' % paramset, stdout, re.MULTILINE) is not None
  assert re.search(
    r'^Added: %d parameters to %s$' % (len(params), paramset),
    stdout, re.MULTILINE) is not None
