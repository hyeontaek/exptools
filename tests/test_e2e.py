import asynctest.mock
import pytest

import asyncio
import concurrent
import os
import tempfile

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
  argv = [f'--port={port}']
  ready_event = asyncio.Event()

  with tempfile.TemporaryDirectory() as tmp_dir:
    print(f'Using temporary directory: {tmp_dir}')
    os.chdir(tmp_dir)

    task = asyncio.ensure_future(run_server(argv, ready_event=ready_event, loop=loop), loop=loop)
    await ready_event.wait()

    yield {'port_arg': f'--port={port}'}

    task.cancel()
    try:
      await task
    except concurrent.futures.CancelledError:
      pass

async def run(server, *args, stdin=None, loop=None):
  '''Run a exptools client'''
  args = [server['port_arg']] + list(args)
  return await run_client(args, loop=loop)

@pytest.mark.asyncio
async def test_s(capsys, loop, server):
  await run(server, 's', loop=loop)
  stdout, stderr = capsys.readouterr()
  # pytest -s
  print(stdout)
