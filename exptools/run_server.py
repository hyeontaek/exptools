'''Run the RPC server.'''

__all__ = ['run_server']

import argparse
import asyncio
import json
import logging
import os
import secrets

from exptools.filter import Filter
from exptools.history import History
from exptools.queue import Queue
from exptools.runner import Runner
from exptools.scheduler import get_scheduler
from exptools.server import Server

def make_parser():
  '''Return a new argument parser.'''
  parser = argparse.ArgumentParser(description='Run the exptools server.')

  parser.add_argument('--host', type=str, default='localhost',
                      help='the hostname of the server (default: %(default)s)')
  parser.add_argument('--port', type=int, default='31234',
                      help='the port number of the server (default: %(default)s)')
  parser.add_argument('--secret-file', type=str,
                      default='secret.json', help='the secret file path (default: %(default)s)')

  parser.add_argument('--scheduler-type', type=str, default='serial',
                      help='the scheduler type')
  parser.add_argument('--scheduler-mode', type=str, default='start',
                      choices=['start', 'stop', 'oneshot'],
                      help='initial scheduler mode')
  parser.add_argument('--scheduler-file', type=str, default='sched_conf.json',
                      help='the scheduler configuration file path')

  parser.add_argument('--history-file', type=str, default='history.json',
                      help='the history file path (default: %(default)s)')
  parser.add_argument('--queue-file', type=str, default='queue.json',
                      help='the queue file path (default: %(default)s)')
  parser.add_argument('--output-dir', type=str, default='output',
                      help='the job output directory (default: %(default)s)')

  parser.add_argument('-v', '--verbose', action='store_true', default=False, help='be verbose')

  return parser

async def run_server(argv, ready_event, loop):
  '''Run the server.'''
  logging_fmt = '%(asctime)s %(name)-19s %(levelname)-8s %(message)s'
  logging.basicConfig(format=logging_fmt, level=logging.INFO)

  # less verbose websockets messages
  logging.getLogger('websockets.protocol').setLevel(logging.WARNING)

  logger = logging.getLogger('exptools.run_server')

  args = make_parser().parse_args(argv)

  if args.verbose:
    logging.getLogger('exptools').setLevel(logging.DEBUG)

  if not os.path.exists(args.secret_file):
    prev_mask = os.umask(0o077)
    json.dump(secrets.token_hex(), open(args.secret_file, 'w'))
    os.umask(prev_mask)
    logger.info(f'Created new secret file at {args.secret_file}')
  else:
    logger.info(f'Using secret file at {args.secret_file}')
  secret = json.load(open(args.secret_file))

  history = History(args.history_file, loop)
  queue = Queue(args.queue_file, history, loop)
  scheduler = get_scheduler(args.scheduler_type)(
      args.scheduler_mode, args.scheduler_file, history, queue, loop)
  runner = Runner(args.output_dir, queue, scheduler, loop)
  filter_ = Filter(loop)
  server = Server(
      args.host, args.port, secret,
      history, queue, scheduler, runner, filter_,
      ready_event, loop)

  state_tasks = [
      asyncio.ensure_future(history.run_forever(), loop=loop),
      asyncio.ensure_future(queue.run_forever(), loop=loop),
      asyncio.ensure_future(scheduler.run_forever(), loop=loop),
      ]

  server_task = asyncio.ensure_future(server.run_forever(), loop=loop)

  execution_tasks = [
      asyncio.ensure_future(runner.run_forever(), loop=loop),
      server_task,
      ]

  try:
    await server_task
  except KeyboardInterrupt:
    pass
  finally:
    try:
      logger.info('Waiting for execution tasks to exit')
      for task in execution_tasks:
        task.cancel()
      await asyncio.gather(*execution_tasks, loop=loop)
    finally:
      logger.info('Waiting for state tasks to exit')
      for task in state_tasks:
        task.cancel()
      await asyncio.gather(*state_tasks, loop=loop)

    logger.info('Tasks exited')
