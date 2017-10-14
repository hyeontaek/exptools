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
from exptools.scheduler import SerialScheduler
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

  parser.add_argument('--history-file', type=str, default='history.json',
                      help='the history file path (default: %(default)s)')
  parser.add_argument('--output-dir', type=str, default='output',
                      help='the job output directory (default: %(default)s)')

  return parser

def run_server():
  '''Run the server.'''
  logging_fmt = '%(asctime)s %(name)-19s %(levelname)-8s %(message)s'
  logging.basicConfig(format=logging_fmt, level=logging.INFO)

  # less verbose websockets messages
  logging.getLogger('websockets.protocol').setLevel(logging.WARNING)

  logger = logging.getLogger('exptools.run_server')

  args = make_parser().parse_args()

  if not os.path.exists(args.secret_file):
    prev_mask = os.umask(0o077)
    json.dump(secrets.token_hex(), open(args.secret_file, 'w'))
    os.umask(prev_mask)
    logger.info(f'Created new secret file at {args.secret_file}')
  else:
    logger.info(f'Using secret file at {args.secret_file}')
  secret = json.load(open(args.secret_file))

  if args.history_file is not None:
    logger.info(f'Using history file {args.history_file}')

  loop = asyncio.get_event_loop()

  history = History(args.history_file, loop)
  queue = Queue(history, loop)
  scheduler = SerialScheduler(queue, loop)
  runner = Runner(args.output_dir, queue, scheduler, loop)
  filter_ = Filter(loop)

  server = Server(args.host, args.port, secret, history, queue, scheduler, runner, filter_, loop)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
