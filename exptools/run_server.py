'''Run the RPC server.'''

__all__ = ['run_server']

import argparse
import asyncio
import json
import logging
import os
import secrets

from exptools.history import History
from exptools.queue import Queue
from exptools.runner import Runner
from exptools.server import Server

def run_server():
  '''Run the server.'''
  logging_fmt = '%(asctime)s %(name)-19s %(levelname)-8s %(message)s'
  logging.basicConfig(format=logging_fmt, level=logging.INFO)

  # less verbose websockets messages
  logging.getLogger('websockets.protocol').setLevel(logging.WARNING)

  logger = logging.getLogger('exptools.run_server')

  parser = argparse.ArgumentParser(description='Run an exptools RPC server.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-file', type=str, default='secret.json', help='secret file path')
  parser.add_argument('--history-file',
                      type=str, default='history.json', help='history file path')
  parser.add_argument('--output-dir', type=str, default='output', help='job output directory')

  args = parser.parse_args()

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
  runner = Runner(args.output_dir, queue, loop)

  server = Server(args.host, args.port, secret, history, queue, runner, loop)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass

if __name__ == '__main__':
  run_server()
