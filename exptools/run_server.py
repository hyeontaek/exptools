'''Run the RPC server.'''

__all__ = ['run_server']

import argparse
import asyncio
import logging
import os
import secrets

from exptools.history import History
from exptools.queue import Queue
from exptools.runner import Runner
from exptools.server import Server

def run_server():
  '''Run the server.'''
  logging_fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
  logging.basicConfig(format=logging_fmt, level=logging.INFO)

  logger = logging.getLogger('exptools.run_server')

  parser = argparse.ArgumentParser(description='Run an exptools RPC server.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-path', type=str, default='secret.dat', help='secret file path')
  parser.add_argument('--history-path',
                      type=str, default='history.dat', help='history file path')

  args = parser.parse_args()

  if not os.path.exists(args.secret_path):
    open(args.secret_path, 'w').write(secrets.token_hex() + '\n')
    logger.info(f'Created new secret file at {args.secret_path}')
  else:
    logger.info(f'Using secret file at {args.secret_path}')
  secret = open(args.secret_path).read().strip()

  if args.history_path is not None:
    logger.info(f'Using history file {args.history_path}')

  loop = asyncio.get_event_loop()

  history = History(args.history_path, loop)
  queue = Queue(history, loop)
  runner = Runner(queue, loop)

  server = Server(args.host, args.port, secret, history, queue, runner, loop)
  server.serve_forever()

if __name__ == '__main__':
  run_server()
