'''Provide runnerctl.'''

import asyncio
import argparse
import logging
#import pprint

from exptools.client import Client

async def handle(client, args):
  '''Handle a command.'''

  if args.command == 'start':
    await client.runner.start()

  elif args.command == 'stop':
    await client.runner.stop()

  elif args.command == 'kill':
    if args.argument and args.argument[0] == 'force':
      job_ids = [int(arg) for arg in arg.argument[1:]]
      await client.runner.kill(job_ids, force=True)
    else:
      job_ids = [int(arg) for arg in args.argument]
      await client.runner.kill(job_ids)

  elif args.command == 'killall':
    if args.argument and args.argument[0] == 'force':
      await client.runner.killall(force=True)
    else:
      await client.runner.killall()

  else:
    raise RuntimeError(f'Invalid command: {args[0]}')

def runnerctl():
  '''Parse arguments and process a command.'''

  logging_fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
  logging.basicConfig(format=logging_fmt, level=logging.INFO)
  logger = logging.getLogger('exptools.runnerctl')

  parser = argparse.ArgumentParser(description='Control the runner.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-path', type=str, default='secret.txt', help='secret file path')
  parser.add_argument('command', type=str, help='command')
  parser.add_argument('argument', type=str, nargs='*', help='arguments')

  args = parser.parse_args()

  logger.info(f'Using secret file at {args.secret_path}')
  secret = open(args.secret_path, 'r').read().strip()

  loop = asyncio.get_event_loop()

  client = Client(args.host, args.port, secret, loop)
  loop.run_until_complete(asyncio.ensure_future(handle(client, args), loop=loop))

if __name__ == '__main__':
  runnerctl()
