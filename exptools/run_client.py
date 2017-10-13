'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
#import logging

from exptools.client import Client
from exptools.estimator import Estimator
from exptools.param import get_exec_id, get_name
from exptools.pretty import pprint
from exptools.time import diff_sec, utcnow, parse_utc

async def _handle_start(client, args):
  await client.runner.start()

async def _handle_stop(client, args):
  await client.runner.stop()

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()

  estimator = Estimator(client.history)

  now = utcnow()

  def _format_elapsed(job):
    if job['finished']:
      sec = job['duration']
    else:
      sec = diff_sec(now, parse_utc(job['started']))
    return '%d:%02d:%02d' % (int(sec / 3600), int(sec % 3600 / 60), int(sec % 60))

  async def _format_remaining(state):
    sec = await estimator.estimate_remaining_time(state)
    return '%d:%02d:%02d' % (int(sec / 3600), int(sec % 3600 / 60), int(sec % 60))

  output = f"Finished jobs ({len(queue_state['finished_jobs'])}):\n"
  for job in queue_state['finished_jobs']:
    output += f"  [{job['id']}] {get_exec_id(job['param'])}"
    output += f' ({_format_elapsed(job)})'
    if not job['succeeded']:
      output += ' [FAILED]'
    output += f"  {get_name(job['param'])}\n"
  output += '\n'

  partial_state = {'started_jobs': [], 'queued_jobs': [],
                   'concurrency': queue_state['concurrency']}

  output += f"Started jobs ({len(queue_state['started_jobs'])}):\n"
  for job in queue_state['started_jobs']:
    partial_state['started_jobs'].append(job)
    output += f"  [{job['id']}] {get_exec_id(job['param'])}"
    rem = await _format_remaining(partial_state)
    output += f' ({_format_elapsed(job)}+{rem})'
    output += f"  {get_name(job['param'])}\n"
  output += '\n'

  output += f"Queued jobs ({len(queue_state['queued_jobs'])}):\n"
  for job in queue_state['queued_jobs']:
    partial_state['pending_jobs'].append(job)
    output += f"  [{job['id']}] {get_exec_id(job['param'])}"
    rem = await _format_remaining(partial_state)
    output += f' ({_format_elapsed(job)}+{rem})'
    output += f"  {get_name(job['param'])}\n"

  #output += '\n'
  #output += f"Concurrency: {queue_state['concurrency']}"
  print(output)

async def _handle_add(client, args):
  param = {'cmd': args.argument}
  job_ids = await client.queue.add([param])
  print(f'New job: {job_ids[0]}')

async def _handle_rm(client, args):
  argument = args.argument
  all_ = False
  if argument and argument[0] == 'all':
    argument = argument[1:]
  if all_:
    await client.queue.remove_queued(None)
  else:
    job_ids = [int(arg) for arg in argument]
    await client.queue.remove_queued(job_ids)

async def _handle_clear(client, args):
  argument = args.argument
  all_ = False
  if argument and argument[0] == 'all':
    all_ = True
    argument = argument[1:]
  if all_:
    await client.queue.remove_finished(None)
  else:
    job_ids = [int(arg) for arg in argument]
    await client.queue.remove_finished(job_ids)

async def _handle_kill(client, args):
  argument = args.argument
  force = False
  all_ = False
  while True:
    if argument and argument[0] == 'force':
      force = True
      argument = argument[1:]
    elif argument and argument[0] == 'all':
      all_ = True
      argument = argument[1:]
    else:
      break
  if all_:
    await client.runner.kill(None, force=force)
  else:
    job_ids = [int(arg) for arg in args.argument]
    await client.runner.kill(job_ids, force=force)

async def handle_command(client, args):
  '''Handle a client command.'''

  if args.command == 'start':
    await _handle_start(client, args)
  elif args.command == 'stop':
    await _handle_stop(client, args)
  elif args.command == 'status':
    await _handle_status(client, args)
  elif args.command == 'add':
    await _handle_add(client, args)
  elif args.command == 'rm':
    await _handle_rm(client, args)
  elif args.command == 'clear':
    await _handle_clear(client, args)
  elif args.command == 'kill':
    await _handle_kill(client, args)

  else:
    raise RuntimeError(f'Invalid command: {args[0]}')

def run_client():
  '''Parse arguments and process a client command.'''

  #logging_fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
  #logging.basicConfig(format=logging_fmt, level=logging.INFO)
  #logger = logging.getLogger('exptools.run_client')

  parser = argparse.ArgumentParser(description='Control the runner.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-path', type=str, default='secret.dat', help='secret file path')
  parser.add_argument('command', type=str, help='command')
  parser.add_argument('argument', type=str, nargs='*', help='arguments')

  args = parser.parse_args()

  #logger.info(f'Using secret file at {args.secret_path}')
  secret = open(args.secret_path, 'r').read().strip()

  loop = asyncio.get_event_loop()

  client = Client(args.host, args.port, secret, loop)
  loop.run_until_complete(asyncio.ensure_future(handle_command(client, args), loop=loop))

if __name__ == '__main__':
  run_client()
