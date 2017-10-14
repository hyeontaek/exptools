'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import json
#import logging
import sys
import traceback

import termcolor

from exptools.client import Client
#from exptools.pretty import pprint
from exptools.time import (
    format_elapsed_time_short,
    format_remaining_time_short,
    format_estimated_time,
    )
from exptools.param import get_exec_ids


# pylint: disable=unused-argument
async def _handle_start(client, args):
  await client.runner.start()

# pylint: disable=unused-argument
async def _handle_stop(client, args):
  await client.runner.stop()

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()
  print(await format_estimated_time(client.estimator, queue_state))

async def _handle_monitor(client, client2, args):
  async for queue_state in client2.queue.watch_state():
    print(await format_estimated_time(client.estimator, queue_state))

async def _handle_ls(client, args):
  queue_state = await client.queue.get_state()

  if not args.arguments:
    show = set(['finished', 'started', 'queued'])
  else:
    show = set(args.arguments)

  output = ''

  if 'finished' in show:
    output += f"Finished jobs ({len(queue_state['finished_jobs'])}):\n"
    for job in queue_state['finished_jobs']:
      line = f"  {job['job_id']} {job['exec_id']}"
      line += f' [{format_elapsed_time_short(job)}]'
      if job['succeeded']:
        line += ' succeeded:'
      else:
        line += ' FAILED:'
      line += f" {job['name']}"
      if not job['succeeded']:
        line = termcolor.colored(line, 'red')
      output += line + '\n'
    output += '\n'

  partial_state = {'finished_jobs': queue_state['finished_jobs'],
                   'started_jobs': [], 'queued_jobs': [],
                   'concurrency': queue_state['concurrency']}

  if 'started' in show:
    output += f"Started jobs ({len(queue_state['started_jobs'])}):\n"
    for job in queue_state['started_jobs']:
      partial_state['started_jobs'].append(job)
      line = f"  {job['job_id']} {job['exec_id']}"
      rem = await format_remaining_time_short(client.estimator, partial_state)
      line += f' [{format_elapsed_time_short(job)}+{rem}]:'
      line += f" {job['name']}"
      line = termcolor.colored(line, 'yellow')
      output += line + '\n'
    output += '\n'
  else:
    for job in queue_state['started_jobs']:
      partial_state['started_jobs'].append(job)

  if 'queued' in show:
    output += f"Queued jobs ({len(queue_state['queued_jobs'])}):\n"
    for job in queue_state['queued_jobs']:
      partial_state['pending_jobs'].append(job)
      line = f"  {job['job_id']} {job['exec_id']}"
      rem = await format_remaining_time_short(client.estimator, partial_state)
      line += f' [{format_elapsed_time_short(job)}+{rem}]:'
      line += f" {job['name']}"
      line = termcolor.colored(line, 'blue')
      output += line + '\n'
    output += '\n'

  #output += f"Concurrency: {queue_state['concurrency']}"
  print(output.strip() + '\n')

async def _handle_run(client, args):
  params = [{'cmd': args.arguments}]
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {job_ids[0]}')

async def _handle_retry(client, args):
  arguments = args.arguments
  if not arguments:
    job_ids = await client.queue.retry(None)
  else:
    job_ids = await client.queue.retry(arguments)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _omit_params(client, args, params):
  '''Omit parameters.'''
  omit = args.omit.split(',')
  if omit:
    for entry in omit:
      assert entry in ('finished', 'succeeded', 'started', 'queued'), f'Invalid omission: {entry}'
  if 'finished' in omit:
    params = await client.history.omit(params, only_succeeded=False)
  if 'succeeded' in omit:
    params = await client.history.omit(params, only_succeeded=True)
  if 'started' in omit:
    params = await client.queue.omit(params, queued=False, started=True, finished=False)
  if 'queued' in omit:
    params = await client.queue.omit(params, queued=True, started=False, finished=False)
  return params

async def _handle_add(client, args):
  params = []
  if len(args.arguments) == 1 and args.arguments[0] == '-':
    params.extend(json.loads(sys.stdin.read()))
  else:
    for path in args.arguments:
      with open(path) as file:
        params.extend(json.loads(file.read()))
  params = await _omit_params(client, args, params)
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _handle_rm(client, args):
  arguments = args.arguments
  if not arguments:
    count = await client.queue.remove_queued(None)
  else:
    count = await client.queue.remove_queued(arguments)
  print(f'Removed queued jobs: {count}')

async def _handle_clear(client, args):
  arguments = args.arguments
  if not arguments:
    count = await client.queue.remove_finished(None)
  else:
    count = await client.queue.remove_finished(arguments)
  print(f'Removed finished jobs: {count}')

async def _handle_kill(client, args):
  arguments = args.arguments
  force = False
  if arguments and arguments[0] == 'force':
    force = True
    arguments = arguments[1:]
  if not arguments:
    count = await client.runner.kill(None, force=force)
  else:
    count = await client.runner.kill(arguments, force=force)
  print(f'Killed jobs: {count}')

async def _handle_prune(client, args):
  params = []
  if len(args.arguments) == 1 and args.arguments[0] == '-':
    params.extend(json.loads(sys.stdin.read()))
  else:
    for path in args.arguments:
      with open(path) as file:
        params.extend(json.loads(file.read()))

  if not params:
    raise RuntimeError('Cannot prune without parameters for safety')

  exec_ids = get_exec_ids(params)

  await client.runner.prune_absent(exec_ids)
  await client.history.prune_absent(exec_ids)
  print(f'Pruned')

async def handle_command(client, client2, args):
  '''Handle a client command.'''

  try:
    if args.command == 'start':
      await _handle_start(client, args)

    elif args.command == 'stop':
      await _handle_stop(client, args)

    elif args.command == 'status':
      pass

    elif args.command == 'monitor':
      await _handle_monitor(client, client2, args)

    elif args.command == 'ls':
      await _handle_ls(client, args)

    elif args.command == 'run':
      await _handle_run(client, args)

    elif args.command == 'retry':
      await _handle_retry(client, args)

    elif args.command == 'add':
      await _handle_add(client, args)

    elif args.command == 'rm':
      await _handle_rm(client, args)

    elif args.command == 'clear':
      await _handle_clear(client, args)

    elif args.command == 'kill':
      await _handle_kill(client, args)

    elif args.command == 'prune':
      await _handle_prune(client, args)

    else:
      print(f'Invalid command: {args.command}')
      return 1

    if args.command not in ['stop', 'monitor', 'ls']:
      await _handle_status(client, args)

    return 0

  except Exception: # pylint: disable=broad-except
    traceback.print_exc()
    return 1

def run_client():
  '''Parse arguments and process a client command.'''

  #logging_fmt = '%(asctime)s %(name)-19s %(levelname)-8s %(message)s'
  #logging.basicConfig(format=logging_fmt, level=logging.INFO)
  #logger = logging.getLogger('exptools.run_client')

  parser = argparse.ArgumentParser(description='Control the runner.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-file', type=str, default='secret.json', help='secret file path')
  parser.add_argument('--omit', type=str, default='succeeded,started,queued',
                      help='omit parameters before adding')
  parser.add_argument('--no-omit', action='store_const', dest='omit', const='',
                      help='do not omit parameters')
  parser.add_argument('command', type=str, help='command')
  parser.add_argument('arguments', type=str, nargs='*', help='arguments')

  args = parser.parse_args()

  #logger.info(f'Using secret file at {args.secret_file}')
  secret = json.load(open(args.secret_file))

  loop = asyncio.get_event_loop()

  client = Client(args.host, args.port, secret, loop)
  if args.command in ['monitor']:
    client2 = Client(args.host, args.port, secret, loop)
  else:
    client2 = None
  handle_comamnd_future = asyncio.ensure_future(handle_command(client, client2, args), loop=loop)
  loop.run_until_complete(handle_comamnd_future)
  return handle_comamnd_future.result()
