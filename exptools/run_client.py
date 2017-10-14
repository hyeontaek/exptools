'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import json
#import logging
import sys

import termcolor

from exptools.client import Client
#from exptools.pretty import pprint
from exptools.time import (
    format_elapsed_time_short,
    format_remaining_time_short,
    format_estimated_time,
    )


# pylint: disable=unused-argument
async def _handle_start(client, args):
  await client.runner.start()

# pylint: disable=unused-argument
async def _handle_stop(client, args):
  await client.runner.stop()

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()
  print(await format_estimated_time(client.estimator, queue_state))

async def _handle_ls(client, args):
  queue_state = await client.queue.get_state()

  if not args.argument:
    show = set(['finished', 'started', 'queued'])
  else:
    show = set(args.argument)

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
  params = [{'cmd': args.argument}]
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {job_ids[0]}')

async def _handle_retry(client, args):
  argument = args.argument
  if not argument:
    job_ids = await client.queue.retry(None)
  else:
    job_ids = await client.queue.retry(argument)
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
  if len(args.argument) == 1 and args.argument[0] == '-':
    params.extend(json.loads(sys.stdin.read()))
  else:
    for path in args.argument:
      with open(path) as file:
        params.extend(json.loads(file.read()))
  params = await _omit_params(client, args, params)
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _handle_rm(client, args):
  argument = args.argument
  if not argument:
    count = await client.queue.remove_queued(None)
  else:
    count = await client.queue.remove_queued(argument)
  print(f'Removed queued jobs: {count}')

async def _handle_clear(client, args):
  argument = args.argument
  if not argument:
    count = await client.queue.remove_finished(None)
  else:
    count = await client.queue.remove_finished(argument)
  print(f'Removed finished jobs: {count}')

async def _handle_kill(client, args):
  argument = args.argument
  force = False
  if argument and argument[0] == 'force':
    force = True
    argument = argument[1:]
  if not argument:
    count = await client.runner.kill(None, force=force)
  else:
    count = await client.runner.kill(argument, force=force)
  print(f'Killed jobs: {count}')

async def handle_command(client, args):
  '''Handle a client command.'''

  if args.command == 'start':
    await _handle_start(client, args)

  elif args.command == 'stop':
    await _handle_stop(client, args)

  elif args.command == 'status':
    pass

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

  else:
    raise RuntimeError(f'Invalid command: {args[0]}')

  if args.command not in ['stop', 'ls']:
    await _handle_status(client, args)

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
  parser.add_argument('argument', type=str, nargs='*', help='arguments')

  args = parser.parse_args()

  #logger.info(f'Using secret file at {args.secret_file}')
  secret = json.load(open(args.secret_file))

  loop = asyncio.get_event_loop()

  client = Client(args.host, args.port, secret, loop)
  loop.run_until_complete(asyncio.ensure_future(handle_command(client, args), loop=loop))

if __name__ == '__main__':
  run_client()
