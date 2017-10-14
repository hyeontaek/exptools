'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import json
#import logging

import termcolor

from exptools.client import Client
from exptools.estimator import Estimator
#from exptools.pretty import pprint
from exptools.time import diff_sec, utcnow, parse_utc

# pylint: disable=unused-argument
async def _handle_start(client, args):
  await client.runner.start()

# pylint: disable=unused-argument
async def _handle_stop(client, args):
  await client.runner.stop()

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()

  if not args.argument:
    show = set(['finished', 'started', 'queued'])
  else:
    show = set(args.argument)

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

  output = ''

  if 'finished' in show:
    output += f"Finished jobs ({len(queue_state['finished_jobs'])}):\n"
    for job in queue_state['finished_jobs']:
      line = f"  {job['job_id']} {job['exec_id']}"
      line += f' [{_format_elapsed(job)}]'
      if job['succeeded']:
        line += ' succeeded:'
      else:
        line += ' FAILED:'
      line += f" {job['name']}"
      if not job['succeeded']:
        line = termcolor.colored(line, 'red')
      output += line + '\n'

    output += '\n'

  partial_state = {'started_jobs': [], 'queued_jobs': [],
                   'concurrency': queue_state['concurrency']}

  if 'started' in show:
    output += f"Started jobs ({len(queue_state['started_jobs'])}):\n"
    for job in queue_state['started_jobs']:
      partial_state['started_jobs'].append(job)
      line = f"  {job['job_id']} {job['exec_id']}"
      rem = await _format_remaining(partial_state)
      line += f' [{_format_elapsed(job)}+{rem}]:'
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
      rem = await _format_remaining(partial_state)
      line += f' [{_format_elapsed(job)}+{rem}]:'
      line += f" {job['name']}"
      line = termcolor.colored(line, 'blue')
      output += line + '\n'

  #output += '\n'
  #output += f"Concurrency: {queue_state['concurrency']}"
  print(output)

async def _handle_add(client, args):
  param = {'cmd': args.argument}
  job_ids = await client.queue.add([param])
  print(f'New job: {job_ids[0]}')

async def _handle_retry(client, args):
  argument = args.argument
  if not argument:
    job_ids = await client.queue.retry(None)
  else:
    job_ids = await client.queue.retry(argument)
  print(f'New job: {job_ids[0]}')

async def _handle_rm(client, args):
  argument = args.argument
  if not argument:
    count = await client.queue.remove_queued(None)
  else:
    count = await client.queue.remove_queued(argument)
  print(f'Removed {count} jobs')

async def _handle_clear(client, args):
  argument = args.argument
  if not argument:
    count = await client.queue.remove_finished(None)
  else:
    count = await client.queue.remove_finished(argument)
  print(f'Removed {count} jobs')

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
  print(f'Killed {count} jobs')

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
  elif args.command == 'retry':
    await _handle_retry(client, args)
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

  #logging_fmt = '%(asctime)s %(name)-19s %(levelname)-8s %(message)s'
  #logging.basicConfig(format=logging_fmt, level=logging.INFO)
  #logger = logging.getLogger('exptools.run_client')

  parser = argparse.ArgumentParser(description='Control the runner.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-file', type=str, default='secret.json', help='secret file path')
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
