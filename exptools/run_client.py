'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import json
import pprint
import sys
import traceback

import termcolor

from exptools.client import Client
from exptools.time import (
    format_elapsed_time_short,
    format_remaining_time_short,
    format_estimated_time,
    )
from exptools.param import get_exec_id, get_exec_ids, get_param_id, get_name


# pylint: disable=unused-argument
async def _handle_start(client, args):
  await client.runner.start()

# pylint: disable=unused-argument
async def _handle_stop(client, args):
  await client.runner.stop()

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()
  print(await format_estimated_time(client.estimator, queue_state))

async def _handle_monitor(client, client_watch, args):
  forever = False
  if args.arguments and args.arguments[0] == 'forever':
    forever = True

  async for queue_state in client_watch.queue.watch_state():
    print(await format_estimated_time(client.estimator, queue_state))
    if not forever and \
       not queue_state['started_jobs'] and not queue_state['queued_jobs']:
      break

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
      partial_state['queued_jobs'].append(job)
      line = f"  {job['job_id']} {job['exec_id']}"
      rem = await format_remaining_time_short(client.estimator, partial_state)
      line += f' [+{rem}]:'
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
  if not args.omit:
    return params

  omit = args.omit.split(',')
  for entry in omit:
    assert entry in (
        'finished', 'succeeded', 'started', 'queued', 'duplicated',
        ), f'Invalid omission: {entry}'

  if 'finished' in omit:
    params = await client.history.omit(params, only_succeeded=False)
  if 'succeeded' in omit:
    params = await client.history.omit(params, only_succeeded=True)
  if 'started' in omit:
    params = await client.queue.omit(params, queued=False, started=True, finished=False)
  if 'queued' in omit:
    params = await client.queue.omit(params, queued=True, started=False, finished=False)
  if 'duplicated' in omit:
    seen_exec_ids = set()
    unique_params = []
    for param in params:
      exec_id = get_exec_id(param)
      if exec_id not in seen_exec_ids:
        seen_exec_ids.add(exec_id)
        unique_params.append(param)
    params = unique_params
  return params

async def _read_params(client, args, skip=0):
  if len(args.arguments) <= skip:
    params = json.loads(sys.stdin.read())
  else:
    params = []
    for path in args.arguments[skip:]:
      with open(path) as file:
        params.extend(json.loads(file.read()))

  if args.history:
    exec_ids = set()
    for param in params:
      param['_exptools'] = {}
      param['_exptools']['param_id'] = get_param_id(param)
      exec_id = get_exec_id(param)
      param['_exptools']['exec_id'] = exec_id
      exec_ids.add(exec_id)

    if exec_ids:
      history = await client.history.get_all(list(exec_ids))
      for param in params:
        exec_id = param['_exptools']['exec_id']
        if exec_id in exec_ids:
          param['_exptools'].update(history.get(exec_id, {}))

  return params

async def _handle_c(client, args):
  params = await _read_params(client, args)
  for param in params:
    print(f'{get_exec_id(param)}  {get_name(param)}')

async def _handle_ca(client, args):
  params = await _read_params(client, args)
  for param in params:
    print(f'{get_exec_id(param)}')
    #for line in json.dumps(params, sort_keys=True, indent=2).split('\n'):
    for line in pprint.pformat(param).split('\n'):
      print('  ' + line)

async def _handle_cat(client, args):
  params = await _read_params(client, args)
  print(json.dumps(params, sort_keys=True, indent=2))

async def _handle_filter(client, args):
  filter_expr = args.arguments[0]
  params = await _read_params(client, args, skip=1)

  params = await client.filter.filter(filter_expr, params)
  print(json.dumps(params, sort_keys=True, indent=2))

async def _handle_add(client, args):
  params = await _read_params(client, args)
  params = await _omit_params(client, args, params)
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _handle_up(client, args):
  changed_job_ids = set(args.arguments)
  queue_state = await client.queue.get_state()

  job_ids = [job['job_id'] for job in queue_state['queued_jobs']]
  for i in range(1, len(job_ids)):
    if job_ids[i] in changed_job_ids:
      job_ids[i - 1], job_ids[i] = job_ids[i], job_ids[i - 1]
  count = await client.queue.reorder(job_ids)
  print(f'Reordered queued jobs: {count}')

async def _handle_down(client, args):
  changed_job_ids = set(args.arguments)
  queue_state = await client.queue.get_state()

  job_ids = [job['job_id'] for job in queue_state['queued_jobs']]
  for i in range(len(job_ids) - 2, -1, -1):
    if job_ids[i] in changed_job_ids:
      job_ids[i], job_ids[i + 1] = job_ids[i + 1], job_ids[i]
  count = await client.queue.reorder(job_ids)
  print(f'Reordered queued jobs: {count}')

async def _handle_rm(client, args):
  arguments = args.arguments
  if not arguments:
    count = await client.queue.remove_queued(None)
  else:
    count = await client.queue.remove_queued(arguments)
  print(f'Removed queued jobs: {count}')

async def _handle_dismiss(client, args):
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

async def _handle_prune_matching(client, args):
  params = await _read_params(client, args)
  exec_ids = get_exec_ids(params)

  symlink_count, dir_count = await client.runner.prune(exec_ids, prune_matching=True)
  entry_count = await client.history.prune(exec_ids, prune_matching=True)
  print(f'Pruned: {symlink_count} symlinks, ' + \
        f'{dir_count} output directories, ' + \
        f'{entry_count} histroy entries')

async def _handle_prune_mismatching(client, args):
  params = await _read_params(client, args)
  exec_ids = get_exec_ids(params)

  symlink_count, dir_count = await client.runner.prune(exec_ids, prune_mismatching=True)
  entry_count = await client.history.prune(exec_ids, prune_mismatching=True)
  print(f'Pruned: {symlink_count} symlinks, ' + \
        f'{dir_count} output directories, ' + \
        f'{entry_count} histroy entries')

async def handle_command(client, client_watch, args):
  '''Handle a client command.'''

  try:
    if args.command == 'start':
      await _handle_start(client, args)
      await _handle_status(client, args)

    elif args.command == 'stop':
      await _handle_stop(client, args)

    elif args.command == 'status':
      await _handle_status(client, args)

    elif args.command == 'monitor':
      await _handle_monitor(client, client_watch, args)

    elif args.command == 'ls':
      await _handle_ls(client, args)

    elif args.command == 'run':
      await _handle_run(client, args)
      await _handle_status(client, args)

    elif args.command == 'retry':
      await _handle_retry(client, args)
      await _handle_status(client, args)

    elif args.command == 'add':
      await _handle_add(client, args)
      await _handle_status(client, args)

    elif args.command == 'up':
      await _handle_up(client, args)

    elif args.command == 'down':
      await _handle_down(client, args)

    elif args.command == 'rm':
      await _handle_rm(client, args)
      await _handle_status(client, args)

    elif args.command == 'dismiss':
      await _handle_dismiss(client, args)
      await _handle_status(client, args)

    elif args.command == 'kill':
      await _handle_kill(client, args)
      await _handle_status(client, args)

    elif args.command == 'prune-matching':
      await _handle_prune_matching(client, args)

    elif args.command == 'prune-mismatching':
      await _handle_prune_mismatching(client, args)

    elif args.command == 'c':
      await _handle_c(client, args)

    elif args.command == 'ca':
      await _handle_ca(client, args)

    elif args.command == 'cat':
      await _handle_cat(client, args)

    elif args.command == 'filter':
      await _handle_filter(client, args)

    else:
      print(f'Invalid command: {args.command}')
      return 1

    return 0

  except Exception: # pylint: disable=broad-except
    traceback.print_exc()
    return 1

def run_client():
  '''Parse arguments and process a client command.'''

  parser = argparse.ArgumentParser(description='Control the runner.')
  parser.add_argument('--host', type=str, default='localhost', help='hostname')
  parser.add_argument('--port', type=int, default='31234', help='port')
  parser.add_argument('--secret-file', type=str, default='secret.json', help='secret file path')
  parser.add_argument('--omit', type=str, default='succeeded,started,queued,duplicated',
                      help='omit parameters before adding')
  parser.add_argument('--no-omit', action='store_const', dest='omit', const='',
                      help='do not omit parameters')
  parser.add_argument('--history', action='store_true', dest='history', default=True,
                      help='augment history data to parameters')
  parser.add_argument('--no-history', action='store_false', dest='history',
                      help='do not augment history data to parameters')
  parser.add_argument('command', type=str, help='command')
  parser.add_argument('arguments', type=str, nargs='*', help='arguments')

  args = parser.parse_args()

  #logger.info(f'Using secret file at {args.secret_file}')
  secret = json.load(open(args.secret_file))

  loop = asyncio.get_event_loop()

  if args.command in ['c', 'ca', 'cat'] and not args.history:
    client = None
  else:
    client = Client(args.host, args.port, secret, loop)

  if args.command in ['monitor']:
    client_watch = Client(args.host, args.port, secret, loop)
  else:
    client_watch = None

  handle_command_future = asyncio.ensure_future(
      handle_command(client, client_watch, args), loop=loop)
  loop.run_until_complete(handle_command_future)
  return handle_command_future.result()

if __name__ == '__main__':
  run_client()
