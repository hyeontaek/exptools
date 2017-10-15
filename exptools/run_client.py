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
from exptools.param import get_exec_id, get_param_id, get_name

async def _read_params(client, args):
  if not args.arguments:
    params = json.loads(sys.stdin.read())
  else:
    params = []
    for path in args.arguments:
      with open(path) as file:
        params.extend(json.loads(file.read()))

  if 'history' in args and args.history:
    exec_ids = set()
    for param in params:
      if '_' not in param:
        meta = param['_'] = {}
      else:
        meta = param['_']

      if 'exec_id' not in meta:
        meta['param_id'] = get_param_id(param)
        exec_id = get_exec_id(param)
        meta['exec_id'] = exec_id

        exec_ids.add(exec_id)

    if exec_ids:
      history = await client.history.get_all(list(exec_ids))
      for param in params:
        meta = param['_']
        exec_id = meta['exec_id']
        if exec_id in exec_ids:
          meta.update(history.get(exec_id, {}))

  return params

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
    print()

async def _handle_cat(client, args):
  params = await _read_params(client, args)
  print(json.dumps(params, sort_keys=True, indent=2))

async def _handle_filter(client, args):
  filter_expr = args.filter[0]
  params = await _read_params(client, args)

  params = await client.filter.filter(filter_expr, params)
  print(json.dumps(params, sort_keys=True, indent=2))

# pylint: disable=unused-argument
async def _handle_start(client, args):
  await client.scheduler.start()

# pylint: disable=unused-argument
async def _handle_stop(client, args):
  await client.scheduler.stop()

async def _handle_stat(client, args):
  queue_state = await client.queue.get_state()
  print(await format_estimated_time(client.estimator, queue_state))

async def _handle_status(client, args):
  queue_state = await client.queue.get_state()

  show = set(args.show.split(','))

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

async def _handle_monitor(client, client_watch, args):
  async for queue_state in client_watch.queue.watch_state():
    print(await format_estimated_time(client.estimator, queue_state))
    if not args.forever and \
       not queue_state['started_jobs'] and not queue_state['queued_jobs']:
      break

async def _handle_run(client, args):
  params = [{'command': args.arguments}]
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {job_ids[0]}')

async def _handle_add(client, args):
  params = await _read_params(client, args)
  params = await _omit_params(client, args, params)
  job_ids = await client.queue.add(params)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _handle_estimate(client, args):
  params = await _read_params(client, args)
  params = await _omit_params(client, args, params)

  queue_state = await client.queue.get_state()
  print('Current:   ' + await format_estimated_time(client.estimator, queue_state))

  queue_state['queued_jobs'].extend(
      [{'exec_id': get_exec_id(param), 'param': param} for param in params])
  print('Estimated: ' + await format_estimated_time(client.estimator, queue_state))

async def _handle_retry(client, args):
  arguments = args.arguments
  if not arguments:
    job_ids = await client.queue.retry(None)
  else:
    job_ids = await client.queue.retry(arguments)
  print(f'Added queued jobs: {" ".join(job_ids)}')

async def _handle_rm(client, args):
  arguments = args.arguments
  if not arguments:
    count = await client.queue.remove_queued(None)
  else:
    count = await client.queue.remove_queued(arguments)
  print(f'Removed queued jobs: {count}')

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

async def _handle_dismiss(client, args):
  arguments = args.arguments
  if not arguments:
    count = await client.queue.remove_finished(None)
  else:
    count = await client.queue.remove_finished(arguments)
  print(f'Removed finished jobs: {count}')

async def _handle_prune_matching(client, args):
  params = await _read_params(client, args)
  exec_ids = [get_exec_id(param) for param in params]

  symlink_count, dir_count = await client.runner.prune(exec_ids, prune_matching=True)
  entry_count = await client.history.prune(exec_ids, prune_matching=True)
  print(f'Pruned: {symlink_count} symlinks, ' + \
        f'{dir_count} output directories, ' + \
        f'{entry_count} histroy entries')

async def _handle_prune_mismatching(client, args):
  params = await _read_params(client, args)
  exec_ids = [get_exec_id(param) for param in params]

  symlink_count, dir_count = await client.runner.prune(exec_ids, prune_mismatching=True)
  entry_count = await client.history.prune(exec_ids, prune_mismatching=True)
  print(f'Pruned: {symlink_count} symlinks, ' + \
        f'{dir_count} output directories, ' + \
        f'{entry_count} histroy entries')

async def handle_command(client, client_watch, args):
  '''Handle a client command.'''

  try:
    if args.command == 'c':
      await _handle_c(client, args)

    elif args.command == 'ca':
      await _handle_ca(client, args)

    elif args.command == 'cat':
      await _handle_cat(client, args)

    elif args.command == 'filter':
      await _handle_filter(client, args)

    elif args.command == 'start':
      await _handle_start(client, args)
      await _handle_stat(client, args)

    elif args.command == 'stop':
      await _handle_stop(client, args)

    elif args.command == 'stat':
      await _handle_stat(client, args)

    elif args.command == 'status':
      await _handle_status(client, args)

    elif args.command == 'monitor':
      await _handle_monitor(client, client_watch, args)

    elif args.command == 'run':
      await _handle_run(client, args)
      await _handle_stat(client, args)

    elif args.command == 'add':
      await _handle_add(client, args)
      await _handle_stat(client, args)

    elif args.command == 'estimate':
      await _handle_estimate(client, args)

    elif args.command == 'retry':
      await _handle_retry(client, args)
      await _handle_stat(client, args)

    elif args.command == 'rm':
      await _handle_rm(client, args)
      await _handle_stat(client, args)

    elif args.command == 'up':
      await _handle_up(client, args)

    elif args.command == 'down':
      await _handle_down(client, args)

    elif args.command == 'kill':
      await _handle_kill(client, args)
      await _handle_stat(client, args)

    elif args.command == 'dismiss':
      await _handle_dismiss(client, args)
      await _handle_stat(client, args)

    elif args.command == 'prune-matching':
      await _handle_prune_matching(client, args)

    elif args.command == 'prune-mismatching':
      await _handle_prune_mismatching(client, args)

    else:
      print(f'Invalid command: {args.command}')
      return 1

    return 0

  except Exception: # pylint: disable=broad-except
    traceback.print_exc()
    return 1

def make_parser():
  '''Return a new argument parser.'''
  parser = argparse.ArgumentParser(description='Interact with the exptools server.')

  parser.add_argument('--host', type=str, default='localhost',
                      help='the hostname of the server (default: %(default)s)')
  parser.add_argument('--port', type=int, default='31234',
                      help='the port number of the server (default: %(default)s)')
  parser.add_argument('--secret-file', type=str,
                      default='secret.json', help='the secret file path (default: %(default)s)')

  def _add_read_params_argument(sub_parser):
    sub_parser.add_argument('arguments', type=str, nargs='*',
                            help='path to json files containing parameters; ' + \
                                 'leave empty to read from standard input')

  def _add_job_ids(sub_parser):
    sub_parser.add_argument('arguments', type=str, nargs='+', help='job IDs')

  def _add_job_ids_auto_select_all(sub_parser):
    sub_parser.add_argument('arguments', type=str, nargs='*',
                            help='job IDs; leave empty to select all jobs')

  def _add_history(sub_parser):
    sub_parser.add_argument('--history', action='store_true', dest='history', default=True,
                            help='augment parameters with history data (default)',)
    sub_parser.add_argument('--no-history', action='store_false', dest='history',
                            help='do not augment parameters with history data',
                            default=argparse.SUPPRESS)

  def _add_omit(sub_parser):
    sub_parser.add_argument('--omit', type=str, default='succeeded,started,queued,duplicated',
                            help='omit parameters before adding if existing matches are found ' + \
                                 '(default: %(default)s)')
    sub_parser.add_argument('--no-omit', action='store_const', dest='omit', const='',
                            help='do not omit parameters',
                            default=argparse.SUPPRESS)

  subparsers = parser.add_subparsers(dest='command')

  sub_parser = subparsers.add_parser('c', help='summarize parameters concisely')
  _add_history(sub_parser)
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('ca', help='summarize parameters')
  _add_history(sub_parser)
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('cat', help='dump parameters')
  _add_history(sub_parser)
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('filter', help='filter parameters')
  _add_history(sub_parser)
  sub_parser.add_argument('filter', type=str, nargs=1, help='YAML expression')
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('start', help='start the scheduler')

  sub_parser = subparsers.add_parser('stop', help='stop the scheduler')

  sub_parser = subparsers.add_parser('stat', help='summarize the queue state')

  sub_parser = subparsers.add_parser('status', help='show the queue state')
  sub_parser.add_argument('--show', type=str, default='finished,started,queued',
                          help='specify job types to show (default: %(default)s)')

  sub_parser = subparsers.add_parser('monitor', help='monitor the queue state')
  sub_parser.add_argument('--forever', action='store_true', dest='forever', default=True,
                          help='run forever (default)')
  sub_parser.add_argument('--no-forever', action='store_false', dest='forever',
                          default=argparse.SUPPRESS, help='stop upon empty queue')

  sub_parser = subparsers.add_parser('run', help='run an adhoc command')
  sub_parser.add_argument('arguments', type=str, nargs='+', help='adhoc command')

  sub_parser = subparsers.add_parser('add', help='add parameters to the queue')
  _add_omit(sub_parser)
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('estimate', help='estimate execution time for parameters')
  _add_omit(sub_parser)
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('retry', help='retry finished jobs')
  sub_parser.add_argument('arguments', type=str, nargs='*',
                          help='job IDs; leave empty to select the last finished job')

  sub_parser = subparsers.add_parser('rm', help='remove queued jobs')
  _add_job_ids_auto_select_all(sub_parser)

  sub_parser = subparsers.add_parser('up', help='priorize queued jobs')
  _add_job_ids(sub_parser)

  sub_parser = subparsers.add_parser('down', help='depriorize queued jobs')
  _add_job_ids(sub_parser)

  sub_parser = subparsers.add_parser('kill', help='kill started jobs')
  _add_job_ids_auto_select_all(sub_parser)

  sub_parser = subparsers.add_parser('dismiss', help='clear finished jobs')
  _add_job_ids_auto_select_all(sub_parser)

  sub_parser = subparsers.add_parser('prune-matching',
                                     help='prune output data that match parameters')
  _add_read_params_argument(sub_parser)

  sub_parser = subparsers.add_parser('prune-mismatching',
                                     help='prune output data that do not match parameters')
  _add_read_params_argument(sub_parser)


  #  elif args.command == 'estimate':

  return parser


def run_client():
  '''Parse arguments and process a client command.'''
  args = make_parser().parse_args()

  secret = json.load(open(args.secret_file))

  loop = asyncio.get_event_loop()

  if args.command in ['c', 'ca', 'cat', 'filter'] and not args.history:
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
