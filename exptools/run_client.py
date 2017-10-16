'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import io
import json
import pprint
import sys

import termcolor

from exptools.client import Client
from exptools.time import (
    format_elapsed_time_short,
    format_remaining_time_short,
    format_estimated_time,
    )
from exptools.param import get_param_id, get_name

# pylint: disable=too-few-public-methods
class CommandHandler:
  '''Handle a command.'''
  def __init__(self, stdin, stdout, common_args, args, client_pool, loop):
    self.stdin = stdin
    self.stdout = stdout
    self.common_args = common_args
    self.args = args
    self.client_pool = client_pool
    self.loop = loop

    self._init()

  def _init(self):
    command = self.args.command

    if command not in ['c', 'ca', 'cat', 'select'] or self.args.history:
      if 'client' not in self.client_pool:
        secret = json.load(open(self.common_args.secret_file))
        client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
        self.client_pool['client'] = client
      self.client = self.client_pool['client']

    if command in ['monitor']:
      if 'client_watch' not in self.client_pool:
        secret = json.load(open(self.common_args.secret_file))
        client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
        self.client_pool['client_watch'] = client
      self.client_watch = self.client_pool['client_watch']

    self._handler = getattr(self, '_handle_' + command)

  async def handle(self):
    '''Handle a command.'''
    await self._handler()

  async def _read_params(self):
    if not self.args.arguments:
      params = json.loads(self.stdin.read())
    else:
      params = []
      for path in self.args.arguments:
        with open(path) as file:
          params.extend(json.loads(file.read()))

    for param in params:
      if '_' not in param:
        meta = param['_'] = {}
      else:
        meta = param['_']

      if 'param_id' not in meta:
        param_id = get_param_id(param)
        meta['param_id'] = param_id

    if 'history' in self.args and self.args.history:
      param_ids = [param['_']['param_id'] for param in params]
      history = await self.client.history.get_all(param_ids)
      for param in params:
        meta = param['_']
        param_id = meta['param_id']
        if param_id in param_ids:
          meta.update(history.get(param_id, {}))

    return params

  async def _omit_params(self, params):
    '''Omit parameters.'''
    if not self.args.omit:
      return params

    omit = self.args.omit.split(',')
    for entry in omit:
      assert entry in (
          'finished', 'succeeded', 'started', 'queued', 'duplicated',
          ), f'Invalid omission: {entry}'

    if 'finished' in omit:
      params = await self.client.history.omit(params, only_succeeded=False)
    if 'succeeded' in omit:
      params = await self.client.history.omit(params, only_succeeded=True)
    if 'started' in omit:
      params = await self.client.queue.omit(params, queued=False, started=True, finished=False)
    if 'queued' in omit:
      params = await self.client.queue.omit(params, queued=True, started=False, finished=False)
    if 'duplicated' in omit:
      seen_param_ids = set()
      unique_params = []
      for param in params:
        param_id = get_param_id(param)
        if param_id not in seen_param_ids:
          seen_param_ids.add(param_id)
          unique_params.append(param)
      params = unique_params
    return params

  async def _handle_c(self):
    params = await self._read_params()
    for param in params:
      self.stdout.write(f'{get_param_id(param)}  {get_name(param)}\n')

  async def _handle_ca(self):
    params = await self._read_params()
    for param in params:
      self.stdout.write(f'{get_param_id(param)}\n')
      #for line in json.dumps(params, sort_keys=True, indent=2).split('\n'):
      for line in pprint.pformat(param).split('\n'):
        self.stdout.write('  ' + line + '\n')
      self.stdout.write('\n')

  async def _handle_cat(self):
    params = await self._read_params()
    self.stdout.write(json.dumps(params, sort_keys=True, indent=2) + '\n')

  async def _handle_filter(self):
    filter_expr = self.args.filter[0]
    params = await self._read_params()

    params = await self.client.filter.filter(filter_expr, params)
    self.stdout.write(json.dumps(params, sort_keys=True, indent=2) + '\n')

  async def _handle_select(self):
    filter_param_ids = self.args.filter[0].split(',')
    params = await self._read_params()
    params_map = {param['_']['param_id']: param for param in params}

    selected_params = []
    for filter_param_id in filter_param_ids:
      if filter_param_id in params_map:
        # exact match
        selected_params.append(params_map[filter_param_id])
        continue

      # partial match
      selected_param = None
      for param_id in params_map:
        if param_id.startswith(filter_param_id):
          if selected_param is not None:
            raise RuntimeError(f'Ambiguous parameter ID: {param_id}')
          selected_param = params_map[param_id]
      selected_params.append(selected_param)

    params = selected_params
    self.stdout.write(json.dumps(params, sort_keys=True, indent=2) + '\n')

  async def _handle_start(self):
    succeeded = await self.client.scheduler.start()
    self.stdout.write('Scheduler started\n' if succeeded else 'Failed to start scheduler\n')

    await self._handle_stat()

  async def _handle_stop(self):
    succeeded = await self.client.scheduler.stop()
    self.stdout.write('Scheduler stopped\n' if succeeded else 'Failed to stop scheduler\n')

  async def _handle_stat(self):
    queue_state = await self.client.queue.get_state()
    self.stdout.write(await format_estimated_time(self.client.estimator, queue_state) + '\n')

  async def _handle_status(self):
    queue_state = await self.client.queue.get_state()

    show = set(self.args.show.split(','))

    output = ''

    if 'finished' in show:
      output += f"Finished jobs ({len(queue_state['finished_jobs'])}):\n"
      for job in queue_state['finished_jobs']:
        line = f"  {job['job_id']} {job['param_id']}"
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
        line = f"  {job['job_id']} {job['param_id']}"
        rem = await format_remaining_time_short(self.client.estimator, partial_state)
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
        line = f"  {job['job_id']} {job['param_id']}"
        rem = await format_remaining_time_short(self.client.estimator, partial_state)
        line += f' [+{rem}]:'
        line += f" {job['name']}"
        line = termcolor.colored(line, 'blue')
        output += line + '\n'
      output += '\n'

    #output += f"Concurrency: {queue_state['concurrency']}"
    self.stdout.write(output.strip() + '\n')

  async def _handle_monitor(self):
    async for queue_state in self.client_watch.queue.watch_state():
      self.stdout.write(await format_estimated_time(self.client.estimator, queue_state) + '\n')
      if not self.args.forever and \
         not queue_state['started_jobs'] and not queue_state['queued_jobs']:
        break

  async def _handle_run(self):
    params = [{'command': self.args.arguments}]
    job_ids = await self.client.queue.add(params)
    self.stdout.write(f'Added queued jobs: {job_ids[0]}\n')

    await self._handle_stat()

  async def _handle_add(self):
    params = await self._read_params()
    params = await self._omit_params(params)
    job_ids = await self.client.queue.add(params)
    self.stdout.write(f'Added queued jobs: {" ".join(job_ids)}\n')

    await self._handle_stat()

  async def _handle_estimate(self):
    params = await self._read_params()
    params = await self._omit_params(params)

    queue_state = await self.client.queue.get_state()
    self.stdout.write('Current:   ' + \
        await format_estimated_time(self.client.estimator, queue_state) + '\n')

    queue_state['queued_jobs'].extend(
        [{'param_id': get_param_id(param), 'param': param} for param in params])
    self.stdout.write('Estimated: ' + \
        await format_estimated_time(self.client.estimator, queue_state) + '\n')

  async def _handle_retry(self):
    arguments = self.args.arguments
    if not arguments:
      job_ids = await self.client.queue.retry(None)
    else:
      job_ids = await self.client.queue.retry(arguments)
    self.stdout.write(f'Added queued jobs: {" ".join(job_ids)}\n')

    await self._handle_stat()

  async def _handle_rm(self):
    arguments = self.args.arguments
    if not arguments:
      count = await self.client.queue.remove_queued(None)
    else:
      count = await self.client.queue.remove_queued(arguments)
    self.stdout.write(f'Removed queued jobs: {count}\n')

    await self._handle_stat()

  async def _handle_up(self):
    changed_job_ids = set(self.args.arguments)
    queue_state = await self.client.queue.get_state()

    job_ids = [job['job_id'] for job in queue_state['queued_jobs']]
    for i in range(1, len(job_ids)):
      if job_ids[i] in changed_job_ids:
        job_ids[i - 1], job_ids[i] = job_ids[i], job_ids[i - 1]
    count = await self.client.queue.reorder(job_ids)
    self.stdout.write(f'Reordered queued jobs: {count}\n')

  async def _handle_down(self):
    changed_job_ids = set(self.args.arguments)
    queue_state = await self.client.queue.get_state()

    job_ids = [job['job_id'] for job in queue_state['queued_jobs']]
    for i in range(len(job_ids) - 2, -1, -1):
      if job_ids[i] in changed_job_ids:
        job_ids[i], job_ids[i + 1] = job_ids[i + 1], job_ids[i]
    count = await self.client.queue.reorder(job_ids)
    self.stdout.write(f'Reordered queued jobs: {count}\n')

  async def _handle_kill(self):
    arguments = self.args.arguments
    force = False
    if arguments and arguments[0] == 'force':
      force = True
      arguments = arguments[1:]
    if not arguments:
      count = await self.client.runner.kill(None, force=force)
    else:
      count = await self.client.runner.kill(arguments, force=force)
    self.stdout.write(f'Killed jobs: {count}\n')

    await self._handle_stat()

  async def _handle_dismiss(self):
    arguments = self.args.arguments
    if not arguments:
      count = await self.client.queue.remove_finished(None)
    else:
      count = await self.client.queue.remove_finished(arguments)
    self.stdout.write(f'Removed finished jobs: {count}\n')

    await self._handle_stat()

  async def _handle_prune_matching(self):
    params = await self._read_params()
    param_ids = [get_param_id(param) for param in params]

    symlink_count, dir_count = await self.client.runner.prune(param_ids, prune_matching=True)
    entry_count = await self.client.history.prune(param_ids, prune_matching=True)
    self.stdout.write(f'Pruned: {symlink_count} symlinks, ' + \
                      f'{dir_count} output directories, ' + \
                      f'{entry_count} histroy entries\n')

  async def _handle_prune_mismatching(self):
    params = await self._read_params()
    param_ids = [get_param_id(param) for param in params]

    symlink_count, dir_count = await self.client.runner.prune(param_ids, prune_mismatching=True)
    entry_count = await self.client.history.prune(param_ids, prune_mismatching=True)
    self.stdout.write(f'Pruned: {symlink_count} symlinks, ' + \
                      f'{dir_count} output directories, ' + \
                      f'{entry_count} histroy entries\n')

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
    sub_parser.add_argument('--history', action='store_true', dest='history', default=False,
                            help='augment parameters with history data',)
    sub_parser.add_argument('--no-history', action='store_false', dest='history',
                            help='do not augment parameters with history data (default)',
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

  sub_parser = subparsers.add_parser('select', help='select parameters by parameter IDs')
  _add_history(sub_parser)
  sub_parser.add_argument('filter', type=str, nargs=1, help='comma-separated parameter IDs')
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

  return parser

def run_client():
  '''Parse arguments and process a client command.
    Use "I" to pipe commands.
    Use "//" to chain commands without pipe connection.'''
  parser = make_parser()

  argv = sys.argv[1:]

  group_start = -1
  for i, arg in enumerate(argv):
    if not arg.startswith('-'):
      group_start = i
      break

  if group_start == -1:
    parser.parse_args(argv)
    assert False, 'This line must be unreachable'

  # Use a dummy start command to parse the main arguments
  common_args = parser.parse_args(argv[:group_start] + ['start'])

  # Parse sub-arguments
  args_list = []
  pipe_break = []
  for i in range(group_start, len(argv)):
    if argv[i] == 'I' or argv[i] == '//':
      args = parser.parse_args(argv[group_start:i])
      args_list.append(args)
      group_start = i + 1
      if argv[i] == 'I':
        pipe_break.append(False)
      else:
        pipe_break.append(True)
  args = parser.parse_args(argv[group_start:])
  args_list.append(args)
  pipe_break.append(True)

  # Construct and run chain
  loop = asyncio.get_event_loop()

  client_pool = {}
  stdin = sys.stdin
  stdout = io.StringIO()
  for i, args in enumerate(args_list):
    # Run a handler
    handler = CommandHandler(stdin, stdout, common_args, args, client_pool, loop)
    handle_future = asyncio.ensure_future(handler.handle(), loop=loop)
    loop.run_until_complete(handle_future)

    if pipe_break[i]:
      # Spill output
      stdout.seek(0)
      sys.stdout.write(stdout.read())
      stdin = io.StringIO()
      stdin.close()
    else:
      # Connect stdout to stdin
      stdout.seek(0)
      stdin = stdout

    stdout = io.StringIO()
