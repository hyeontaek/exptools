'''Handle client commands.'''

__all__ = ['run_client']

import asyncio
import argparse
import base64
import collections
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

_ARG_EXPORTS = collections.OrderedDict()

def arg_export(name):
  '''Export an argument set.'''
  def _wrapper(func):
    _ARG_EXPORTS[name] = func
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    return func
  return _wrapper

def arg_import(name):
  '''Import an argument set.'''
  def _wrapper(func):
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    func.arg_defs.insert(0, ('import', name))
    return func
  return _wrapper

def arg_define(*args, **kwargs):
  '''Add an argument option.'''
  def _wrapper(func):
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    func.arg_defs.insert(0, ('add', args, kwargs))
    return func
  return _wrapper

def arg_add_options(parser, func):
  '''Add argument options defined for the function to the parser.'''
  for arg_def in func.arg_defs:
    if arg_def[0] == 'import':
      arg_add_options(parser, _ARG_EXPORTS[arg_def[1]])
    elif arg_def[0] == 'add':
      parser.add_argument(*arg_def[1], **arg_def[2])
    else:
      assert False

# pylint: disable=too-few-public-methods
class CommandHandler:
  '''Handle a command.'''
  def __init__(self, stdin, stdout, common_args, args, unknown_args, client_pool, loop):
    self.stdin = stdin
    self.stdout = stdout
    self.common_args = common_args
    self.args = args
    self.unknown_args = unknown_args
    self.client_pool = client_pool
    self.loop = loop

    self._init()

  def _init(self):
    command = self.args.command

    if command not in ['d', 'dum', 'dump', 'select'] or self.args.history:
      if 'client' not in self.client_pool:
        secret = json.load(open(self.common_args.secret_file))
        client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
        self.client_pool['client'] = client
      self.client = self.client_pool['client']

    if 'follow' in self.args and self.args.follow:
      if 'client_watch' not in self.client_pool:
        secret = json.load(open(self.common_args.secret_file))
        client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
        self.client_pool['client_watch'] = client
      self.client_watch = self.client_pool['client_watch']

    self._handler = _ARG_EXPORTS['command_' + command]

  async def handle(self):
    '''Handle a command.'''
    await self._handler(self)

  @arg_export('common_read_params')
  @arg_define('params_files', type=str, nargs='*',
              help='path to json files containing parameters; ' + \
                   'leave empty to read from standard input')
  @arg_define('-p', '--param-id', action='store_true', default=False,
              help='augment parameters with parameter IDs')
  @arg_define('-H', '--history', action='store_true', default=False,
              help='augment parameters with history data')
  async def _read_params(self):
    if not self.args.params_files:
      params = json.loads(self.stdin.read())
    else:
      params = []
      for path in self.args.params_files:
        if path == '-':
          params.extend(json.loads(self.stdin.read()))
        else:
          with open(path) as file:
            params.extend(json.loads(file.read()))

    if self.args.param_id:
      for param in params:
        if '_' not in param:
          meta = param['_'] = {}
        else:
          meta = param['_']

        if 'param_id' not in meta:
          param_id = get_param_id(param)
          meta['param_id'] = param_id

    if self.args.history:
      param_ids = set()
      for param in params:
        if '_' not in param:
          meta = param['_'] = {}
        else:
          meta = param['_']

        if 'succeeded' not in meta:
          param_ids.add(get_param_id(param))

      history = await self.client.history.get_all(list(param_ids))

      for param in params:
        param_id = get_param_id(param)
        if param_id in param_ids:
          param['_'].update(history[param_id])

    return params

  @arg_export('common_omit_params')
  @arg_define('-o', '--omit', action='append',
              choices=['succeeded', 'finished', 'started', 'queued', 'duplicate'],
              help='omit specified parameter types')
  async def _omit_params(self, params):
    '''Omit parameters.'''

    if self.args.omit and 'finished' in self.args.omit:
      params = await self.client.history.omit(params, only_succeeded=False)
    if self.args.omit and 'succeeded' in self.args.omit:
      params = await self.client.history.omit(params, only_succeeded=True)
    if self.args.omit and 'started' in self.args.omit:
      params = await self.client.queue.omit(params, queued=False, started=True, finished=False)
    if self.args.omit and 'queued' in self.args.omit:
      params = await self.client.queue.omit(params, queued=True, started=False, finished=False)
    if self.args.omit and 'duplicate' in self.args.omit:
      seen_param_ids = set()
      unique_params = []
      for param in params:
        param_id = get_param_id(param)
        if param_id not in seen_param_ids:
          seen_param_ids.add(param_id)
          unique_params.append(param)
      params = unique_params
    return params

  @arg_export('common_get_queue_state')
  @arg_define('-f', '--follow', action='store_true', default=False, help='follow state changes')
  @arg_define('-s', '--stop-empty', action='store_true', default=False,
              help='stop upon empty queue')
  async def _get_queue_state(self):
    if self.args.follow:
      async for queue_state in self.client_watch.queue.watch_state():
        yield queue_state
    else:
      yield await self.client.queue.get_state()

  @arg_export('common_get_job_ids')
  @arg_define('job_ids', type=str, nargs='*', help='job IDs; use "last" to select the last job')
  @arg_define('-a', '--all', action='store_true', default=False, help='select all jobs')
  async def _get_job_ids(self, job_type):
    job_ids = []

    for job_id in self.args.job_ids:
      if job_id == 'last':
        queue_state = await self.client.queue.get_state()
        if not queue_state[job_type]:
          raise RuntimeError(f'No last job found')
        job_ids.append(queue_state[job_type][-1]['job_id'])
      else:
        job_ids.append(job_id)

    if self.args.all:
      queue_state = await self.client.queue.get_state()
      job_ids.extend([job['job_id'] for job in queue_state[job_type]])

    return job_ids

  @arg_export('common_get_job_or_param_id')
  @arg_define('job_or_param_id', type=str,
              help='a job or parameter ID; use "last" to select the last started or finished job')
  async def _get_job_or_param_id(self):
    queue_state = await self.client.queue.get_state()
    if self.args.job_or_param_id == 'last':
      if queue_state['started_jobs']:
        job_id = queue_state['started_jobs'][-1]['job_id']
      elif queue_state['finished_jobs']:
        job_id = queue_state['finished_jobs'][-1]['job_id']
      else:
        raise RuntimeError(f'No last job found')
      return job_id

    if self.args.job_or_param_id.startswith('p-'):
      job_id = (await self.client.history.get(self.args.job_or_param_id))['job_id']
      if job_id is None:
        raise RuntimeError(f'No job found for parameter {self.args.job_or_param_id}')
    elif self.args.job_or_param_id.startswith('j-'):
      job_id = self.args.job_or_param_id
    else:
      raise RuntimeError(f'Invalid job or parameter {self.args.job_or_param_id}')
    return job_id

  @arg_export('common_estimate')
  @arg_define('-e', '--estimate', action='store_true', default=False,
              help='estimate execution time instead of adding')
  async def _estimate(self, params):
    queue_state = await self.client.queue.get_state()
    oneshot = await self.client.scheduler.is_oneshot()
    self.stdout.write('Current:   ' + \
        await format_estimated_time(self.client.estimator, queue_state, oneshot) + '\n')

    queue_state['queued_jobs'].extend(
        [{'param_id': get_param_id(param), 'param': param} for param in params])
    oneshot = await self.client.scheduler.is_oneshot()
    self.stdout.write('Estimated: ' + \
        await format_estimated_time(self.client.estimator, queue_state, oneshot) + '\n')

  @arg_export('common_get_stdout_stderr')
  @arg_define('-o', '--stdout', action='store_true', default=True,
              help='read stdout (default)')
  @arg_define('-e', '--stderr', action='store_false', dest='stdout',
              help='read stderr instead of stdout', default=argparse.SUPPRESS)
  async def _get_stdout_stderr(self):
    return self.args.stdout

  @arg_export('command_d')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  async def _handle_d(self):
    '''summarize parameters'''
    params = await self._read_params()
    params = await self._omit_params(params)
    for param in params:
      self.stdout.write(f'{get_param_id(param)}  {get_name(param)}\n')

  @arg_export('command_dum')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  async def _handle_dum(self):
    '''dump parameters in Python'''
    params = await self._read_params()
    params = await self._omit_params(params)
    for param in params:
      self.stdout.write(f'{get_param_id(param)}\n')
      for line in pprint.pformat(param).split('\n'):
        self.stdout.write('  ' + line + '\n')
      self.stdout.write('\n')

  @arg_export('command_dump')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  async def _handle_dump(self):
    '''dump parameters in JSON'''
    params = await self._read_params()
    params = await self._omit_params(params)
    self.stdout.write(json.dumps(params, sort_keys=True, indent=2) + '\n')

  @arg_export('command_filter')
  @arg_define('filter', type=str, help='YAQL expression')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  async def _handle_filter(self):
    '''filter parameters using YAQL'''
    filter_expr = self.args.filter
    params = await self._read_params()
    params = await self._omit_params(params)

    params = await self.client.filter.filter(filter_expr, params)
    self.stdout.write(json.dumps(params, sort_keys=True, indent=2) + '\n')

  @arg_export('command_select')
  @arg_define('filter', type=str,
              help='comma-separated parameter IDs; partial matches are supported')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  async def _handle_select(self):
    '''select parameters by parameter IDs'''
    filter_param_ids = self.args.filter.split(',')
    params = await self._read_params()
    params = await self._omit_params(params)
    params_map = {get_param_id(param): param for param in params}

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

  @arg_export('command_start')
  async def _handle_start(self):
    '''start the scheduler'''
    succeeded = await self.client.scheduler.start()
    self.stdout.write('Scheduler started\n' if succeeded else 'Failed to start scheduler\n')

  @arg_export('command_stop')
  async def _handle_stop(self):
    '''stop the scheduler'''
    succeeded = await self.client.scheduler.stop()
    self.stdout.write('Scheduler stopped\n' if succeeded else 'Failed to stop scheduler\n')

  @arg_export('command_oneshot')
  async def _handle_oneshot(self):
    '''schedule only one job and stop'''
    succeeded = await self.client.scheduler.set_oneshot()
    self.stdout.write('Scheduler oneshot mode set\n' \
                      if succeeded else 'Failed to set oneshot mode\n')

  @arg_export('command_resource')
  @arg_define('operation', type=str, choices=['add', 'rm'], help='operation')
  @arg_define('key', type=str, help='resource key')
  @arg_define('value', type=str, help='resource value')
  async def _handle_resource(self):
    '''add or remove a resource'''
    if self.args.operation == 'add':
      succeeded = await self.client.scheduler.add_resource(self.args.key, self.args.value)
    elif self.args.operation == 'remove':
      succeeded = await self.client.scheduler.remove_resource(self.args.key, self.args.value)
    else:
      assert False
    self.stdout.write('Resource updated\n' if succeeded else 'Failed to update resource\n')

  @arg_export('command_s')
  @arg_import('common_get_queue_state')
  async def _handle_s(self):
    '''summarize the queue state'''
    async for queue_state in self._get_queue_state():
      oneshot = await self.client.scheduler.is_oneshot()

      self.stdout.write(
          await format_estimated_time(self.client.estimator, queue_state, oneshot) + '\n')

      if self.args.stop_empty and \
         not queue_state['started_jobs'] and (oneshot or not queue_state['queued_jobs']):
        break

  @arg_export('command_status')
  @arg_import('common_get_queue_state')
  @arg_define('job_types', type=str, nargs='*',
              choices=['all', 'finished', 'started', 'queued'], default='all',
              help='specify job types to show (default: %(default)s)')
  async def _handle_status(self):
    '''show the queue state'''
    if self.args.job_types == 'all':
      job_types = set(['finished', 'started', 'queued'])
    else:
      job_types = set(self.args.job_types)

    async for queue_state in self._get_queue_state():
      output = ''

      if 'finished' in job_types:
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

      if 'started' in job_types:
        output += f"Started jobs ({len(queue_state['started_jobs'])}):\n"
        for job in queue_state['started_jobs']:
          partial_state['started_jobs'].append(job)
          line = f"  {job['job_id']} {job['param_id']}"
          rem = await format_remaining_time_short(self.client.estimator, partial_state, False)
          line += f' [{format_elapsed_time_short(job)}+{rem}]:'
          line += f" {job['name']}"
          line = termcolor.colored(line, 'yellow')
          output += line + '\n'
        output += '\n'
      else:
        for job in queue_state['started_jobs']:
          partial_state['started_jobs'].append(job)

      if 'queued' in job_types:
        output += f"Queued jobs ({len(queue_state['queued_jobs'])}):\n"
        for job in queue_state['queued_jobs']:
          partial_state['queued_jobs'].append(job)
          line = f"  {job['job_id']} {job['param_id']}"
          rem = await format_remaining_time_short(self.client.estimator, partial_state, False)
          line += f' [+{rem}]:'
          line += f" {job['name']}"
          line = termcolor.colored(line, 'blue')
          output += line + '\n'
        output += '\n'

      #output += f"Concurrency: {queue_state['concurrency']}"
      self.stdout.write(output + '\n')

      oneshot = await self.client.scheduler.is_oneshot()

      if self.args.stop_empty and \
         not queue_state['started_jobs'] and (oneshot or not queue_state['queued_jobs']):
        break

  @arg_export('command_run')
  @arg_import('common_omit_params')
  @arg_import('common_estimate')
  @arg_define('command', type=str, nargs='+', help='adhoc command')
  async def _handle_run(self):
    '''run an adhoc command'''
    params = [{'command': self.args.command}]
    params = await self._omit_params(params)

    if self.args.estimate:
      await self._estimate(params)
      return

    job_ids = await self.client.queue.add(params)
    self.stdout.write(f'Added queued jobs: {job_ids[0]}\n')

  @arg_export('command_add')
  @arg_import('common_read_params')
  @arg_import('common_omit_params')
  @arg_import('common_estimate')
  async def _handle_add(self):
    '''add parameters to the queue'''
    params = await self._read_params()
    params = await self._omit_params(params)

    if self.args.estimate:
      await self._estimate(params)
      return

    job_ids = await self.client.queue.add(params)
    self.stdout.write(f'Added queued jobs: {" ".join(job_ids)}\n')

  @arg_export('command_rm')
  @arg_import('common_get_job_ids')
  async def _handle_rm(self):
    '''remove queued jobs'''
    job_ids = await self._get_job_ids('queued_jobs')
    count = await self.client.queue.remove_queued(job_ids)
    self.stdout.write(f'Removed queued jobs: {count}\n')

  @arg_export('command_move')
  @arg_define('operation', type=str, choices=['up', 'down'], help='operation')
  @arg_import('common_get_job_ids')
  async def _handle_up(self):
    '''priorize (up) or depriorize (down) queued jobs'''
    changed_job_ids = await self._get_job_ids('queued_jobs')

    queue_state = await self.client.queue.get_state()
    job_ids = [job['job_id'] for job in queue_state['queued_jobs']]

    if self.args.operation == 'up':
      for i in range(1, len(job_ids)):
        if job_ids[i] in changed_job_ids:
          job_ids[i - 1], job_ids[i] = job_ids[i], job_ids[i - 1]
    elif self.args.operation == 'down':
      for i in range(len(job_ids) - 2, -1, -1):
        if job_ids[i] in changed_job_ids:
          job_ids[i], job_ids[i + 1] = job_ids[i + 1], job_ids[i]
    else:
      assert False

    count = await self.client.queue.reorder(job_ids)
    self.stdout.write(f'Reordered queued jobs: {count}\n')

  @arg_export('command_kill')
  @arg_import('common_get_job_ids')
  @arg_define('-f', '--force', action='store_true', default=False, help='kill forcefully')
  async def _handle_kill(self):
    '''kill started jobs'''
    job_ids = await self._get_job_ids('started_jobs')
    count = await self.client.runner.kill(job_ids, force=self.args.force)
    self.stdout.write(f'Killed jobs: {count}\n')

  @arg_export('command_retry')
  @arg_import('common_get_job_ids')
  @arg_import('common_omit_params')
  @arg_import('common_estimate')
  async def _handle_retry(self):
    '''retry finished jobs'''
    params = []
    queue_state = await self.client.queue.get_state()

    for job_id in await self._get_job_ids('finished_jobs'):
      for job in queue_state['finished_jobs']:
        if job['job_id'] == job_id:
          params.append(job['param'])
          break
      else:
        raise RuntimeError(f'No parameter for job {job_id}')

    params = await self._omit_params(params)

    if self.args.estimate:
      await self._estimate(params)
      return

    job_ids = await self.client.queue.add(params)
    self.stdout.write(f'Added queued jobs: {" ".join(job_ids)}\n')

  @arg_export('command_dismiss')
  @arg_import('common_get_job_ids')
  async def _handle_dismiss(self):
    '''clear finished jobs'''
    job_ids = await self._get_job_ids('finished_jobs')
    count = await self.client.queue.remove_finished(job_ids)
    self.stdout.write(f'Removed finished jobs: {count}\n')

  @arg_export('command_cat')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_get_job_or_param_id')
  async def _handle_cat(self):
    '''show the job output'''
    stdout = await self._get_stdout_stderr()
    job_id = await self._get_job_or_param_id()
    async for data in self.client.runner.cat(job_id, stdout, self.unknown_args):
      data = base64.a85decode(data.encode('ascii')).decode('utf-8')
      sys.stdout.write(data)

  @arg_export('command_head')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_get_job_or_param_id')
  async def _handle_head(self):
    '''show the head of job output'''
    stdout = await self._get_stdout_stderr()
    job_id = await self._get_job_or_param_id()
    async for data in self.client.runner.head(job_id, stdout, self.unknown_args):
      data = base64.a85decode(data.encode('ascii')).decode('utf-8')
      sys.stdout.write(data)

  @arg_export('command_tail')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_get_job_or_param_id')
  async def _handle_tail(self):
    '''show the tail of job output'''
    stdout = await self._get_stdout_stderr()
    job_id = await self._get_job_or_param_id()
    async for data in self.client.runner.tail(job_id, stdout, self.unknown_args):
      data = base64.a85decode(data.encode('ascii')).decode('utf-8')
      sys.stdout.write(data)

  @arg_export('command_migrate')
  @arg_define('old_params_file', type=str, help='path to the file containing old parameters ')
  @arg_import('common_read_params')
  async def _handle_migrate(self):
    '''migrate output data for new parameters'''
    # Use vars() on Namespace to access a field containing -
    old_params = json.load(open(self.args.old_params_file))
    new_params = await self._read_params()

    if len(old_params) != len(new_params):
      raise RuntimeError('Two inputs of parameters must have the same length')

    changes = []
    for i, new_param in enumerate(new_params):
      old_param = old_params[i]
      old_param_id = get_param_id(old_param)
      new_param_id = get_param_id(new_param)
      if old_param_id != new_param_id:
        changes.append((old_param_id, new_param_id))

    symlink_count = await self.client.runner.migrate(changes)
    entry_count = await self.client.history.migrate(changes)
    self.stdout.write(f'Migrated: {symlink_count} symlinks, ' + \
                      f'{entry_count} histroy entries\n')

  @arg_export('command_prune')
  @arg_define('type', type=str, choices=['matching', 'mismatching'], help='type')
  @arg_import('common_read_params')
  async def _handle_prune(self):
    '''prune output data matching or mismatching parameters'''
    params = await self._read_params()
    param_ids = [get_param_id(param) for param in params]

    if self.args.type == 'matching':
      symlink_count, dir_count = await self.client.runner.prune(param_ids, prune_matching=True)
      entry_count = await self.client.history.prune(param_ids, prune_matching=True)
    elif self.args.type == 'mismatching':
      symlink_count, dir_count = await self.client.runner.prune(param_ids, prune_mismatching=True)
      entry_count = await self.client.history.prune(param_ids, prune_mismatching=True)
    else:
      assert False

    self.stdout.write(f'Pruned: {symlink_count} symlinks, ' + \
                      f'{dir_count} output directories, ' + \
                      f'{entry_count} histroy entries\n')

def make_parser():
  '''Return a new argument parser.'''
  parser = argparse.ArgumentParser(description=\
      '''interact with the exptools server.
      ":" connects commands with pipes, and "::" chains commands without pipe connection.''')

  parser.add_argument('--host', type=str, default='localhost',
                      help='the hostname of the server (default: %(default)s)')
  parser.add_argument('--port', type=int, default='31234',
                      help='the port number of the server (default: %(default)s)')
  parser.add_argument('--secret-file', type=str,
                      default='secret.json', help='the secret file path (default: %(default)s)')

  parser.add_argument('-v', '--verbose', help='be verbose')

  subparsers = parser.add_subparsers(dest='command')

  for name, func in _ARG_EXPORTS.items():
    if name.startswith('command_'):
      command = name.partition('_')[2]
      subparser = subparsers.add_parser(command, help=func.__doc__)

      arg_add_options(subparser, func)

  return parser

def run_client():
  '''Parse arguments and process a client command.'''
  parser = make_parser()

  argv = sys.argv[1:]

  group_start = -1
  for i, arg in enumerate(argv):
    if not arg.startswith('-'):
      group_start = i
      break

  if group_start == -1:
    # No command given
    parser.print_help()
    sys.exit(1)

  # Use a dummy start command to parse the main arguments
  common_args = parser.parse_args(argv[:group_start] + ['start'])

  # Parse sub-arguments
  def _parse_group(group_start, group_end):
    if argv[group_start] in ['cat', 'head', 'tail']:
      args, unknown_args = parser.parse_known_args(argv[group_start:group_end])
    else:
      args, unknown_args = parser.parse_args(argv[group_start:group_end]), None
    return args, unknown_args
  args_list = []
  pipe_break = []
  for i in range(group_start, len(argv)):
    if argv[i] == ':' or argv[i] == '::':
      args, unknown_args = _parse_group(group_start, i)
      args_list.append((args, unknown_args))
      group_start = i + 1
      if argv[i] == ':':
        pipe_break.append(False)
      else:
        pipe_break.append(True)
  args, unknown_args = _parse_group(group_start, len(argv))
  args_list.append((args, unknown_args))
  pipe_break.append(True)

  # Run commands
  loop = asyncio.get_event_loop()

  try:
    client_pool = {}
    stdin = sys.stdin
    if pipe_break[0]:
      stdout = sys.stdout
    else:
      stdout = io.StringIO()
    for i, (args, unknown_args) in enumerate(args_list):
      # Run a handler
      handler = CommandHandler(stdin, stdout, common_args, args, unknown_args, client_pool, loop)
      handle_future = asyncio.ensure_future(handler.handle(), loop=loop)
      loop.run_until_complete(handle_future)

      if pipe_break[i]:
        stdin = sys.stdin
      else:
        # Connect stdout to stdin
        assert isinstance(stdout, io.StringIO)
        stdout.seek(0)
        stdin = stdout

      if i + 1 < len(args_list) and pipe_break[i + 1]:
        stdout = sys.stdout
      else:
        stdout = io.StringIO()
  except KeyboardInterrupt:
    if common_args.verbose:
      raise
