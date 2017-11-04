"""Handle client commands."""

__all__ = ['client_main']

import argparse
import asyncio
import base64
import collections
import concurrent
import json
import os
import pprint
import sys

import termcolor

from exptools.estimator import Estimator
from exptools.param import get_param_id, get_hash_id, get_name
from exptools.rpc_client import Client
from exptools.time import (
  job_elapsed_time,
  format_sec_short,
  format_estimated_time,
  format_local,
  parse_utc
)

_ARG_EXPORTS = collections.OrderedDict()


def arg_export(name):
  """Export an argument set."""

  def _wrapper(func):
    _ARG_EXPORTS[name] = func
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    return func

  return _wrapper


def arg_import(name):
  """Import an argument set."""

  def _wrapper(func):
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    func.arg_defs.insert(0, ('import', name))
    return func

  return _wrapper


def arg_define(*args, **kwargs):
  """Add an argument option."""

  def _wrapper(func):
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    func.arg_defs.insert(0, ('add', args, kwargs))
    return func

  return _wrapper


def arg_define_exclusive(*args, **kwargs):
  """Add an argument option."""

  def _wrapper(func):
    if 'arg_defs' not in dir(func):
      func.arg_defs = []
    func.arg_defs.insert(0, ('add-exclusive', args, kwargs))
    return func

  return _wrapper


def arg_add_options(parser, func):
  """Add argument options defined for the function to the parser."""
  exclusive_sets = collections.OrderedDict()
  for arg_def in func.arg_defs:
    if arg_def[0] == 'import':
      arg_add_options(parser, _ARG_EXPORTS[arg_def[1]])
    elif arg_def[0] == 'add':
      parser.add_argument(*arg_def[1], **arg_def[2])
    elif arg_def[0] == 'add-exclusive':
      if arg_def[1][0] not in exclusive_sets:
        group = parser.add_mutually_exclusive_group()
        exclusive_sets[arg_def[1][0]] = group
      group = exclusive_sets[arg_def[1][0]]
      group.add_argument(*arg_def[1][1:], **arg_def[2])
    else:
      assert False


class CommandHandler:
  """Handle a command."""

  def __init__(self, common_args, args, unknown_args, pipe_break, chain, client_pool, loop):
    self.common_args = common_args
    self.args = args
    self.unknown_args = unknown_args
    self.pipe_break = pipe_break
    self.chain = chain
    self.client_pool = client_pool
    self.loop = loop

    self.client = None
    self.client_watch = None
    self._handler = None

  async def _init(self):
    """Initialize the command handler."""
    command = self.args.command

    # if command not in ['d', 'dum', 'dump', 'select', 'grep']:
    if 'client' not in self.client_pool:
      secret = json.load(open(self.common_args.secret_file))
      client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
      await client.connect()
      self.client_pool['client'] = client
    self.client = self.client_pool['client']

    if 'follow' in self.args and self.args.follow:
      if 'client_watch' not in self.client_pool:
        secret = json.load(open(self.common_args.secret_file))
        client = Client(self.common_args.host, self.common_args.port, secret, self.loop)
        await client.connect()
        self.client_pool['client_watch'] = client
      self.client_watch = self.client_pool['client_watch']

    self._handler = _ARG_EXPORTS['command_' + command]

  async def handle(self):
    """Handle a command."""
    await self._init()
    await self._handler(self)
    if self.pipe_break and self.chain:
      args = argparse.Namespace()
      args.command = 'd'
      sink = CommandHandler(
        self.common_args, args, None, True, self.chain, self.client_pool, self.loop)
      await sink.handle()
      assert not self.chain

  #### Chain methods

  def _add_to_chain(self, operation, *args, **kwargs):
    self.chain.append([operation, args, kwargs])

  async def _execute_chain(self, output_type):
    if not self.pipe_break:
      raise RuntimeError('Chain can be executed only at the end of the chain')

    if not self.chain or self.chain[0][0] != 'select':
      raise RuntimeError('No selector specified; use select command')

    if output_type == 'params':
      self.chain.append(['get_params', [], {}])
    elif output_type == 'param_ids':
      self.chain.append(['get_param_ids', [], {}])
    elif output_type == 'hash_ids':
      self.chain.append(['get_hash_ids', [], {}])
    else:
      assert False, f'Invalid output type {output_type}'

    output = await self.client.resolver.filter_params(self.chain)

    self.chain.clear()
    return output

  #### Parse methods

  @arg_export('common_job_ids')
  @arg_define('job_ids', type=str, nargs='+',
              help=('job IDs; ' +
                    '"all" selects all jobs; ' +
                    '"first[-N]" selects to the first [N] job; ' +
                    '"last[-N]" selects to the last [N] job; ' +
                    'BEGIN:END selects all jobs between two IDs (inclusive).'))
  async def _parse_job_ids(self, job_types):
    """Parse job IDs."""
    all_job_ids = await self.client.queue.job_ids(job_types)

    job_ids = []
    for id_ in self.args.job_ids:
      if id_ == 'all':
        job_ids.extend(all_job_ids)

      elif id_.startswith('first'):
        if id_.find('-') != -1:
          first = int(id_.partition('-')[2])
        else:
          first = 1
        job_ids.extend(all_job_ids[:first])

      elif id_.startswith('last'):
        if id_.find('-') != -1:
          last = int(id_.partition('-')[2])
        else:
          last = 1
        job_ids.extend(all_job_ids[-last:])

      elif id_.find(':') != -1:
        begin_id, _, end_id = id_.partition(':')

        begin_pos = 0
        if begin_id:
          while begin_pos < len(all_job_ids):
            if all_job_ids[begin_pos] == begin_id:
              break
            begin_pos += 1
          else:
            raise RuntimeError(f'No job found: {begin_id}')

        end_pos = len(all_job_ids) - 1
        if end_id:
          end_pos = begin_pos
          while end_pos < len(all_job_ids):
            if all_job_ids[end_pos] == end_id:
              break
            end_pos += 1
          else:
            raise RuntimeError(f'No job found: {end_id}')

        job_ids.extend(all_job_ids[begin_pos:end_pos + 1])

      else:
        assert id_.startswith('j-')
        job_ids.append(id_)

    return job_ids

  #### Formatting methods

  @staticmethod
  def _get_job_id_max_len(jobs=None):
    if jobs:
      return max([len(job['job_id']) for job in jobs])
    return 3

  @staticmethod
  def _get_param_id_max_len(params=None):
    if params:
      return max([len(get_param_id(param)) for param in params])
    return 3

  #### Common methods

  @arg_export('common_get_queue_state')
  @arg_define('-f', '--follow', action='store_true', default=False, help='follow queue changes')
  @arg_define('-i', '--interval', type=float, default=0.,
              help=('refresh interval in seconds for no queue change; ' +
                    'use 0 to refresh upon changes only (default: %(default)s)'))
  @arg_define('-s', '--stop-empty', action='store_true', default=False,
              help='stop upon empty queue')
  @arg_define('-c', '--clear-screen', action='store_true', default=False,
              help='clear screen before showing the queue')
  async def _get_queue_state(self):
    if self.args.follow:
      interval = self.args.interval

      if not interval:
        async for state in self.client_watch.queue.watch_state_fast():
          yield state
      else:
        # Manually access asynchronous generator to use asyncio.wait() for timeout
        watch_state_gen = self.client_watch.queue.watch_state_fast().__aiter__()
        gen_next = asyncio.ensure_future(watch_state_gen.__anext__(), loop=self.loop)
        try:
          state = await self.client.queue.get_state_fast()
          try:
            while True:
              await asyncio.wait([gen_next], timeout=interval, loop=self.loop)

              if gen_next.done():
                state = gen_next.result()
                # Make a new task to get the next item
                gen_next = asyncio.ensure_future(watch_state_gen.__anext__(), loop=self.loop)
              yield state
          except StopAsyncIteration:
            pass
        finally:
          gen_next.cancel()
          try:
            await gen_next
          except concurrent.futures.CancelledError:
            # Ignore CancelledError because we caused it
            pass
    else:
      state = await self.client.queue.get_state_fast()
      yield state

  @arg_export('common_get_stdout_stderr')
  @arg_define_exclusive('stdout',
                        '-o', '--stdout', action='store_true', default=True,
                        help='read stdout (default)')
  @arg_define_exclusive('stdout',
                        '-e', '--stderr', action='store_false', dest='stdout',
                        help='read stderr instead of stdout', default=argparse.SUPPRESS)
  async def _get_stdout_stderr(self):
    return self.args.stdout

  #### Scheduler command handlers

  @arg_export('command_start')
  async def _handle_start(self):
    """start the scheduler"""
    succeeded = await self.client.scheduler.start()
    if succeeded:
      print('Scheduler started')
    else:
      raise RuntimeError('Failed to start scheduler')

  @arg_export('command_stop')
  async def _handle_stop(self):
    """stop the scheduler"""
    succeeded = await self.client.scheduler.stop()
    if succeeded:
      print('Scheduler stopped')
    else:
      raise RuntimeError('Failed to stop scheduler')

  @arg_export('command_oneshot')
  async def _handle_oneshot(self):
    """schedule only one job and stop"""
    succeeded = await self.client.scheduler.set_oneshot()
    if succeeded:
      print('Scheduler oneshot mode set')
    else:
      raise RuntimeError('Failed to set oneshot mode')

  @arg_export('command_resource')
  @arg_define('operation', type=str, choices=['add', 'rm'], help='operation')
  @arg_define('key', type=str, help='resource key')
  @arg_define('value', type=str, help='resource value')
  async def _handle_resource(self):
    """add or remove a resource"""
    if self.args.operation == 'add':
      succeeded = await self.client.scheduler.add_resource(self.args.key, self.args.value)
    elif self.args.operation == 'remove':
      succeeded = await self.client.scheduler.remove_resource(self.args.key, self.args.value)
    else:
      assert False
    if succeeded:
      print('Resource updated')
    else:
      raise RuntimeError('Failed to update resource')

  #### Parameter set handlers

  @arg_export('command_paramset')
  @arg_define_exclusive('operation',
                        '-l', '--list', action='store_const', const='list', dest='operation',
                        default=argparse.SUPPRESS,
                        help='list existing parameter sets')
  @arg_define_exclusive('operation',
                        '-c', '--create', action='store_const', const='create', dest='operation',
                        default='create',
                        help='create a new parameter set')
  @arg_define_exclusive('operation',
                        '-d', '--delete', action='store_const', const='delete', dest='operation',
                        default=argparse.SUPPRESS,
                        help='delete existing parameter sets')
  @arg_define_exclusive('operation',
                        '-r', '--rename', action='store_const', const='rename', dest='operation',
                        default=argparse.SUPPRESS,
                        help='rename an existing parameter set')
  @arg_define_exclusive('operation',
                        '-m', '--migrate', action='store_const', const='migrate', dest='operation',
                        default=argparse.SUPPRESS,
                        help='migrate the output and history of a parameter set into another')
  @arg_define('-i', '--initialize', action='store_true', default=False,
              help='initialize existing parameter sets if exist')
  @arg_define('paramsets', type=str, nargs='*', help='parameter sets')
  async def _handle_paramset(self):
    """manage parameter sets"""
    if self.args.operation == 'list':
      if self.args.paramsets:
        raise RuntimeError('-l/--list does not take parameter sets')
      return await self._paramset_list()

    elif self.args.operation == 'create':
      if not self.args.paramsets:
        raise RuntimeError('No parameter set is given')
      return await self._paramset_add(self.args.paramsets, self.args.initialize)

    elif self.args.operation == 'delete':
      if not self.args.paramsets:
        raise RuntimeError('No parameter set is given')
      return await self._paramset_remove(self.args.paramsets)

    elif self.args.operation == 'rename':
      if len(self.args.paramsets) != 2:
        raise RuntimeError('-r/--rename take two parameter sets')
      return await self._paramset_rename(*self.args.paramsets)

    elif self.args.operation == 'migrate':
      if len(self.args.paramsets) != 2:
        raise RuntimeError('-m/--migrate take two parameter sets')
      return await self._paramset_migrate(*self.args.paramsets)

    else:
      assert False

  async def _paramset_list(self):
    paramsets = await self.client.registry.paramsets()
    print('Parameter sets:')
    for paramset in paramsets:
      print('  ' + paramset)

  async def _paramset_add(self, paramsets, initialize):
    for paramset in paramsets:
      if initialize and paramset in await self.client.registry.paramsets():
        succeeded = await self.client.registry.remove_paramset(paramset)
        if succeeded:
          print(f'Removed: {paramset}')
        else:
          raise RuntimeError(f'Failed to remove: {paramset}')

      succeeded = await self.client.registry.add_paramset(paramset)
      if succeeded:
        print(f'Added: {paramset}')
      else:
        raise RuntimeError(f'Failed to add: {paramset}')

  async def _paramset_rename(self, old_paramset, new_paramset):
    succeeded = await self.client.registry.rename_paramset(old_paramset, new_paramset)
    if succeeded:
      print(f'Renamed: {old_paramset} to {new_paramset}')
    else:
      raise RuntimeError(f'Failed to rename: {old_paramset} to {new_paramset}')

  async def _paramset_remove(self, paramsets):
    for paramset in paramsets:
      succeeded = await self.client.registry.remove_paramset(paramset)
      if succeeded:
        print(f'Removed: {paramset}')
      else:
        raise RuntimeError(f'Failed to remove: {paramset}')

  async def _paramset_migrate(self, old_paramset, new_paramset):
    old_param_ids = await self.client.registry.paramset(old_paramset)
    new_param_ids = await self.client.registry.paramset(new_paramset)

    old_hash_ids = [get_hash_id(param) for param
                    in await self.client.registry.params(old_param_ids)]
    new_hash_ids = [get_hash_id(param) for param
                    in await self.client.registry.params(new_param_ids)]

    if len(old_param_ids) != len(new_param_ids):
      raise RuntimeError('Two parameter sets must have the same number of parameters')

    param_id_pairs = list(zip(old_param_ids, new_param_ids))
    hash_id_pairs = list(zip(old_hash_ids, new_hash_ids))

    migrated_param_id_pairs, migrated_hash_id_pairs = (
      await self.client.runner.migrate(param_id_pairs, hash_id_pairs))
    migrated_hash_id_pairs = await self.client.history.migrate(hash_id_pairs)
    print(f'Migrated: ' +
          f'{len(migrated_param_id_pairs) + len(migrated_hash_id_pairs)} runner data, ' +
          f'{len(migrated_hash_id_pairs)} history data')

  @arg_export('command_add')
  @arg_define('paramset', type=str, help='parameter set to modify')
  @arg_define('-c', '--create', action='store_true', default=False,
              help='create a new parameter set if not exists')
  @arg_define('-i', '--initialize', action='store_true', default=False,
              help='initialize the existing parameter set if exists')
  @arg_define('-f', '--file', type=str, default=None,
              help=('load from file instead of using selected parameters; ' +
                    'use "-" to use standard input'))
  async def _handle_add(self):
    """add parameters to a parameter set"""
    paramset = self.args.paramset
    if self.args.file is None:
      params = None
      param_ids = await self._execute_chain('param_ids')
    elif self.args.file == '-':
      params = json.loads(sys.stdin.read())
      param_ids = None
    else:
      with open(self.args.file) as file:
        params = json.loads(file.read())
        param_ids = None

    if paramset not in await self.client.registry.paramsets():
      if self.args.create:
        await self._paramset_add([paramset], False)
    else:
      if self.args.initialize:
        await self._paramset_add([paramset], True)

    if param_ids is None:
      param_ids = await self.client.registry.add(paramset, params)
    else:
      param_ids = await self.client.registry.add_by_param_ids(paramset, param_ids)
    print(f'Added: {len(param_ids)} parameters to {paramset}')

  @arg_export('command_rm')
  @arg_define('paramset', type=str, help='parameter set to modify')
  async def _handle_rm(self):
    """remove parameters from a parameter set"""
    paramset = self.args.paramset
    param_ids = await self._execute_chain('param_ids')

    param_ids = await self.client.registry.remove(paramset, param_ids)
    print(f'Removed: {len(param_ids)} parameters from {paramset}')

  @arg_export('command_reset')
  async def _handle_reset(self):
    """reset history data of matching IDs"""
    hash_ids = await self._execute_chain('hash_ids')

    reset_hash_ids = await self.client.history.reset(hash_ids)
    print(f'Reset: {len(reset_hash_ids)} history data')

  @arg_export('command_prune')
  async def _handle_prune(self):
    """prune unknown output and history data"""
    valid_param_ids = set(await self.client.registry.param_ids())
    valid_hash_ids = set(await self.client.registry.hash_ids())

    param_ids = set(await self.client.runner.param_ids()) - valid_param_ids
    hash_ids = set(await self.client.runner.hash_ids()) - valid_hash_ids
    removed_outputs = await self.client.runner.remove_output(list(param_ids), list(hash_ids))

    hash_ids = set(await self.client.history.hash_ids()) - valid_hash_ids
    removed_hash_ids = await self.client.history.remove(list(hash_ids))

    print(f'Pruned: {len(removed_outputs)} runner data, ' +
          f'{len(removed_hash_ids)} history data')

  #### Parameter selection and filtering handlers

  @arg_export('command_select')
  @arg_define('ids', type=str, nargs='+', help='parameter sets or IDs; use "all" to select all')
  async def _handle_select(self):
    """select parameters in the registry"""
    self._add_to_chain('select', self.args.ids)

  @arg_export('command_grep')
  @arg_define('filter_expr', type=str, help='regular expression')
  @arg_define('-i', '--ignore-case', action='store_true', default=False,
              help='ignore case')
  @arg_define('-v', '--invert-match', action='store_true', default=False,
              help='invert the match result')
  @arg_define('-x', '--line-regexp', action='store_true', default=False,
              help='match the whole line')
  async def _handle_grep(self):
    """filter parameters using a regular expression on parameter names"""
    self._add_to_chain(
      'grep', self.args.filter_expr,
      self.args.ignore_case, self.args.invert_match, self.args.line_regexp)

  @arg_export('command_yaql')
  @arg_define('filter_expr', type=str, help='YAQL expression for $.where()')
  async def _handle_yaql(self):
    """filter parameters using a YAQL expression"""
    self._add_to_chain('yaql', self.args.filter_expr)

  @arg_export('command_omit')
  @arg_define('omit_types',
              choices=[
                'succeeded', 'S',
                'failed', 'F',
                'finished', 'f',
                'started', 'A',
                'queued', 'Q',
                'identical', 'I',
                'duplicate', 'D'],
              nargs='*',
              help=('omit specified parameter types; ' +
                    'S=success, F=failed, f=finished, A=started, Q=queued, ' +
                    'I=identical, D=duplicate'))
  async def _handle_omit(self):
    """omit parameters of specified types"""
    types = []
    for type_ in self.args.omit_types:
      type_ = {
        'S': 'succeeded',
        'F': 'failed',
        'f': 'finished',
        'A': 'started',
        'Q': 'queued',
        'I': 'identical',
        'D': 'duplicate',
      }.get(type_, type_)
      types.append(type_)

    self._add_to_chain('omit', types)

  @arg_export('command_only')
  @arg_define('only_types',
              choices=[
                'succeeded', 'S',
                'failed', 'F',
                'finished', 'f',
                'started', 'A',
                'queued', 'Q',
                'identical', 'I',
                'duplicate', 'D'],
              nargs='*',
              help=('only include specified parameter types; ' +
                    'S=success, F=failed, f=finished, A=started, Q=queued, ' +
                    'I=identical, D=duplicate'))
  async def _handle_only(self):
    """only include parameters of specified types"""
    types = []
    for type_ in self.args.only_types:
      type_ = {
        'S': 'succeeded',
        'F': 'failed',
        'f': 'finished',
        'A': 'started',
        'Q': 'queued',
        'I': 'identical',
        'D': 'duplicate',
      }.get(type_, type_)
      types.append(type_)

    self._add_to_chain('only', types)

  @arg_export('command_sort')
  @arg_define('sort_key', type=str,
              help='sort parameters by the given key (e.g., _.finished)')
  @arg_define('-r', '--reverse', action='store_true', default=False, help='reverse sorting')
  async def _handle_sort(self):
    """sort parameters"""
    self._add_to_chain('sort', self.args.sort_key, reverse=self.args.reverse)

  #### Parameter dump handlers

  @arg_export('command_d')
  async def _handle_d(self):
    """summarize parameters"""
    params = await self._execute_chain('params')

    param_id_max_len = self._get_param_id_max_len(params)

    for param in params:
      line = f'{get_param_id(param):{param_id_max_len}} '
      line += f'{get_hash_id(param)}  '
      line += get_name(param)

      print(line)

  @arg_export('command_du')
  @arg_define('-l', '--local', action='store_true', default=False,
              help='show local time instead of UTC')
  async def _handle_du(self):
    """summarize parameters with time information"""
    params = await self._execute_chain('params')

    param_id_max_len = self._get_param_id_max_len(params)

    for param in params:
      meta = param['_']

      line = f'{get_param_id(param):{param_id_max_len}} '
      line += f'{get_hash_id(param)} '

      if meta['finished'] is not None:
        finished = meta['finished']
        if self.args.local:
          finished = format_local(parse_utc(finished))
        line += f"[{finished.partition('.')[0]:>19}] "
      else:
        line += ' ' * (19 + 3)

      if meta['duration'] is not None:
        line += f"[{format_sec_short(meta['duration']):>7}] "
      else:
        line += ' ' * (7 + 3)

      if meta['succeeded'] is None:
        line += f'           '
      elif meta['succeeded']:
        line += f'succeeded  '
      else:
        line += f'FAILED     '

      line += get_name(param)

      print(line)

  @arg_export('command_dum')
  async def _handle_dum(self):
    """dump parameters in Python"""
    params = await self._execute_chain('params')

    for param in params:
      print(f'{get_param_id(param)}')
      for line in pprint.pformat(param).split('\n'):
        print('  ' + line)
      print('')

  @arg_export('command_dump')
  @arg_define('-f', '--file', type=str, default=None,
              help='write to a file instead of standard output')
  async def _handle_dump(self):
    """dump parameters in JSON"""
    params = await self._execute_chain('params')

    json_data = json.dumps(params, sort_keys=True, indent=2)

    if self.args.file:
      with open(self.args.file, 'w') as file:
        file.write(json_data + '\n')
    else:
      print(json_data)

  #### Queue command handlers

  @arg_export('command_s')
  @arg_import('common_get_queue_state')
  async def _handle_s(self):
    """summarize the queue state"""
    estimator = Estimator(self.client.history)
    use_color = self.common_args.color == 'yes'

    async for queue_state in self._get_queue_state():
      oneshot = await self.client.scheduler.is_oneshot()

      output = await format_estimated_time(estimator, queue_state, oneshot, use_color)

      if self.args.clear_screen:
        os.system('clear')
      print(output)

      if (self.args.stop_empty and
          not queue_state['started_jobs'] and (oneshot or not queue_state['queued_jobs'])):
        break

  @arg_export('command_status')
  @arg_import('common_get_queue_state')
  @arg_define('-l', '--limit', type=int, default=0,
              help=('limit the number of jobs for each job type; ' +
                    'use 0 to show all (default: %(default)s)'))
  @arg_define('job_types', type=str, nargs='*',
              choices=['all', 'finished', 'started', 'queued'], default='all',
              help='specify job types to show (default: %(default)s)')
  async def _handle_status(self):
    """show the queue state"""
    limit = self.args.limit

    if self.args.job_types == 'all':
      job_types = {'finished', 'started', 'queued'}
    else:
      job_types = set(self.args.job_types)

    estimator = Estimator(self.client.history)
    use_color = self.common_args.color == 'yes'

    if use_color:
      colored = termcolor.colored
    else:
      def colored(s, *args, **kwargs):  # pylint: disable=invalid-name,unused-argument
        """Use no color."""
        return s

    async for queue_state in self._get_queue_state():
      oneshot = await self.client.scheduler.is_oneshot()

      output = ''

      all_jobs = (queue_state['finished_jobs'] +
                  queue_state['started_jobs'] +
                  queue_state['queued_jobs'])
      all_params = [job['param'] for job in all_jobs]

      job_id_max_len = self._get_job_id_max_len(all_jobs)
      param_id_max_len = self._get_param_id_max_len(all_params)

      if 'finished' in job_types:
        succeeded_count = len([job for job in queue_state['finished_jobs'] if job['succeeded']])
        failed_count = len(queue_state['finished_jobs']) - succeeded_count
        finished_jobs_color = 'red' if failed_count else 'green'
        output += colored(
          f"Finished jobs (S:{succeeded_count} / F:{failed_count})",
          finished_jobs_color, attrs=['reverse']) + '\n'

        if limit and len(queue_state['finished_jobs']) > limit:
          line = colored('  ', finished_jobs_color, attrs=['reverse'])
          output += line + ' ...\n'

        jobs = queue_state['finished_jobs']
        if limit:
          jobs = jobs[-limit:]

        for job in jobs:
          if job['succeeded']:
            line = colored('  ', 'green', attrs=['reverse'])
          else:
            line = colored('  ', 'red', attrs=['reverse'])

          param_id = get_param_id(job['param'])
          hash_id = get_hash_id(job['param'])
          name = get_name(job['param'])

          line += f" {job['job_id']:{job_id_max_len}} {param_id:{param_id_max_len}} {hash_id}"
          line += f' [{format_sec_short(job_elapsed_time(job)):>7}]'
          if job['succeeded']:
            line += ' succeeded  '
          else:
            line += ' FAILED     '
          line += f"{name}"
          output += line + '\n'

        output += '\n'

      _, rem_map = await estimator.estimate_remaining_time(queue_state, False)
      last_rem = 0.

      if 'started' in job_types:
        output += colored(
          f"Started jobs (A:{len(queue_state['started_jobs'])})",
          'cyan', attrs=['reverse']) + '\n'

        if limit and len(queue_state['started_jobs']) > limit:
          line = colored('  ', 'cyan', attrs=['reverse'])
          output += line + ' ...\n'

        jobs = queue_state['started_jobs']
        if limit:
          jobs = jobs[-limit:]

        for job in jobs:
          rem = rem_map[job['job_id']]

          param_id = get_param_id(job['param'])
          hash_id = get_hash_id(job['param'])
          name = get_name(job['param'])

          line = colored('  ', 'cyan', attrs=['reverse'])
          line += f" {job['job_id']:{job_id_max_len}} {param_id:{param_id_max_len}} {hash_id}"
          line += f' [{format_sec_short(job_elapsed_time(job)):>7}]'
          line += f'+[{format_sec_short(max(rem - last_rem, 0)):>7}]'
          line += '  '
          last_rem = rem
          line += f"{name}"
          output += line + '\n'

        output += '\n'

      if 'queued' in job_types:
        output += colored(
          f"Queued jobs (Q:{len(queue_state['queued_jobs'])})",
          'blue', attrs=['reverse']) + '  '

        output += 'Scheduler: '
        if oneshot:
          output += colored('Oneshot', 'blue')
        elif await self.client.scheduler.is_running():
          output += colored('Running', 'cyan')
        else:
          output += colored('Stopped', 'red')
        output += '\n'

        jobs = queue_state['queued_jobs']
        if limit:
          jobs = jobs[:limit]

        for job in jobs:
          rem = rem_map[job['job_id']]

          param_id = get_param_id(job['param'])
          hash_id = get_hash_id(job['param'])
          name = get_name(job['param'])

          line = colored('  ', 'blue', attrs=['reverse'])
          line += f" {job['job_id']:{job_id_max_len}} {param_id:{param_id_max_len}} {hash_id}"
          line += f'           [{format_sec_short(max(rem - last_rem, 0)):>7}]'
          line += '  '
          last_rem = rem
          line += f"{name}"
          output += line + '\n'

        if limit and len(queue_state['queued_jobs']) > limit:
          line = colored('  ', 'blue', attrs=['reverse'])
          output += line + ' ...\n'

        output += '\n'

      # output += f"Concurrency: {queue_state['concurrency']}"

      output += await format_estimated_time(estimator, queue_state, oneshot, use_color) + '\n'

      if self.args.clear_screen:
        os.system('clear')
      print(output)

      if (self.args.stop_empty and
          not queue_state['started_jobs'] and (oneshot or not queue_state['queued_jobs'])):
        break

  @arg_export('command_adhoc')
  @arg_define('-t', '--top', action='store_true', default=False,
              help='insert at the queue top instead of bottom')
  @arg_define('arguments', type=str, nargs='+',
              help='command and arguments')
  async def _handle_adhoc(self):
    """add an adhoc parameter to the queue"""
    paramset = 'adhoc'
    param = {'command': list(self.args.arguments)}

    if paramset not in await self.client.registry.paramsets():
      await self._paramset_add([paramset], False)

    param_ids = await self.client.registry.add(paramset, [param])
    print(f'Added: {len(param_ids)} parameters to {paramset}')

    job_ids = await self.client.queue.add(param_ids, not self.args.top)
    print(f'Added: {len(job_ids)} queued jobs')

  @arg_export('command_enqueue')
  @arg_define('-t', '--top', action='store_true', default=False,
              help='insert at the queue top instead of bottom')
  async def _handle_enqueue(self):
    """add parameters to the queue"""
    param_ids = await self._execute_chain('param_ids')

    job_ids = await self.client.queue.add(param_ids, not self.args.top)
    print(f'Added: {len(job_ids)} queued jobs')

  @arg_export('command_estimate')
  async def _handle_estimate(self):
    """estimate execution time instead of enqueueing"""
    hash_ids = await self._execute_chain('hash_ids')

    queue_state = await self.client.queue.get_state_fast()
    oneshot = await self.client.scheduler.is_oneshot()
    use_color = self.common_args.color == 'yes'

    estimator = Estimator(self.client.history)

    print('Current:   ' +
          await format_estimated_time(estimator, queue_state, oneshot, use_color))

    queue_state['queued_jobs'].extend(
      [{'job_id': 'dummy-j-{i}', 'param': {'_': {'hash_id': hash_id}}}
       for i, hash_id in enumerate(hash_ids)])

    print('Estimated: ' +
          await format_estimated_time(estimator, queue_state, oneshot, use_color))

  @arg_export('command_move')
  @arg_define('offset', type=int,
              help=('offset in the queue position; ' +
                    'negative numbers to prioritize, positive numbers for deprioritize'))
  @arg_import('common_job_ids')
  async def _handle_move(self):
    """prioritize or deprioritize queued jobs"""
    job_ids = await self._parse_job_ids(['queued'])

    moved_job_ids = await self.client.queue.move(job_ids, self.args.offset)
    print(f'Moved: {len(moved_job_ids)} queued jobs')

  @arg_export('command_cancel')
  @arg_import('common_job_ids')
  async def _handle_cancel(self):
    """cancel queued jobs"""
    job_ids = await self._parse_job_ids(['queued'])

    removed_job_ids = await self.client.queue.remove_queued(job_ids)
    print(f'Removed: {len(removed_job_ids)} queued jobs')

  @arg_export('command_retry')
  @arg_import('common_job_ids')
  @arg_define('-t', '--top', action='store_true', default=False,
              help='insert at the queue top instead of bottom')
  @arg_define('-d', '--dismiss', action='store_true', default=False, help='dismiss retried jobs')
  async def _handle_retry(self):
    """retry finished jobs"""
    finished_job_ids = await self._parse_job_ids(['finished'])

    finished_jobs = await self.client.queue.jobs(finished_job_ids)
    param_ids = [get_param_id(job['param']) for job in finished_jobs]

    job_ids = await self.client.queue.add(param_ids, not self.args.top)
    print(f'Added: {len(job_ids)} queued jobs')

    removed_job_ids = await self.client.queue.remove_finished(finished_job_ids)
    print(f'Removed: {len(removed_job_ids)} finished jobs')

  @arg_export('command_kill')
  @arg_define_exclusive('signal_type',
                        '-2', '--int', action='store_const', const='int', dest='signal_type',
                        default=argparse.SUPPRESS,
                        help='kill using SIGINT')
  @arg_define_exclusive('signal_type',
                        '-9', '--kill', action='store_const', const='kill', dest='signal_type',
                        default=argparse.SUPPRESS,
                        help='kill using SIGKILL')
  @arg_define_exclusive('signal_type',
                        '-15', '--term', action='store_const', const='term', dest='signal_type',
                        default='term',
                        help='kill using SIGTERM (default)')
  @arg_import('common_job_ids')
  async def _handle_kill(self):
    """kill started jobs"""
    job_ids = await self._parse_job_ids(['started'])

    killed_job_ids = await self.client.runner.kill(job_ids, signal_type=self.args.signal_type)

    print(f'Killed: {len(killed_job_ids)} started jobs')

  @arg_export('command_dismiss')
  @arg_import('common_job_ids')
  async def _handle_dismiss(self):
    """clear finished jobs"""
    job_ids = await self._parse_job_ids(['finished'])

    removed_job_ids = await self.client.queue.remove_finished(job_ids)
    print(f'Removed: {len(removed_job_ids)} finished jobs')

  #### Job output retrieval

  @arg_export('command_cat')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_job_ids')
  async def _handle_cat(self):
    """show the job output"""
    stdout = await self._get_stdout_stderr()

    job_ids = await self._parse_job_ids(['finished', 'started'])
    if len(job_ids) > 1:
      print('Only the first job ID will be used')
    job_id = job_ids[0]

    async for data in self.client.runner.cat_like(job_id, stdout, 'cat', self.unknown_args):
      data = base64.b64decode(data.encode('ascii')).decode('utf-8')
      print(data, end='')

  @arg_export('command_head')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_job_ids')
  async def _handle_head(self):
    """show the head of job output"""
    stdout = await self._get_stdout_stderr()

    job_ids = await self._parse_job_ids(['finished', 'started'])
    if len(job_ids) > 1:
      print('Only the first job ID will be used')
    job_id = job_ids[0]

    async for data in self.client.runner.cat_like(job_id, stdout, 'head', self.unknown_args):
      data = base64.b64decode(data.encode('ascii')).decode('utf-8')
      print(data, end='')

  @arg_export('command_tail')
  @arg_import('common_get_stdout_stderr')
  @arg_import('common_job_ids')
  async def _handle_tail(self):
    """show the tail of job output"""
    stdout = await self._get_stdout_stderr()

    job_ids = await self._parse_job_ids(['finished', 'started'])
    if len(job_ids) > 1:
      print('Only the first job ID will be used')
    job_id = job_ids[0]

    async for data in self.client.runner.cat_like(job_id, stdout, 'tail', self.unknown_args):
      data = base64.b64decode(data.encode('ascii')).decode('utf-8')
      print(data, end='')


def make_parser():
  """Return a new argument parser."""
  parser = argparse.ArgumentParser(
    description=(
      'interact with the exptools server.' +
      '":" connects commands with pipes, and "::" chains commands without pipe connection.'))

  parser.add_argument('--host', type=str, default='localhost',
                      help='the hostname of the server (default: %(default)s)')
  parser.add_argument('--port', type=int, default='31234',
                      help='the port number of the server (default: %(default)s)')
  parser.add_argument('--secret-file', type=str,
                      default='secret.json', help='the secret file path (default: %(default)s)')
  parser.add_argument('--color', type=str, default='yes',
                      choices=['yes', 'no'],
                      help='use colors (default: %(default)s)')

  parser.add_argument('-v', '--verbose', action='store_true', default=False, help='be verbose')

  subparsers = parser.add_subparsers(dest='command')

  for name, func in _ARG_EXPORTS.items():
    if not name.startswith('command_'):
      continue

    command = name.partition('_')[2]
    subparser = subparsers.add_parser(command, help=func.__doc__)

    arg_add_options(subparser, func)

  return parser


async def client_main(argv, loop):
  """Parse arguments and process a client command."""
  parser = make_parser()

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
  chain = []
  client_pool = {}
  for i, (args, unknown_args) in enumerate(args_list):
    # Run a handler
    handler = CommandHandler(
      common_args, args, unknown_args, pipe_break[i], chain, client_pool, loop)
    await handler.handle()
