'''Provide the History class.'''

__all__ = ['History']

import asyncio
import json
import os
import logging

import aiofiles
import base58

from exptools.param import get_param_id
from exptools.rpc_helper import rpc_export_function
from exptools.time import diff_sec, parse_utc

class History:
  '''Manage the history data of previous job execution.'''

  stub = {
      'job_id': None,
      'queued': None,
      'started': None,
      'finished': None,
      'duration': None,
      'succeeded': None,
      }

  def __init__(self, path, loop):
    self.path = path
    self.loop = loop

    self.logger = logging.getLogger('exptools.History')

    self.lock = asyncio.Lock()
    self._load()

    self.dump_future = None

  def __del__(self):
    if self.dump_future is not None:
      self.loop.run_until_complete(self.dump_future)

  def _load(self):
    '''Load the history file.'''
    if os.path.exists(self.path):
      self.history = json.load(open(self.path, 'r'))
      self.logger.info(f'Loaded history data at {self.path}')
    else:
      self.history = {'next_job_id': 0}
      self.logger.info(f'Initialized new history data')

  def _schedule_dump(self):
    '''Schedule a dump operation.'''
    assert self.lock.locked()

    if self.dump_future is None or self.dump_future.done():
      # Schedule a new operation only if there is no finished future
      self.dump_future = asyncio.ensure_future(self._dump(), loop=self.loop)

  async def _dump(self):
    '''Dump the current history to the history file.'''
    async with self.lock:
      data = json.dumps(self.history, sort_keys=True, indent=2)
      async with aiofiles.open(self.path + '.tmp', 'w') as file:
        await file.write(data)
      os.rename(self.path + '.tmp', self.path)
      self.logger.debug(f'Stored history data at {self.path}')

  async def get_next_job_id(self):
    '''Return the next job ID.'''
    async with self.lock:
      next_job_id = self.history['next_job_id']
      self.history['next_job_id'] = next_job_id + 1
      return 'j-' + base58.b58encode_int(next_job_id)

  async def update(self, job):
    '''Record finished time and result.'''
    param_id = job['param_id']
    async with self.lock:
      hist_data = self._get(param_id)
      for key in self.stub:
        hist_data[key] = job[key]
      self.history[param_id] = hist_data
      self.logger.info(f'Updated history entry for parameter {param_id}')
      self._schedule_dump()

  @rpc_export_function
  async def get_all(self, param_ids=None):
    '''Get all history entries.'''
    async with self.lock:
      if param_ids is None:
        return {param_id: self._get(param_id) \
                for param_id in self.history if param_id.startswith('p-')}

      param_ids = set(param_ids)
      return {param_id: self._get(param_id) for param_id in param_ids}

  @rpc_export_function
  async def get(self, param_id):
    '''Get a parameter's history entry.'''
    async with self.lock:
      return self._get(param_id)

  def _get(self, param_id):
    '''Get a parameter's history entry.'''
    assert self.lock.locked()

    if param_id in self.history:
      return dict(self.history[param_id])
    return dict(self.stub)

  @rpc_export_function
  async def add(self, param_id, hist_data):
    '''Add a parameter's history entries manually.'''
    async with self.lock:
      self.history[param_id] = hist_data
      self._schedule_dump()

  @rpc_export_function
  async def remove(self, param_id):
    '''Remove a parameter's history entries manually.'''
    async with self.lock:
      del self.history[param_id]
      self._schedule_dump()

  @rpc_export_function
  async def migrate(self, changes):
    '''Migrate symlinks for parameter ID changes.'''
    count = 0
    async with self.lock:
      for old_param_id, new_param_id in changes:
        if old_param_id not in self.history:
          self.logger.info(f'Ignoring missing history entry for old parameter {old_param_id}')
          continue

        if new_param_id in self.history:
          self.logger.info(f'Ignoring existing history entry for new parameter {new_param_id}')
          continue

        self.history[new_param_id] = self.history[old_param_id]
        self.logger.info(f'Migrated history entry of old parameter {old_param_id} ' + \
                         f'to new parameter {new_param_id}')
        count += 1
    return count

  @rpc_export_function
  async def prune(self, param_ids, *, prune_matching=False, prune_mismatching=False):
    '''Remove history entries.'''
    entry_count = 0
    param_ids = set(param_ids)
    async with self.lock:
      for param_id in list(self.history.keys()):
        if not param_id.startswith('p-'):
          continue

        if (prune_matching and param_id in param_ids) or \
           (prune_mismatching and param_id not in param_ids):
          del self.history[param_id]
          self.logger.info(f'Removed history entry of parameter {param_id}')
          entry_count += 1

      self._schedule_dump()

    return entry_count

  @rpc_export_function
  async def is_finished(self, param_id):
    '''Check if a parameter finished.'''
    async with self.lock:
      return param_id in self.history and self.history[param_id]['finished']

  @rpc_export_function
  async def is_succeded(self, param_id):
    '''Check if a paramter did not succeed.'''
    async with self.lock:
      return param_id in self.history and self.history[param_id]['succeeded']

  @rpc_export_function
  async def omit(self, params, *, only_succeeded=False):
    '''Omit parameters that has finished (and succeeded), leavning unfinished (or failed) ones.'''
    async with self.lock:
      if not only_succeeded:
        params = filter(lambda param: not self._get(get_param_id(param))['finished'], params)
      else:
        params = filter(lambda param: not self._get(get_param_id(param))['succeeded'], params)
      return list(params)
