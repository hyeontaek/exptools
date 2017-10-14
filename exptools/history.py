'''Provide the History class.'''

__all__ = ['History']

import asyncio
import json
import os

import aiofiles
import base58

from exptools.param import get_exec_id
from exptools.rpc_helper import rpc_export_function
from exptools.time import diff_sec, parse_utc

class History:
  '''Manage the history data of previous job execution.'''

  stub = {
      'queued': None,
      'started': None,
      'finished': None,
      'duration': None,
      'succeeded': None,
      }

  def __init__(self, path, loop):
    self.path = path
    self.loop = loop

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
    else:
      self.history = {'next_job_id': 0}

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

  async def get_next_job_id(self):
    '''Return the next job ID.'''
    async with self.lock:
      next_job_id = self.history['next_job_id']
      self.history['next_job_id'] = next_job_id + 1
      return 'j-' + base58.b58encode_int(next_job_id)

  @rpc_export_function
  async def set_queued(self, exec_id, now):
    '''Record started time.'''
    async with self.lock:
      hist_data = self._get(exec_id)
      hist_data['queued'] = now
      hist_data['started'] = None
      hist_data['finished'] = None
      # Keep duration for Estimator
      hist_data['succeeded'] = None

      self.history[exec_id] = hist_data
      self._schedule_dump()

  @rpc_export_function
  async def set_started(self, exec_id, now):
    '''Record started time.'''
    async with self.lock:
      hist_data = self._get(exec_id)
      hist_data['started'] = now
      hist_data['finished'] = None
      # Keep duration for Estimator
      hist_data['succeeded'] = None

      self.history[exec_id] = hist_data
      self._schedule_dump()

  @rpc_export_function
  async def set_finished(self, exec_id, succeeded, now):
    '''Record finished time and result.'''
    async with self.lock:
      hist_data = self._get(exec_id)
      hist_data['finished'] = now
      if hist_data['started'] is not None:
        hist_data['duration'] = \
            diff_sec(parse_utc(now), parse_utc(hist_data['started']))
      hist_data['succeeded'] = succeeded

      self.history[exec_id] = hist_data
      self._schedule_dump()

  @rpc_export_function
  async def get_all(self):
    '''Get all history data.'''
    async with self.lock:
      return {exec_id: dict(self.history[exec_id]) \
              for exec_id in self.history if exec_id.startswith('e-')}

  @rpc_export_function
  async def get(self, exec_id):
    '''Get a parameter's history data.'''
    async with self.lock:
      return self._get(exec_id)

  def _get(self, exec_id):
    '''Get a parameter's history data.'''
    assert self.lock.locked()

    if exec_id in self.history:
      return self.history[exec_id]
    return dict(self.stub)

  @rpc_export_function
  async def add(self, exec_id, hist_data):
    '''Add a parameter's history data manually.'''
    async with self.lock:
      self.history[exec_id] = hist_data
      self._schedule_dump()

  @rpc_export_function
  async def remove(self, exec_id):
    '''Remove a parameter's history data manually.'''
    async with self.lock:
      del self.history[exec_id]
      self._schedule_dump()

  @rpc_export_function
  async def prune(self, exec_ids, *, prune_matching=False, prune_mismatching=False):
    '''Remove history entries.'''
    entry_count = 0
    exec_ids = set(exec_ids)
    async with self.lock:
      for exec_id in list(self.history.keys()):
        if not exec_id.startswith('e-'):
          continue

        if (prune_matching and exec_id in exec_ids) or \
           (prune_mismatching and exec_id not in exec_ids):
          del self.history[exec_id]
          entry_count += 1

      self._schedule_dump()

    return entry_count

  @rpc_export_function
  async def is_finished(self, exec_id):
    '''Check if a parameter finished.'''
    async with self.lock:
      return exec_id in self.history and self.history[exec_id]['finished']

  @rpc_export_function
  async def is_succeded(self, exec_id):
    '''Check if a paramter did not succeed.'''
    async with self.lock:
      return exec_id in self.history and self.history[exec_id]['succeeded']

  @rpc_export_function
  async def omit(self, params, *, only_succeeded=False):
    '''Omit parameters that has finished (and succeeded), leavning unfinished (or failed) ones.'''
    async with self.lock:
      if not only_succeeded:
        params = filter(lambda param: not self._get(get_exec_id(param))['finished'], params)
      else:
        params = filter(lambda param: not self._get(get_exec_id(param))['succeeded'], params)
      return list(params)
