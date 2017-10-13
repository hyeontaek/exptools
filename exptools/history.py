'''Provide the History class.'''

__all__ = ['History']

import asyncio
import os
import pickle

import aiofiles

from exptools.param import get_exec_id
from exptools.rpc_helper import rpc_export_function
from exptools.time import diff_sec, parse_utc

class History:
  '''Manage the history data of previous job execution.'''

  def __init__(self, path, loop):
    self.path = path
    self.loop = loop

    self.lock = asyncio.Lock()
    self.history = {}

    self._load()

  def __del__(self):
    self.loop.run_until_complete(asyncio.ensure_future(self._dump(), loop=self.loop))

  def _load(self):
    '''Load history data.'''
    if self.path and os.path.exists(self.path):
      with open(self.path, 'rb') as file:
        self.history = pickle.load(file)
    else:
      self.history = {}

  async def _dump(self):
    '''Store history data.'''
    assert self.lock.locked() # pylint: disable=no-member

    if self.path:
      data = pickle.dumps(self.history)
      async with aiofiles.open(self.path + '.tmp', 'wb') as file:
        await file.write(data)
      os.rename(self.path + '.tmp', self.path)

  @rpc_export_function
  async def dump(self):
    '''Store history data.'''
    async with self.lock:
      await self._dump()

  @rpc_export_function
  async def set_queued(self, exec_id, now):
    '''Record started time.'''
    async with self.lock:
      if exec_id not in self.history:
        self.history[exec_id] = {
            'queued': None,
            'started': None,
            'finished': None,
            'duration': None,
            'succeeded': None,
            }
      self.history[exec_id]['queued'] = now
      self.history[exec_id]['started'] = None
      self.history[exec_id]['finished'] = None
      # Keep duration for Estimator
      self.history[exec_id]['succeeded'] = None
      await self._dump()

  @rpc_export_function
  async def set_started(self, exec_id, now):
    '''Record started time.'''
    async with self.lock:
      if exec_id not in self.history:
        self.history[exec_id] = {
            'queued': None,
            'started': None,
            'finished': None,
            'duration': None,
            'succeeded': None,
            }
      self.history[exec_id]['started'] = now
      self.history[exec_id]['finished'] = None
      # Keep duration for Estimator
      self.history[exec_id]['succeeded'] = None
      await self._dump()

  @rpc_export_function
  async def set_finished(self, exec_id, succeeded, now):
    '''Record finished time and result.'''
    async with self.lock:
      if exec_id not in self.history:
        self.history[exec_id] = {
            'queued': None,
            'started': None,
            'finished': None,
            'duration': None,
            'succeeded': None,
            }
      self.history[exec_id]['finished'] = now
      if self.history[exec_id]['started'] is not None:
        self.history[exec_id]['duration'] = \
            diff_sec(parse_utc(now), parse_utc(self.history[exec_id]['started']))
      else:
        self.history[exec_id]['duration'] = None
      self.history[exec_id]['succeeded'] = succeeded
      await self._dump()

  @rpc_export_function
  async def get_all(self):
    '''Get all history data.'''
    async with self.lock:
      return {exec_id: self.history[exec_id] for exec_id in self.history}

  @rpc_export_function
  async def get(self, exec_id):
    '''Get a parameter's history data.'''
    stub = {
        'queued': None,
        'started': None,
        'finished': None,
        'duration': None,
        'succeeded': None,
        }
    async with self.lock:
      return self.history.get(exec_id, stub)

  @rpc_export_function
  async def add(self, exec_id, hist_data, *, defer_dump=False):
    '''Add a parameter's history data manually.'''
    async with self.lock:
      self.history[exec_id] = hist_data
      if not defer_dump:
        await self._dump()

  @rpc_export_function
  async def remove(self, exec_id, *, defer_dump=False):
    '''Remove a parameter's history data manually.'''
    async with self.lock:
      del self.history[exec_id]
      if not defer_dump:
        await self._dump()

  @rpc_export_function
  async def prune_absent(self, exec_ids, *, defer_dump=False):
    '''Remove history entries that are absent in parameters.'''
    async with self.lock:
      valid_exec_ids = set(exec_ids)
      self.history = {exec_id: hist_entry
                      for exec_id, hist_entry in self.history.items() if exec_id in valid_exec_ids}
      if not defer_dump:
        await self._dump()

  #@rpc_export_function
  #async def reset(self, exec_ids, *, defer_dump=False):
  #  '''Remove finished data for parameters.'''
  #  async with self.lock:
  #    for exec_id in exec_ids:
  #      if exec_id in self.history:
  #        self.history[exec_id]['finished'] = None
  #        self.history[exec_id]['succeeded'] = None
  #    if not defer_dump:
  #      await self._dump()

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

    empty = {'finished': None, 'succeeded': None}
    async with self.lock:
      if not only_succeeded:
        params = filter(
            lambda param: not self.history.get(get_exec_id(param), empty)['finished'], params)
      else:
        params = filter(
            lambda param: not self.history.get(get_exec_id(param), empty)['succeeded'], params)
      return list(params)
