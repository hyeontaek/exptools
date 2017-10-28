'''Provide the State class.'''

__all__ = ['State']

import asyncio
import json
import logging
import os

import aiofiles

from exptools.rpc_helper import rpc_export_function, rpc_export_generator

# pylint: disable=too-many-instance-attributes
class State:
  '''Manage a persistent state.'''

  def __init__(self, cls_name, path, loop):
    self.cls_name = cls_name
    self.path = path
    self.loop = loop

    self.logger = logging.getLogger(f'exptools.{cls_name}')

    self.lock = asyncio.Condition(loop=self.loop)

    self._dump_scheduled = False
    self._state = None
    self._load()

  def _initialize_state(self):
    '''Initialize the state.'''
    self._state = None

  def _serialize_state(self):
    '''Serialize the state for a dump.'''
    return self._state

  def _deserialize_state(self, state):
    '''Deserialize the state from a dump.'''
    self._state = state

  def _get_state(self):
    '''Return the state.'''
    assert self.lock.locked()
    return self._serialize_state()

  @rpc_export_function
  async def get_state(self):
    '''Return the state.'''
    async with self.lock:
      return self._get_state()

  def _load(self):
    '''Load the state file.'''
    if not os.path.exists(self.path):
      self._initialize_state()
      self.logger.warning(f'Initialized new state')
    else:
      state = json.load(open(self.path))
      self._deserialize_state(state)
      self.logger.info(f'Loaded state at {self.path}')

  def _schedule_dump(self):
    '''Schedule for a dump.'''
    self._dump_scheduled = True

  async def _dump(self):
    '''Dump the state to the persistent file.'''
    async with self.lock:
      if not self._dump_scheduled:
        return
      self._dump_scheduled = False

      state = self._serialize_state()

      data = json.dumps(state, sort_keys=True, indent=2)
      async with aiofiles.open(self.path + '.tmp', 'w') as file:
        await file.write(data)
      os.rename(self.path + '.tmp', self.path)
      self.logger.debug(f'Stored state at {self.path}')

  async def run_forever(self):
    '''Manage the state and dump the state as needed.'''
    try:
      while True:
        await self._dump()

        await asyncio.sleep(10, loop=self.loop)
    finally:
      await self._dump()

  async def notify(self):
    '''Notify any listeners to state changes.'''
    async with self.lock:
      self.lock.notify_all()

  @rpc_export_generator
  async def watch_state(self):
    '''Wait for any changes to the state.'''
    while True:
      async with self.lock:
        self.logger.debug(f'State change notified')
        yield self._get_state()
        await self.lock.wait()
