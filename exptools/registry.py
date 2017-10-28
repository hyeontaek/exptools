'''Provide the Registry class.'''

__all__ = ['Registry']

import collections
import copy

import base58

from exptools.param import get_hash_id, make_hash_id
from exptools.rpc_helper import rpc_export_function
from exptools.state import State

# pylint: disable=too-many-instance-attributes
class Registry(State):
  '''Manage a parameter registry.'''

  def __init__(self, path, loop):
    super().__init__('Registry', path, loop)
    self._build_index()

  def _initialize_state(self):
    self._state = {
        'paramsets': collections.OrderedDict(),
        'params': collections.OrderedDict(),
        'next_param_id': 0,
        }

  def _serialize_state(self):
    return {
        'paramsets': list(self._state['paramsets'].items()),
        'params': list(self._state['params'].values()),
        'next_param_id': self._state['next_param_id'],
        }

  def _deserialize_state(self, state):
    self._state = {
        'paramsets': collections.OrderedDict(state['paramsets']),
        'params': collections.OrderedDict(
            [(param['_']['param_id'], param) for param in state['params']]),
        'next_param_id': state['next_param_id'],
        }

  def _build_index(self):
    self._hash_id_index = {}
    for param_id, param in self._state['params'].items():
      hash_id = param['_']['hash_id']
      if hash_id not in self._hash_id_index:
        self._hash_id_index[hash_id] = [param_id]
      else:
        self._hash_id_index[hash_id].append(param_id)

  def _get_next_param_id(self):
    '''Return the next parameter ID.'''
    assert self.lock.locked()
    next_param_id = self._state['next_param_id']
    self._state['next_param_id'] = next_param_id + 1
    self._schedule_dump()
    return 'p-' + base58.b58encode_int(next_param_id)

  async def _add(self, paramset, params):
    '''Add parameters.'''
    assert self.lock.locked()

    if not params:
      return 0

    if paramset in self._state['paramsets']:
      raise RuntimeError(f'Parameter set already exists: {paramset}')

    param_ids = []
    for param in params:
      param = copy.deepcopy(param)
      if '_' not in param:
        param['_'] = {}

      param_id = self._get_next_param_id()
      hash_id = make_hash_id(param)

      param['_']['param_id'] = param_id
      param['_']['hash_id'] = hash_id

      self._state['params'][param_id] = param
      param_ids.append(param_id)

      if hash_id not in self._hash_id_index:
        self._hash_id_index[hash_id] = [param_id]
      else:
        self._hash_id_index[hash_id].append(param_id)

    self._state['paramsets'][paramset] = param_ids

    self.logger.info(f'Added {len(params)} parameters')
    self.lock.notify_all()
    self._schedule_dump()
    return len(params)

  @rpc_export_function
  async def add(self, paramset, params):
    '''Add parameters.'''
    async with self.lock:
      return await self._add(paramset, params)

  async def _remove(self, paramset):
    '''Remove parameters.'''
    assert self.lock.locked()

    param_ids = self._state['paramsets'][paramset]

    for param_id in param_ids:
      param = self._state['params'][param_id]

      hash_id = get_hash_id(param)
      pos = self._hash_id_index[hash_id].index(param_id)
      assert pos != -1
      del self._hash_id_index[hash_id][pos]
      if not self._hash_id_index[hash_id]:
        del self._hash_id_index[hash_id]

      del self._state['params'][param_id]

    del self._state['paramsets'][paramset]

    self.logger.info(f'Removed {len(param_ids)} parameters')
    self.lock.notify_all()
    self._schedule_dump()
    return len(param_ids)

  @rpc_export_function
  async def remove(self, paramset):
    '''Remove parameters.'''
    async with self.lock:
      return await self._remove(paramset)

  @rpc_export_function
  async def paramsets(self):
    '''Get all parameter sets.'''
    async with self.lock:
      return list(self._state['paramsets'].keys())

  @rpc_export_function
  async def paramset(self, paramset):
    '''Get parameter IDs by parameter set.'''
    async with self.lock:
      return self._state['paramsets'][paramset]

  @rpc_export_function
  async def param_ids(self):
    '''Get all parameter IDs.'''
    async with self.lock:
      return list(self._state['params'].keys())

  @rpc_export_function
  async def params(self, param_ids):
    '''Get all parameters by parameter IDs.'''
    async with self.lock:
      params = [self._state['params'][param_id] for param_id in param_ids]
      return params

  @rpc_export_function
  async def param(self, param_id):
    '''Get a parameter by parameter ID.'''
    async with self.lock:
      return self._state['params'][param_id]

  @rpc_export_function
  async def hash_ids(self):
    '''Get all hash IDs.'''
    async with self.lock:
      return list(self._hash_id_index.keys())

  @rpc_export_function
  async def param_ids_by_hash_ids(self, hash_ids):
    '''Get all parameter IDs by hash IDs.'''
    async with self.lock:
      param_ids_list = []
      for hash_id in hash_ids:
        if hash_id in self._hash_id_index:
          param_ids_list.append(self._hash_id_index[hash_id])
        else:
          param_ids_list.append([])
      return param_ids_list

  @rpc_export_function
  async def param_ids_by_hash_id(self, hash_id):
    '''Get all parameter IDs by a hash ID.'''
    return await self.param_ids_by_hash_ids([hash_id])[0]
