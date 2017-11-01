"""Provide the Param class."""

__all__ = [
  'get_param_id',
  'get_hash_id',
  'make_hash_id',
  'make_unique_id',
  'get_name',
  'get_command',
  'get_cwd',
  'get_time_limit',
  'ParamBuilder',
]

import collections
import copy
import hashlib
import json

import base58

_HASH_FUNC = hashlib.blake2b  # pylint: disable=no-member

HISTORY_STUB = {
  'queued': None,
  'started': None,
  'finished': None,
  'duration': None,
  'resources': None,
  'status': None,
  'succeeded': None,
}


def get_param_id(param):
  """Return the parameter ID of a parameter."""
  return param['_']['param_id']


def get_hash_id(param):
  """Return the hash ID of a parameter."""
  return param['_']['hash_id']


def make_hash_id(param):
  """Calculate the hash ID of a parameter."""
  filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
  param_str = json.dumps(filtered_param, sort_keys=True)
  # param_str = json.dumps(filtered_param, sort_keys=True, separators=(',', ':'))
  # Skip first few bytes because they are typically skewed to a few characters in base58
  return 'h-' + base58.b58encode(
    _HASH_FUNC(param_str.encode('utf-8')).digest())[3:3 + 20]


def make_unique_id(param):
  """Calculate the unique ID of a parameter for equality tests."""
  filtered_param = copy.deepcopy(param)
  if '_' in filtered_param:
    meta = filtered_param['_']
    for key in ['param_id', 'hash_id'] + list(HISTORY_STUB.keys()):
      if key in meta:
        del meta[key]
  param_str = json.dumps(filtered_param, sort_keys=True)
  # param_str = json.dumps(filtered_param, sort_keys=True, separators=(',', ':'))
  return 'u-' + base58.b58encode(_HASH_FUNC(param_str.encode('utf-8')).digest())


def _get_property(param, key, default):
  """Return a property of a parameter."""
  value = default
  if '_' in param and key in param['_']:
    value = param['_'][key]
  elif key in param:
    value = param[key]
  return value


def get_name(param):
  """Return the name of a parameter."""
  name = _get_property(param, 'name', None)
  if name is None:
    filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
    name = json.dumps(filtered_param, sort_keys=True)
  return name


def get_command(param):
  """Return the command of a parameter."""
  command = _get_property(param, 'command', None)
  if command is None:
    raise RuntimeError(f'command must exists for parameter {get_param_id(param)}')
  return command


def get_cwd(param):
  """Return the working directory of a parameter."""
  return _get_property(param, 'cwd', None)


def get_retry(param):
  """Return the retry of a parameter."""
  return _get_property(param, 'retry', 0)


def get_retry_delay(param):
  """Return the retry delay of a parameter."""
  return _get_property(param, 'retry_delay', 0)


def get_time_limit(param):
  """Return the time limit of a parameter."""
  return _get_property(param, 'time_limit', 0)


class ParamBuilder(collections.ChainMap):
  """A parameter builder."""

  def __add__(self, update):
    """Return a new parameter with updates on top of the current parameter."""
    child = self.new_child()
    child.update(update)
    return child
