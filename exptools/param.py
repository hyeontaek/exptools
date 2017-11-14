"""Provide the Param class."""

__all__ = [
  'get_param_id',
  'get_hash_id',
  'make_hash_id',
  'make_unique_id',
  'get_property',
  'get_name',
  'get_command',
  'get_cwd',
  'get_time_limit',
  'ParamBuilder',
  'ParamListBuilder',
]

import collections
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
  # Make a copy because key '_' will be replaced
  filtered_param = dict(param)
  if '_' in filtered_param:
    del filtered_param['_']
  param_str = json.dumps(filtered_param, sort_keys=True)
  # param_str = json.dumps(filtered_param, sort_keys=True, separators=(',', ':'))
  return 'u-' + base58.b58encode(_HASH_FUNC(param_str.encode('utf-8')).digest())


def get_property(param, key, default):
  """Return a property of a parameter."""
  value = default
  if '__' + key in param:
    value = param['__' + key]
  elif '_' + key in param:
    value = param['_' + key]
  elif key in param:
    value = param[key]
  return value


def get_name(param):
  """Return the name of a parameter."""
  name = get_property(param, 'name', None)
  if name is None:
    filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
    name = json.dumps(filtered_param, sort_keys=True)
  return name


def get_command(param):
  """Return the command of a parameter."""
  command = get_property(param, 'command', None)
  if command is None:
    raise RuntimeError(f'command must exists for parameter {get_param_id(param)}')
  return command


def get_cwd(param):
  """Return the working directory of a parameter."""
  return get_property(param, 'cwd', None)


def get_retry(param):
  """Return the retry of a parameter."""
  return get_property(param, 'retry', 0)


def get_retry_delay(param):
  """Return the retry delay of a parameter."""
  return get_property(param, 'retry_delay', 0)


def get_time_limit(param):
  """Return the time limit of a parameter."""
  return get_property(param, 'time_limit', 0)


class ParamBuilder(collections.ChainMap):
  """A parameter builder that facilitates incremental parameter construction."""

  def __add__(self, other):
    """Return a new ParamBuilder with updates on top of the current parameter."""
    assert isinstance(other, ParamBuilder)
    return ParamBuilder(*(other.maps + self.maps))

  def __mul__(self, other):
    """Return a new ParamListBuilder that is a cross product of self and other."""
    assert isinstance(other, (ParamBuilder, ParamListBuilder))
    return ParamListBuilder([self]) * other

  def apply(self, map_func, *, in_place=False):
    """Apply a map function."""
    if in_place:
      map_func(self)
      return self

    new_param = map_func(self)
    if isinstance(new_param, ParamBuilder):
      return new_param
    elif isinstance(new_param, ParamListBuilder):
      return new_param
    else:
      raise TypeError('Unrecognized return type: ' + type(new_param).__name__)
    return new_param

  def copy(self):
    """Return a new copy of self."""
    return ParamBuilder({}, *self.maps)

  def flatten_copy(self):
    """Return a new flattened copy of self."""
    return ParamBuilder(dict(self))

  def flatten(self):
    """Return a flattened version of self."""
    if len(self.maps) > 1:
      return ParamBuilder(dict(self))
    return self

  def finalize(self):
    """Return a parameter that is converted to dict."""
    if len(self.maps) > 1:
      return dict(self)
    return self.maps[0]


class ParamListBuilder(list):
  """A list of parameter builders."""

  def __add__(self, other):
    """Return a new ParamListBuilder that combines self and other."""
    assert isinstance(other, ParamListBuilder)
    return ParamListBuilder(super().__add__(other))

  def __mul__(self, other):
    """Return a new ParamListBuilder that is a cross product of self and other."""
    assert isinstance(other, (ParamBuilder, ParamListBuilder))

    pbl = ParamListBuilder()
    if isinstance(other, ParamListBuilder):
      for param in self:
        for o_param in other:
          pbl.append(param + o_param)
    else:
      for param in self:
        pbl.append(param + other)
    return pbl

  def __getitem__(self, index):
    """Return an item at index or a new ParamListBuilder containing items in the range."""
    if isinstance(index, slice):
      return ParamListBuilder(super().__getitem__(index))
    return super().__getitem__(index)

  def apply(self, map_func, *, in_place=False):
    """Apply a map function."""
    if in_place:
      for param in self:
        map_func(param)
      return self

    new_params = ParamListBuilder()
    for param in self:
      new_param = map_func(param)
      if new_param is None:
        continue
      elif isinstance(new_param, ParamBuilder):
        new_params.append(new_param)
      elif isinstance(new_param, ParamListBuilder):
        new_params.extend(new_param)
      else:
        raise TypeError('Unrecognized return type: ' + type(new_param).__name__)
    return new_params

  def copy(self):
    """Return a new copy of self."""
    return ParamListBuilder(self)

  def flatten_copy(self):
    """Return a new flattened copy of self."""
    return ParamListBuilder([param.flatten_copy() for param in self])

  def flatten(self):
    """Return a flattened version of self."""
    return ParamListBuilder([param.flatten() for param in self])

  def finalize(self):
    """Return a list of parameters that are converted to dict."""
    return [param.finalize() for param in self]
