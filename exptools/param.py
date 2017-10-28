'''Provide the Param class.'''

__all__ = [
    'get_param_id',
    'get_hash_id',
    'make_hash_id',
    'get_name',
    'get_command',
    'get_cwd',
    'get_time_limit',
    'ParamBuilder',
    ]

import collections
import hashlib
import json

import base58

_HASH_FUNC = hashlib.blake2b # pylint: disable=no-member

def get_param_id(param):
  '''Return the parameter ID of a parameter.'''
  return param['_']['param_id']

def get_hash_id(param):
  '''Return the hash ID of a parameter.'''
  return param['_']['hash_id']

def make_hash_id(param):
  '''Calculate the hash ID of a parameter.'''
  filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
  param_str = json.dumps(filtered_param, sort_keys=True)
  #param_str = json.dumps(filtered_param, sort_keys=True, separators=(',', ':'))
  # Skip first few bytes because they are typically skewed to a few characters in base58
  return 'h-' + base58.b58encode(
      _HASH_FUNC(param_str.encode('utf-8')).digest())[3:3+20]

def get_name(param):
  '''Return the name of a parameter.'''
  name = None
  if '_' in param and 'name' in param['_']:
    name = param['_']['name']
  elif 'name' in param:
    name = param['name']
  if name is None:
    filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
    name = json.dumps(filtered_param, sort_keys=True)
  return name

def get_command(param):
  '''Return the command of a parameter.'''
  command = None
  if '_' in param and 'command' in param['_']:
    command = param['_']['command']
  elif 'command' in param:
    command = param['command']
  if command is None:
    raise RuntimeError(f'command must exists for parameter {get_param_id(param)}')
  return command

def get_cwd(param):
  '''Return the working directory of a parameter.'''
  cwd = None
  if '_' in param and 'cwd' in param['_']:
    cwd = param['_']['cwd']
  elif 'cwd' in param:
    cwd = param['cwd']
  return cwd

def get_time_limit(param):
  '''Return the time limit of a parameter.'''
  time_limit = 0
  if '_' in param and 'time_limit' in param['_']:
    time_limit = param['_']['time_limit']
  elif 'time_limit' in param:
    time_limit = param['time_limit']
  return time_limit

class ParamBuilder(collections.ChainMap):
  '''A parameter builder.'''
  def __add__(self, update):
    '''Return a new parameter with updates on top of the current parameter.'''
    child = self.new_child()
    child.update(update)
    return child
