'''Provide the Param class.'''

__all__ = [
    'get_param_id',
    'get_name',
    'get_command',
    'get_cwd',
    ]

import hashlib
import json

import base58

_HASH_FUNC = hashlib.blake2b # pylint: disable=no-member

def get_param_id(param):
  '''Return the parameter ID of a parameter.'''
  filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
  param_str = json.dumps(filtered_param, sort_keys=True)
  # Skip first few bytes because they are typically skewed to a few characters in base58
  return 'p-' + base58.b58encode(
      _HASH_FUNC(param_str.encode('utf-8')).digest())[3:3+20]

def get_name(param):
  '''Return the name of a parameter.'''
  name = None
  if '_' in param and 'name' in param['_']:
    name = param['_']['name']
  elif 'name' in param:
    name = param['name']
  if name is None:
    name = str(param)
  return name

def get_command(param):
  '''Return the command of a parameter.'''
  command = None
  if '_' in param and 'command' in param['_']:
    return param['_']['command']
  elif 'command' in param:
    command = param['command']
  if command is None:
    raise RuntimeError(f'command must exists for parameter {get_param_id(param)}')
  return command

def get_cwd(param):
  '''Return the working directory of a parameter.'''
  cwd = None
  if '_' in param and 'cwd' in param['_']:
    return param['_']['cwd']
  elif 'cwd' in param:
    cwd = param['cwd']
  return cwd
