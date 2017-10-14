'''Provide the Param class.'''

__all__ = [
    'get_param_id', 'get_param_ids',
    'get_exec_id', 'get_exec_ids',
    'get_name', 'get_names',
    ]

import hashlib
import json

import base58

_HASH_FUNC = hashlib.blake2b # pylint: disable=no-member

def get_param_id(param):
  '''Return the parameter ID of a parameter.'''
  param_str = json.dumps(param, sort_keys=True)
  # Skip first few bytes because they are typically skewed to a few characters in base58
  return 'p-' + base58.b58encode(
      _HASH_FUNC(param_str.encode('utf-8')).digest())[3:3+20]

def get_param_ids(params):
  '''Return the parameter IDs of parameters.'''
  return [get_param_id(param) for param in params]

def get_exec_id(param):
  '''Return the execution ID of a parameter.'''
  filtered_param = {key: value for key, value in param.items() if not key.startswith('_')}
  param_str = json.dumps(filtered_param, sort_keys=True)
  # Skip first few bytes because they are typically skewed to a few characters in base58
  return 'e-' + base58.b58encode(
      _HASH_FUNC(param_str.encode('utf-8')).digest())[3:3+20]

def get_exec_ids(params):
  '''Return the execution IDs of parameters.'''
  return [get_exec_id(param) for param in params]

def get_name(param):
  '''Return the name of a parameter.'''
  return param.get('_name', str(param))

def get_names(params):
  '''Return the names of parameters.'''
  return [get_name(param) for param in params]
