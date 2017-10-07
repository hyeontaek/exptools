'''Provide file management functions.'''

__all__ = ['mkdirs', 'rmdirs', 'get_param_dir']

import os
import shutil
from exptools.param import Param

def mkdirs(path, ignore_errors=True):
  '''Make directories recursively.'''
  if ignore_errors:
    try:
      os.makedirs(path)
    except FileExistsError:
      pass
  else:
    os.makedirs(path)

def rmdirs(path, ignore_errors=True):
  '''Remove directories recursively.'''
  shutil.rmtree(path, ignore_errors=ignore_errors)

def get_param_dir(prefix, param_or_exec_id):
  '''Get a path for a parameter.'''
  if isinstance(param_or_exec_id, Param):
    return prefix + param_or_exec_id.exec_id
  return prefix + param_or_exec_id
