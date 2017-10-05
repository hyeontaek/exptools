'''Provide file management functions.'''

__all__ = ['mkdirs', 'rmdirs', 'get_param_dir']

import os
import shutil

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

def get_param_dir(prefix, param):
  '''Get a path for a parameter.'''
  return prefix + param.exec_id
