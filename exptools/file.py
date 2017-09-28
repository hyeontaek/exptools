'''Provide file management functions.'''

import os
import shutil

__all__ = ['mkdirs', 'rmdirs', 'get_param_dir']

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

def get_param_dir(prefix, job_defs, param):
  '''Get a path for param.'''
  return os.path.join(prefix, job_defs[param[0]].hash(param))
