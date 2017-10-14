'''Provide file management functions.'''

__all__ = ['mkdirs', 'rmdirs', 'get_job_dir', 'get_exec_path']

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

def get_job_dir(base_dir, job):
  '''Get a path for a job.'''
  return os.path.join(base_dir, job['job_id'])

def get_exec_path(base_dir, exec_id):
  '''Get a path for an execution ID.'''
  return os.path.join(base_dir, exec_id)
