'''Provide the Work class.'''

__all__ = ['ResourceError', 'Work']

from threading import Lock

class ResourceError(RuntimeError):
  '''An exception that indicates required resources are unavailable to run a parameter.'''
  pass

class Work:
  '''Define work to perform.'''

  def __init__(self):
    self.lock = Lock()

  def __str__(self):
    return 'Work'

  # pylint: disable=no-self-use, unused-argument
  def setup(self, param):
    '''Set up to run a parameter.'''
    return None

  # pylint: disable=no-self-use, unused-argument
  def run(self, param, work_state):
    '''Run a parameter.'''
    pass

  # pylint: disable=no-self-use, unused-argument
  def cleanup(self, param, work_state):
    '''Clean up.'''
    pass
