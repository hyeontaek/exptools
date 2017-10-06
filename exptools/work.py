'''Provide the Work class.'''

__all__ = ['ResourceError', 'Work']

class ResourceError(RuntimeError):
  '''An exception that indicates required resources are unavailable to run a job.'''
  pass

class Work:
  '''Define work to perform.'''

  def __str__(self):
    return 'Work'

  # pylint: disable=no-self-use, unused-argument
  def setup(self, job):
    '''Set up to run a job.'''
    return None

  # pylint: disable=no-self-use, unused-argument
  def run(self, job):
    '''Run a job.'''
    pass

  # pylint: disable=no-self-use, unused-argument
  def kill(self, job):
    '''Kill processes running a job.'''
    pass

  # pylint: disable=no-self-use, unused-argument
  def cleanup(self, job):
    '''Clean up.'''
    pass
