'''Provide the Job class.'''

__all__ = ['Job']

class Job:
  '''Represent a job.'''

  def __init__(self, job_id, param):
    self.job_id = job_id
    self.param = param

  def __str__(self):
    '''Format a job.'''
    return f'[{self.param.priority}/{self.job_id}] {self.param}'

  def sort_key(self):
    '''Return a sort key.'''
    return (self.param.priority, self.job_id)
