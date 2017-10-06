import logging
import time
import exptools

class SleepWork(exptools.Work):
  '''Define work to perform.'''

  def __init__(self, cluster):
    self.cluster = cluster
    self.logger = logging.getLogger('exptools.sample.SleepWork')

  def __str__(self):
    return 'SleepWork'

  # pylint: disable=unused-argument
  def setup(self, job):
    '''Set up to run a job.'''
    req = {'concurrency': 1, 'ps': 1, 'worker': 2}
    with self.cluster[0]:
      for key in req:
        if self.cluster[1][key] < req[key]:
          raise exptools.ResourceError()
      for key in req:
        self.cluster[1][key] -= req[key]
    self.logger.info(f'Taken: {req}')
    job.state['holding'] = req

  # pylint: disable=no-self-use, unused-argument
  def run(self, job):
    '''Run a job.'''
    time.sleep(1 + job.param['i'] * 0.1)

  # pylint: disable=unused-argument
  def cleanup(self, job):
    '''Clean up.'''
    req = job.state.get('holding', {})
    with self.cluster[0]:
      for key in req:
        self.cluster[1][key] += req[key]
      self.logger.info(f'Returned: {req}')
