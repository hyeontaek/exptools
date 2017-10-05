import logging
import time
import exptools

class SleepWork(exptools.Work):
  '''Define work to perform.'''

  def __init__(self, cluster):
    super().__init__()
    self.cluster = cluster
    self.logger = logging.getLogger('exptools.sample.SleepWork')

  def __str__(self):
    return 'SleepWork'

  # pylint: disable=unused-argument
  def setup(self, param):
    '''Set up to run a parameter.'''
    req = {'concurrency': 1, 'ps': 1, 'worker': 2}
    with self.lock:
      for key in req:
        if self.cluster[key] < req[key]:
          raise exptools.ResourceError()
      for key in req:
        self.cluster[key] -= req[key]
    self.logger.info(f'Taken: {req}')
    return {'holding': req}

  # pylint: disable=no-self-use, unused-argument
  def run(self, param, work_state):
    '''Run a parameter.'''
    time.sleep(1 + param['i'] * 0.1)

  # pylint: disable=unused-argument
  def cleanup(self, param, work_state):
    '''Clean up.'''
    req = work_state['holding']
    with self.lock:
      for key in req:
        self.cluster[key] += req[key]
      self.logger.info(f'Returned: {req}')
