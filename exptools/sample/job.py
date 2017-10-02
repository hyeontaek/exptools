'''Example job definition.'''

import time

def job_func(param):
  '''Run param.'''
  time.sleep(1 + param['i'] * 0.1)
