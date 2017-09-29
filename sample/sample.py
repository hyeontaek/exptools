'''Example experiment definition.'''

import time
import pandas as pd
import matplotlib.pyplot as plt
from exptools.job import JobDef, OrderedDict
from exptools.history import HistoryManager
from exptools.runner import Runner

class MyJobDef(JobDef):
  '''Define my job.'''

  def format(self, param):
    return f"{param[0]}: i={param[1]['i']}"

  def demand(self, param):
    '''Require resources.'''
    return {'concurrency': 1, 'ps': 1, 'worker': 2}

  def run(self, param):
    '''Run param.'''
    #print(param['i'])
    time.sleep(1 + param[1]['i'] * 0.1)
    return True

def enum_params():
  '''Enumerate params.'''
  for i in range(4):
    yield ('my_job', OrderedDict({'i': i}))
    #yield ('my_job', {'i': i})

job_defs = {'my_job': MyJobDef()}
init_resources = {'concurrency': 2, 'ps': 2, 'worker': 6}

hist_mgr = HistoryManager(job_defs)
runner = Runner(job_defs, init_resources, hist_mgr)
runner.start()
