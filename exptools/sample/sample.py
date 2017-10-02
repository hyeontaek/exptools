'''Example experiment definition.'''

import importlib
import logging
import sys
import pandas as pd
import matplotlib.pyplot as plt
from exptools import Runner, History
import exptools.sample.job
import exptools.sample.param

logging_fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
logging.basicConfig(format=logging_fmt, level=logging.INFO, stream=sys.__stderr__)

init_resources = {'concurrency': 2, 'ps': 2, 'worker': 6}

hist = History()

runner = Runner(exptools.sample.job.job_func, init_resources, hist)
runner.start()

def get_all_params():
  '''Reset the job function and return all parameters.'''
  importlib.reload(exptools.sample.job)
  importlib.reload(exptools.sample.param)
  runner.set_job_func(exptools.sample.job.job_func)
  return exptools.sample.param.enum_params()
