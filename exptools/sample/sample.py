'''Example experiment definition.'''

import importlib
import logging
import sys
import pandas as pd
import matplotlib.pyplot as plt
from exptools import Runner, History, pprint
import exptools.sample.cluster
import exptools.sample.param
import exptools.sample.work

logging_fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
logging.basicConfig(format=logging_fmt, level=logging.INFO, stream=sys.__stderr__)

hist = History()

runner = Runner(hist)
runner.start()

cluster = exptools.sample.cluster.get_cluster()

def load():
  '''Reset the work and return all parameters.'''
  importlib.reload(exptools.sample.param)
  importlib.reload(exptools.sample.work)
  return exptools.sample.work.SleepWork(cluster), exptools.sample.param.enum_params()
