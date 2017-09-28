'''Provides HistoryManager.'''

from threading import Lock
import datetime
import os
import pickle
import pytz
import tzlocal

__all__ = ['HistoryManager']

class HistoryManager:
  '''Manage the history data of job execution.'''

  def __init__(self, job_defs, path='hist.dat', pickler=pickle.Pickler, unpickler=pickle.Unpickler):
    self.job_defs = job_defs
    self.path = path
    self.pickler = pickler
    self.unpickler = unpickler

    self.lock = Lock()
    self.history = {}
    self._load()

  def _load(self):
    '''Load history data.'''
    if os.path.exists(self.path):
      with open(self.path, 'rb') as file:
        self.history = self.unpickler(file).load()
    else:
      self.history = {}

  def _dump(self):
    '''Store history data.'''
    assert self.lock.locked()

    with open(self.path + '.tmp', 'wb') as file:
      self.pickler(file).dump(self.history)
    os.rename(self.path + '.tmp', self.path)

  def started(self, param):
    '''Record started time.'''
    with self.lock:
      param_hash = self.job_defs[param[0]].hash(param)

      now = datetime.datetime.utcnow()

      if param_hash not in self.history:
        self.history[param_hash] = {
            'param': param,
            'started': now, 'finished': None,
            'duration': None, 'success': None
            }
      else:
        self.history[param_hash]['started'] = now
        self.history[param_hash]['finished'] = None
        # Keep duration for Estimator
        self.history[param_hash]['success'] = None
      self._dump()

  def finished(self, param, success):
    '''Record finished time and result.'''
    with self.lock:
      param_hash = self.job_defs[param[0]].hash(param)

      now = datetime.datetime.utcnow()

      self.history[param_hash]['finished'] = now
      self.history[param_hash]['duration'] = \
          (now - self.history[param_hash]['started']).total_seconds()
      self.history[param_hash]['success'] = success
      self._dump()

  @staticmethod
  def utc_time(utc_time):
    '''Format a UTC time using the UTC timezone.'''
    return utc_time.strftime('%Y-%m-%d %H:%M:%S')

  @staticmethod
  def local_time(utc_time):
    '''Format a UTC time using the local timezone.'''
    return utc_time.replace(tzinfo=pytz.utc).astimezone(
        tzlocal.get_localzone()).strftime('%Y-%m-%d %H:%M:%S')

  def df_utc(self):
    '''Return a dataframe using the UTC timezone.'''
    import pandas as pd
    data = list(self.history.values())
    history_df = pd.DataFrame(data, columns=data[0].keys())
    history_df['started'] = history_df['started']\
        .map(lambda v: self.utc_time(v) if v else v)
    history_df['finished'] = history_df['finished']\
        .map(lambda v: self.utc_time(v) if v else v)
    return history_df

  def df_local(self):
    '''Return a dataframe using the local timezone.'''
    import pandas as pd
    data = list(self.history.values())
    history_df = pd.DataFrame(data, columns=data[0].keys())
    history_df['started'] = history_df['started']\
        .map(lambda v: self.local_time(v) if v else v)
    history_df['finished'] = history_df['finished']\
        .map(lambda v: self.local_time(v) if v else v)
    return history_df

  def prune_absent(self, params):
    '''Remove history entries that are absent in params.'''
    with self.lock:
      valid_hashes = set([self.job_defs[param[0]].hash(param) for param in params])

      self.history = {h: self.history[h] for h in self.history if h in valid_hashes}
      self._dump()

  def reset_finished(self, params):
    '''Remove finished data for params.'''
    with self.lock:
      for param in params:
        param_hash = self.job_defs[param[0]].hash(param)

        if param_hash in self.history:
          self.history[param_hash]['finished'] = None

  def remove_finished(self, params):
    '''Remove finished params.'''
    with self.lock:
      empty = {}
      return [param for param in params
              if self.history\
                  .get(self.job_defs[param[0]].hash(param), empty)\
                  .get('finished', None) is None]

  def get(self, param):
    '''Get param's history data.'''
    with self.lock:
      stub = {
          'param': param,
          'started': None, 'finished': None,
          'duration': None, 'success': None}
      return dict(self.history.get(self.job_defs[param[0]].hash(param), stub))
