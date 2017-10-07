'''Provide the History class.'''

__all__ = ['History']

from collections import OrderedDict
from threading import Lock
import os
import pickle
from exptools.param import Param
from exptools.time import diff_sec, format_local, format_utc, utcnow

class History:
  '''Manage the history data of previous job execution.'''

  def __init__(self, path='hist.dat', pickler=pickle.Pickler, unpickler=pickle.Unpickler):
    self.path = path
    self.pickler = pickler
    self.unpickler = unpickler

    self.lock = Lock()
    self.history = {}
    self._load()

  def _load(self):
    '''Load history data.'''
    if self.path and os.path.exists(self.path):
      with open(self.path, 'rb') as file:
        self.history = self.unpickler(file).load()
    else:
      self.history = {}

  def _dump(self):
    '''Store history data.'''
    assert self.lock.locked() # pylint: disable=no-member

    if self.path:
      with open(self.path + '.tmp', 'wb') as file:
        self.pickler(file).dump(self.history)
      os.rename(self.path + '.tmp', self.path)

  def dump(self):
    '''Store history data.'''
    with self.lock:
      self._dump()

  def started(self, param, defer_dump=False):
    '''Record started time.'''
    exec_id = param.exec_id
    now = utcnow()
    with self.lock:
      if exec_id not in self.history:
        self.history[exec_id] = OrderedDict([
            ('started', None),
            ('finished', None),
            ('duration', None),
            ('success', None),
            ])
      self.history[exec_id]['started'] = now
      self.history[exec_id]['finished'] = None
      # Keep duration for Estimator
      self.history[exec_id]['success'] = None
      if not defer_dump:
        self._dump()

  def finished(self, param, success, defer_dump=False):
    '''Record finished time and result.'''
    exec_id = param.exec_id
    now = utcnow()
    with self.lock:
      if exec_id not in self.history:
        self.history[exec_id] = OrderedDict([
            ('started', None),
            ('finished', None),
            ('duration', None),
            ('success', None),
            ])
      self.history[exec_id]['finished'] = now
      if self.history[exec_id]['started'] is not None:
        self.history[exec_id]['duration'] = \
            diff_sec(now, self.history[exec_id]['started'])
      else:
        self.history[exec_id]['duration'] = None
      self.history[exec_id]['success'] = success
      if not defer_dump:
        self._dump()

  @staticmethod
  def _exec_id(param_or_exec_id):
    if isinstance(param_or_exec_id, Param):
      return param_or_exec_id.exec_id
    return param_or_exec_id

  def get_all(self):
    '''Get all history data.'''
    with self.lock:
      return {exec_id: self.history[exec_id] for exec_id in self.history}

  def get(self, param):
    '''Get a parameter's history data.'''
    stub = OrderedDict([
        ('started', None),
        ('finished', None),
        ('duration', None),
        ('success', None),
        ])
    with self.lock:
      return dict(self.history.get(self._exec_id(param), stub))

  def add(self, param_or_exec_id, hist_data, defer_dump=False):
    '''Add a parameter's history data manually.'''
    exec_id = self._exec_id(param_or_exec_id)
    with self.lock:
      self.history[exec_id] = hist_data
      if not defer_dump:
        self._dump()

  def remove(self, param_or_exec_id, defer_dump=False):
    '''Remove a parameter's history data manually.'''
    exec_id = self._exec_id(param_or_exec_id)
    with self.lock:
      del self.history[exec_id]
      if not defer_dump:
        self._dump()

  def prune_absent(self, param_or_exec_ids, defer_dump=False):
    '''Remove history entries that are absent in parameters.'''
    with self.lock:
      valid_exec_ids = set([self._exec_id(param_or_exec_id)
                            for param_or_exec_id in param_or_exec_ids])

      self.history = {exec_id: hist_entry
                      for exec_id, hist_entry in self.history.items() if exec_id in valid_exec_ids}
      if not defer_dump:
        self._dump()

  def reset_finished(self, param_or_exec_ids, defer_dump=False):
    '''Remove finished data for parameters.'''
    with self.lock:
      for param_or_exec_id in param_or_exec_ids:
        exec_id = self._exec_id(param_or_exec_id)
        if exec_id in self.history:
          self.history[exec_id]['finished'] = None
      if not defer_dump:
        self._dump()

  def get_df(self, time='datetime'):
    '''Return a dataframe for history data.'''
    import pandas as pd
    with self.lock:
      data = list(self.history.values())
      history_df = pd.DataFrame(data, columns=['started', 'finished', 'duration', 'success'])
      if time == 'utc':
        history_df['started'] = history_df['started']\
            .map(lambda v: format_utc(v) if not pd.isnull(v) else None)
        history_df['finished'] = history_df['finished']\
            .map(lambda v: format_utc(v) if not pd.isnull(v) else None)
      elif time == 'local':
        history_df['started'] = history_df['started']\
            .map(lambda v: format_local(v) if not pd.isnull(v) else None)
        history_df['finished'] = history_df['finished']\
            .map(lambda v: format_local(v) if not pd.isnull(v) else None)
      elif time != 'datetime':
        assert False, f'Unsupported timezone: {time}'
      return history_df

  def get_joined_df(self, params):
    '''Return a dataframe that joins parameters and history data on exec_id.'''
    import pandas as pd
    stub = OrderedDict([
        ('started', None),
        ('finished', None),
        ('duration', None),
        ('success', None),
        ])
    with self.lock:
      data = list(params)
      for param in data:
        hist_data = self.history.get(param.exec_id, stub)
        param.update({'_' + key: value for key, value in hist_data.items()})
      return pd.DataFrame(data, columns=['started', 'finished', 'duration', 'success'])

  def is_finished(self, param_or_exec_id):
    '''Check if a parameter finished.'''
    return not self.is_unfinished(param_or_exec_id)

  def is_unfinished(self, param_or_exec_id):
    '''Check if a paramter did not finish.'''
    exec_id = self._exec_id(param_or_exec_id)
    with self.lock:
      return exec_id in self.history and \
          self.history[exec_id]['finished'] is None

  def omit_unfinished(self, param_or_exec_ids):
    '''Omit parameters that has not finished.'''
    empty = {'finished': None}
    with self.lock:
      return [param_or_exec_id for param_or_exec_id in param_or_exec_ids \
              if self.history.get(self._exec_id(param_or_exec_id), empty)['finished'] is not None]

  def omit_finished(self, param_or_exec_ids):
    '''Omit parameters that has finished.'''
    empty = {'finished': None}
    with self.lock:
      return [param_or_exec_id for param_or_exec_id in param_or_exec_ids \
              if self.history.get(self._exec_id(param_or_exec_id), empty)['finished'] is None]

  def is_succeeded(self, param_or_exec_id):
    '''Check if a parameter succeeded.'''
    return not self.is_failed(param_or_exec_id)

  def is_failed(self, param_or_exec_id):
    '''Check if a paramter did not succeed.'''
    exec_id = self._exec_id(param_or_exec_id)
    with self.lock:
      return exec_id in self.history and \
          not self.history[exec_id]['success']

  def omit_failed(self, param_or_exec_ids):
    '''Omit parameters that has not succeeded.'''
    empty = {'success': None}
    with self.lock:
      return [param_or_exec_id for param_or_exec_id in param_or_exec_ids \
              if self.history.get(self._exec_id(param_or_exec_id), empty)['success']]

  def omit_succeeded(self, param_or_exec_ids):
    '''Omit parameters that has succeeded.'''
    empty = {'success': None}
    with self.lock:
      return [param_or_exec_id for param_or_exec_id in param_or_exec_ids \
              if not self.history.get(self._exec_id(param_or_exec_id), empty)['success']]
