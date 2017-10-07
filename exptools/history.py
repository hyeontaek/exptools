'''Provide the History class.'''

__all__ = ['History']

from collections import OrderedDict
from threading import Lock
import os
import pickle
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
      self.history[exec_id]['duration'] = \
          diff_sec(now, self.history[exec_id]['started'])
      self.history[exec_id]['success'] = success
      if not defer_dump:
        self._dump()

  def get(self, param):
    '''Get a parameter's history data.'''
    stub = OrderedDict([
        ('started', None),
        ('finished', None),
        ('duration', None),
        ('success', None),
        ])
    with self.lock:
      return dict(self.history.get(param.exec_id, stub))

  def get_by_exec_id(self, exec_id):
    '''Get a parameter's history data.'''
    stub = OrderedDict([
        ('started', None),
        ('finished', None),
        ('duration', None),
        ('success', None),
        ])
    with self.lock:
      return dict(self.history.get(exec_id, stub))

  def add(self, param, hist_data, defer_dump=False):
    '''Add a parameter's history data manually.'''
    exec_id = param.exec_id
    with self.lock:
      self.history[exec_id] = hist_data
      if not defer_dump:
        self._dump()

  def remove(self, param, defer_dump=False):
    '''Remove a parameter's history data manually.'''
    exec_id = param.exec_id
    with self.lock:
      del self.history[exec_id]
      if not defer_dump:
        self._dump()

  def prune_absent(self, params, defer_dump=False):
    '''Remove history entries that are absent in parameters.'''
    with self.lock:
      valid_exec_ids = set([param.exec_id for param in params])

      self.history = {key: value for key, value in self.history.items() if key in valid_exec_ids}
      if not defer_dump:
        self._dump()

  def reset_finished(self, params, defer_dump=False):
    '''Remove finished data for parameters.'''
    with self.lock:
      for param in params:
        exec_id = param.exec_id
        if exec_id in self.history:
          self.history[exec_id]['finished'] = None
      if not defer_dump:
        self._dump()

  def get_df(self, time='datetime'):
    '''Return a dataframe for history data.'''
    import pandas as pd
    data = list(self.history.values())
    history_df = pd.DataFrame(data, columns=['started', 'finished', 'duration', 'success'])
    if time == 'utc':
      history_df['started'] = history_df['started']\
          .map(lambda v: format_utc(v) if v is not None else v)
      history_df['finished'] = history_df['finished']\
          .map(lambda v: format_utc(v) if v is not None else v)
    elif time == 'local':
      history_df['started'] = history_df['started']\
          .map(lambda v: format_local(v) if v is not None else v)
      history_df['finished'] = history_df['finished']\
          .map(lambda v: format_local(v) if v is not None else v)
    else:
      assert False, 'Unsupported timezone'
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
    data = list(params)
    for item in data:
      exec_id = item.exec_id
      hist_data = self.history.get(exec_id, stub)
      item.update({'_' + key: value for key, value in hist_data.items()})
    return pd.DataFrame(data, columns=['started', 'finished', 'duration', 'success'])

  def is_finished(self, param):
    '''Check if a parameter finished.'''
    return not self.is_unfinished(param)

  def is_unfinished(self, param):
    '''Check if a paramter did not finish.'''
    exec_id = param.exec_id
    with self.lock:
      return exec_id in self.history and \
          self.history[exec_id]['finished'] is None

  def omit_unfinished(self, params):
    '''Omit parameters that has not finished.'''
    empty = {'finished': None}
    with self.lock:
      return [param for param in params \
              if self.history.get(param.exec_id, empty)['finished'] is not None]

  def omit_finished(self, params):
    '''Omit parameters that has finished.'''
    empty = {'finished': None}
    with self.lock:
      return [param for param in params \
              if self.history.get(param.exec_id, empty)['finished'] is None]

  def is_succeeded(self, param):
    '''Check if a parameter succeeded.'''
    return not self.is_failed(param)

  def is_failed(self, param):
    '''Check if a paramter did not succeed.'''
    exec_id = param.exec_id
    with self.lock:
      return exec_id in self.history and \
          not self.history[exec_id]['success']

  def omit_failed(self, params):
    '''Omit parameters that has not succeeded.'''
    empty = {'success': None}
    with self.lock:
      return [param for param in params \
              if self.history.get(param.exec_id, empty)['success']]

  def omit_succeeded(self, params):
    '''Omit parameters that has succeeded.'''
    empty = {'success': None}
    with self.lock:
      return [param for param in params \
              if not self.history.get(param.exec_id, empty)['success']]
