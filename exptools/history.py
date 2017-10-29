'''Provide the History class.'''

__all__ = ['History']

from exptools.param import get_hash_id
from exptools.rpc_helper import rpc_export_function
from exptools.state import State

class History(State):
  '''Manage the history data of previous job execution.'''

  stub = {
      'queued': None,
      'started': None,
      'finished': None,
      'duration': None,
      'resources': None,
      'status': None,
      'succeeded': None,
      }

  def __init__(self, path, loop):
    super().__init__('History', path, loop)

  def _initialize_state(self):
    self._state = {}

  def _serialize_state(self):
    return self._state

  def _deserialize_state(self, state):
    self._state = state

  async def update(self, job):
    '''Record the job execution result.'''
    hash_id = get_hash_id(job['param'])

    hist_data = {}
    for key in self.stub:
      hist_data[key] = job[key]

    async with self.lock:
      self._state[hash_id] = hist_data

      self.logger.info(f'Updated history for parameter {hash_id}')
      self.lock.notify_all()
      self._schedule_dump()

  @rpc_export_function
  async def reset(self, hash_ids):
    '''Reset the job execution history.'''
    if not hash_ids:
      return hash_ids

    async with self.lock:
      for hash_id in hash_ids:
        self._state[hash_id] = dict(self.stub)
        self.logger.info(f'Reset history for parameter {hash_id}')

      self.lock.notify_all()
      self._schedule_dump()
    return hash_ids

  @rpc_export_function
  async def remove(self, hash_ids):
    '''Remove the job execution history.'''
    if not hash_ids:
      return hash_ids

    removed_hash_ids = []
    async with self.lock:
      for hash_id in hash_ids:
        if hash_id in self._state:
          del self._state[hash_id]
          self.logger.info(f'Removed history for parameter {hash_id}')
          removed_hash_ids.append(hash_id)

      self.lock.notify_all()
      self._schedule_dump()
    return removed_hash_ids

  @rpc_export_function
  async def migrate(self, hash_id_pairs):
    '''Migrate symlinks for hash ID changes.'''
    migrated_hash_id_pairs = []
    async with self.lock:
      for old_hash_id, new_hash_id in hash_id_pairs:
        if old_hash_id not in self._state:
          self.logger.info(f'Ignoring missing history for old parameter {old_hash_id}')
          continue

        if new_hash_id in self._state:
          self.logger.info(f'Ignoring existing history for new parameter {new_hash_id}')
          continue

        self._state[new_hash_id] = self._state[old_hash_id]
        self.logger.info(f'Migrated history of old parameter {old_hash_id} ' + \
                         f'to new parameter {new_hash_id}')
        migrated_hash_id_pairs.append((old_hash_id, new_hash_id))

      self.lock.notify_all()
      self._schedule_dump()
    return migrated_hash_id_pairs

  @rpc_export_function
  async def hash_ids(self):
    '''Get all hash IDs.'''
    async with self.lock:
      return list(self._state.keys())

  @rpc_export_function
  async def history_list(self, hash_ids):
    '''Get history data for given hash IDs.'''
    async with self.lock:
      history_list = []
      for hash_id in hash_ids:
        if hash_id in self._state:
          history = self._state[hash_id]
        else:
          history = self.stub
        history_list.append(history)
      return history_list

  @rpc_export_function
  async def history(self, hash_id):
    '''Get history data for the given hash ID.'''
    async with self.lock:
      if hash_id in self._state:
        history = self._state[hash_id]
      else:
        history = self.stub
      return history
