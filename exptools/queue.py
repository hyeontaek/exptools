"""Provide the Queue class."""

__all__ = ['Queue']

import asyncio
import collections
import concurrent
import copy

import base58

from exptools.param import get_param_id, get_hash_id, get_name
from exptools.rpc_helper import rpc_export_function, rpc_export_generator
from exptools.state import State
from exptools.time import diff_sec, utcnow, format_utc, parse_utc


class Queue(State):
  """Manage a job queue."""

  def __init__(self, path, registry, history, loop):
    super().__init__('Queue', path, loop)
    self.registry = registry
    self.history = history

    if not self._state['started_jobs'] and not self._state['queued_jobs']:
      self.logger.warning('Queue empty')

  @staticmethod
  def _make_ordered_dict(jobs):
    job_items = [(job['job_id'], job) for job in jobs]
    return collections.OrderedDict(job_items)

  def _initialize_state(self):
    self._state = {
      'finished_jobs': self._make_ordered_dict([]),
      'started_jobs': self._make_ordered_dict([]),
      'queued_jobs': self._make_ordered_dict([]),
      'concurrency': 1.,
      'next_job_id': 0,
    }

  def _serialize_state(self):
    return {
      'finished_jobs': list(self._state['finished_jobs'].values()),
      'started_jobs': list(self._state['started_jobs'].values()),
      'queued_jobs': list(self._state['queued_jobs'].values()),
      'concurrency': self._state['concurrency'],
      'next_job_id': self._state['next_job_id'],
    }

  def _deserialize_state(self, state):
    self._state = {
      'finished_jobs': self._make_ordered_dict(state['finished_jobs']),
      'started_jobs': self._make_ordered_dict(state['started_jobs']),
      'queued_jobs': self._make_ordered_dict(state['queued_jobs']),
      'concurrency': state['concurrency'],
      'next_job_id': state['next_job_id'],
    }

  async def run_forever(self):
    """Manage the queue."""

    # Make all started jobs at initialization time failed
    for job in list(self._state['started_jobs'].values()):
      await self.set_finished(job['job_id'], False)

    update_concurrency_task = (
      asyncio.ensure_future(self._update_concurrency(), loop=self.loop))

    try:
      await super().run_forever()

    finally:
      update_concurrency_task.cancel()
      try:
        await update_concurrency_task
      except concurrent.futures.CancelledError:
        # Ignore CancelledError because we caused it
        pass

  def _get_state_fast(self):
    """Return the state. Parameter details are removed."""
    assert self.lock.locked()
    state = self._serialize_state()

    for key in ['finished_jobs', 'started_jobs', 'queued_jobs']:
      for job in state[key]:
        param = copy.deepcopy(job['param'])
        param['_'] = {
          'param_id': get_param_id(param),
          'hash_id': get_hash_id(param),
          'name': get_name(param),
        }
    return state

  @rpc_export_function
  async def get_state_fast(self):
    """Return the state. Parameter details are removed."""
    async with self.lock:
      return self._get_state_fast()

  @rpc_export_generator
  async def watch_state_fast(self):
    """Wait for any changes to the state. Parameter details are removed."""
    while True:
      async with self.lock:
        self.logger.debug(f'State change notified')
        yield self._get_state_fast()
        await self.lock.wait()

  async def _update_concurrency(self):
    # Update the current concurrency
    alpha = 0.9

    while True:
      async with self.lock:
        self._state['concurrency'] = max(
          1.,
          alpha * self._state['concurrency'] + (1. - alpha) * len(self._state['started_jobs']))

      await asyncio.sleep(10, loop=self.loop)

  def _get_next_job_id(self):
    """Return the next job ID."""
    assert self.lock.locked()
    next_job_id = self._state['next_job_id']
    self._state['next_job_id'] = next_job_id + 1
    self._schedule_dump()
    return 'j-' + base58.b58encode_int(next_job_id)

  def _check_empty(self):
    assert self.lock.locked()
    if not self._state['started_jobs'] and not self._state['queued_jobs']:
      self.logger.warning('Queue empty')

  @rpc_export_function
  async def add(self, param_ids, append=True):
    """Add parameters as queued jobs."""
    if not param_ids:
      return []

    params = await self.registry.params(param_ids)

    now = format_utc(utcnow())
    job_ids = []
    async with self.lock:
      new_jobs = []
      for param in params:
        job_id = self._get_next_job_id()
        job_ids.append(job_id)

        job = {
          'job_id': job_id,
          'param': param,
          'queued': now,
          'started': None,
          'finished': None,
          'duration': None,
          'resources': None,
          'pid': None,
          'status': None,
          'succeeded': None,
        }
        new_jobs.append(job)

      new_jobs_dict = self._make_ordered_dict(new_jobs)

      if append:
        self._state['queued_jobs'].update(new_jobs_dict)
      else:
        new_jobs_dict.update(self._state['queued_jobs'])
        self._state['queued_jobs'] = new_jobs_dict

      self.logger.info(f'Added {len(params)} jobs')
      self.lock.notify_all()
      self._schedule_dump()
    return job_ids

  async def set_started(self, job_id):
    """Mark a queued job as started."""
    now = format_utc(utcnow())
    async with self.lock:
      if job_id not in self._state['queued_jobs']:
        return False

      job = self._state['queued_jobs'][job_id]
      job['started'] = now

      self._state['started_jobs'][job_id] = job
      del self._state['queued_jobs'][job_id]

      self.logger.info(f'Started job {job_id}')
      self.lock.notify_all()
      self._schedule_dump()
      return True

  async def set_resources(self, job_id, resources):
    """Update resources of a queued job."""
    # Do not acquire the lock because this is called by scheduler,
    # which holds the lock it its schedule() loop
    assert self.lock.locked()

    if job_id not in self._state['queued_jobs']:
      return False

    job = self._state['queued_jobs'][job_id]
    job['resources'] = resources
    self.logger.info(f'Updated job {job_id} resources')
    self._schedule_dump()
    return True

  async def set_pid(self, job_id, pid):
    """Update pid of a started job."""
    async with self.lock:
      if job_id not in self._state['started_jobs']:
        return False

      job = self._state['started_jobs'][job_id]
      job['pid'] = pid
      self.logger.info(f'Updated job {job_id} pid {pid}')
      self._schedule_dump()
      return True

  async def set_status(self, job_id, status):
    """Update status of a started job."""
    async with self.lock:
      if job_id not in self._state['started_jobs']:
        return False

      job = self._state['started_jobs'][job_id]
      job['status'] = status
      self.logger.info(f'Updated job {job_id} status')
      self.lock.notify_all()
      self._schedule_dump()
      return True

  async def set_finished(self, job_id, succeeded):
    """Mark an started job as finished."""
    now = format_utc(utcnow())
    async with self.lock:
      if job_id not in self._state['started_jobs']:
        return False

      job = self._state['started_jobs'][job_id]
      job['finished'] = now
      job['duration'] = (
        diff_sec(parse_utc(now), parse_utc(job['started'])))
      job['pid'] = None
      job['succeeded'] = succeeded

      if succeeded:
        await self.history.update(job)

      self._state['finished_jobs'][job_id] = job
      del self._state['started_jobs'][job_id]

      if succeeded:
        self.logger.info(f'Finished job {job_id} [succeeded]')
      else:
        self.logger.warning(f'Finished job {job_id} [FAILED]')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return True

  @rpc_export_function
  async def move(self, job_ids, offset):
    """Reorder queued jobs."""
    assert isinstance(offset, int)

    if not job_ids:
      return job_ids
    if offset == 0:
      return []

    async with self.lock:
      existing_job_ids = set(list(self._state['queued_jobs'].keys()))
      for job_id in job_ids:
        if job_id not in existing_job_ids:
          raise KeyError(f'Job not found: {job_id}')

      job_ids_set = set(job_ids)

      current_job_ids = list(self._state['queued_jobs'].keys())

      if offset < 0:
        reverse = False
      else:
        reverse = True
        current_job_ids.reverse()
        offset = -offset

      # Limit offset to reduce memory use required for sentinels
      if offset < -len(current_job_ids):
        offset = -len(current_job_ids)

      # Add sentinels
      current_job_ids = [None] * -offset + current_job_ids

      # Shift matching job IDs
      for i in range(-offset, len(current_job_ids)):
        if current_job_ids[i] in job_ids_set:
          current_job_ids[i + offset:i + 1] = [current_job_ids[i]] + current_job_ids[i + offset:i]

      # Remove sentinels
      current_job_ids = list(filter(lambda job_id: job_id is not None, current_job_ids))

      if reverse:
        current_job_ids.reverse()

      # Get job ID order index
      order = dict(zip(current_job_ids, range(len(current_job_ids))))

      # Apply the new order
      def _sort_key(job):
        return order[job['job_id']]

      self._state['queued_jobs'] = self._make_ordered_dict(
        sorted(self._state['queued_jobs'].values(), key=_sort_key))

      self.logger.info(f'Reordered {len(job_ids)} jobs')
      self.lock.notify_all()
      self._schedule_dump()
      return job_ids

  @rpc_export_function
  async def remove_finished(self, job_ids):
    """Remove finished jobs."""
    if not job_ids:
      return job_ids

    async with self.lock:
      existing_job_ids = set(list(self._state['finished_jobs'].keys()))
      for job_id in job_ids:
        if job_id not in existing_job_ids:
          raise KeyError(f'Job not found: {job_id}')

      job_ids_set = set(job_ids)

      def _filter_func(job):
        return job['job_id'] not in job_ids_set

      self._state['finished_jobs'] = self._make_ordered_dict(
        filter(_filter_func, self._state['finished_jobs'].values()))

      self.logger.info(f'Removed {len(job_ids)} finished jobs')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return job_ids

  @rpc_export_function
  async def remove_queued(self, job_ids):
    """Remove queued jobs."""
    if not job_ids:
      return job_ids

    async with self.lock:
      existing_job_ids = set(list(self._state['queued_jobs'].keys()))
      for job_id in job_ids:
        if job_id not in existing_job_ids:
          raise KeyError(f'Job not found: {job_id}')

      job_ids_set = set(job_ids)

      def _filter_func(job):
        return job['job_id'] not in job_ids_set

      self._state['queued_jobs'] = self._make_ordered_dict(
        filter(_filter_func, self._state['queued_jobs'].values()))

      self.logger.info(f'Removed {len(job_ids)} queued jobs')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return job_ids

  @rpc_export_function
  async def job_ids(self, job_types=('finished', 'started', 'queued')):
    """Return job IDs in the queue."""
    async with self.lock:
      job_ids = []
      for job_type in job_types:
        job_ids.extend(list(self._state[job_type + '_jobs'].keys()))
      return job_ids

  @rpc_export_function
  async def jobs(self, job_ids, job_types=('finished', 'started', 'queued')):
    """Return jobs that match given job IDs."""
    async with self.lock:
      jobs = []
      for job_id in job_ids:
        job = None
        for job_type in job_types:
          if job_id in self._state[job_type + '_jobs']:
            job = self._state[job_type + '_jobs'][job_id]
            break
        jobs.append(job)
      return jobs

  @rpc_export_function
  async def job(self, job_id, job_types=('finished', 'started', 'queued')):
    """Return a job that match the given job ID."""
    jobs = await self.jobs([job_id], job_types=job_types)
    assert jobs[0] is not None
    return jobs[0]
