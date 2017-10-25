'''Provide the Queue class.'''

__all__ = ['Queue']

import asyncio
import collections
import json
import logging
import os

import aiofiles
import base58

from exptools.param import get_param_id, get_name, get_command, get_cwd, get_time_limit
from exptools.rpc_helper import rpc_export_function, rpc_export_generator
from exptools.time import diff_sec, utcnow, format_utc, parse_utc

# pylint: disable=too-many-instance-attributes
class Queue:
  '''Manage a job queue.'''

  def __init__(self, path, history, loop):
    self.path = path
    self.history = history
    self.loop = loop

    self.logger = logging.getLogger('exptools.Queue')

    self.lock = asyncio.Condition(loop=self.loop)
    self._dump_scheduled = False
    self._load()
    self._ensure_no_started_jobs = True

  async def run_forever(self):
    '''Manage the queue.'''

    if self._ensure_no_started_jobs:
      self._ensure_no_started_jobs = False

      # Make all started jobs at initialization time failed
      for job in list(self.state['started_jobs'].values()):
        await self.set_finished(job['job_id'], False)

    try:
      while True:
        async with self.lock:
          # Update the current concurrency
          alpha = 0.9
          self.state['concurrency'] = max(
              1.,
              alpha * self.state['concurrency'] + (1. - alpha) * len(self.state['started_jobs']))

        await self._dump()

        await asyncio.sleep(10, loop=self.loop)
    finally:
      await self._dump()

  @staticmethod
  def _make_ordered_dict(jobs):
    job_items = [(job['job_id'], job) for job in jobs]
    return collections.OrderedDict(job_items)

  def _load(self):
    '''Load the queue file.'''
    if os.path.exists(self.path):
      state = json.load(open(self.path))
      self.state = {
          'finished_jobs': self._make_ordered_dict(state['finished_jobs']),
          'started_jobs': self._make_ordered_dict(state['started_jobs']),
          'queued_jobs': self._make_ordered_dict(state['queued_jobs']),
          'concurrency': state['concurrency'],
          'next_job_id': state['next_job_id'],
          }
      self.logger.info(f'Loaded queue state at {self.path}')
    else:
      self.state = {
          'finished_jobs': self._make_ordered_dict([]),
          'started_jobs': self._make_ordered_dict([]),
          'queued_jobs': self._make_ordered_dict([]),
          'concurrency': 1.,
          'next_job_id': 0,
          }
      self.logger.info(f'Initialized new queue state')

    if not self.state['started_jobs'] and not self.state['queued_jobs']:
      self.logger.warning('Queue empty')

  def _schedule_dump(self):
    self._dump_scheduled = True

  async def _dump(self):
    '''Dump the current state to the queue file.'''
    async with self.lock:
      if not self._dump_scheduled:
        return
      self._dump_scheduled = False

      state = {
          'finished_jobs': list(self.state['finished_jobs'].values()),
          'started_jobs': list(self.state['started_jobs'].values()),
          'queued_jobs': list(self.state['queued_jobs'].values()),
          'concurrency': self.state['concurrency'],
          'next_job_id': self.state['next_job_id'],
          }
      data = json.dumps(state, sort_keys=True, indent=2)
      async with aiofiles.open(self.path + '.tmp', 'w') as file:
        await file.write(data)
      os.rename(self.path + '.tmp', self.path)
      self.logger.info(f'Stored state data at {self.path}')

  async def _get_next_job_id(self):
    '''Return the next job ID.'''
    assert self.lock.locked()
    next_job_id = self.state['next_job_id']
    self.state['next_job_id'] = next_job_id + 1
    self._schedule_dump()
    return 'j-' + base58.b58encode_int(next_job_id)

  def _get_state(self):
    '''Get the job queue state.'''
    assert self.lock.locked()
    state = {
        'finished_jobs': self._clone_jobs(self.state['finished_jobs']),
        'started_jobs': self._clone_jobs(self.state['started_jobs']),
        'queued_jobs': self._clone_jobs(self.state['queued_jobs']),
        'concurrency': self.state['concurrency'],
        }
    return state

  @staticmethod
  def _clone_jobs(jobs):
    '''Clone a job list.'''
    return [dict(job) for job in jobs.values()]

  @rpc_export_function
  async def get_state(self):
    '''Get the job queue state.'''
    async with self.lock:
      return self._get_state()

  @rpc_export_generator
  async def watch_state(self):
    '''Wait for any changes to the queue and call the function with the state.'''
    while True:
      async with self.lock:
        yield self._get_state()
        await self.lock.wait()

  async def notify(self):
    '''Notify listeners of the queue for a potential change.'''
    async with self.lock:
      self.lock.notify_all()

  @rpc_export_function
  async def omit(self, params, *, queued=True, started=True, finished=True):
    '''Omit parameters that are already in the queue.'''
    param_ids = set()
    async with self.lock:
      if queued:
        param_ids.update(map(lambda job: job['param_id'], self.state['queued_jobs'].values()))
      if started:
        param_ids.update(map(lambda job: job['param_id'], self.state['started_jobs'].values()))
      if finished:
        param_ids.update(map(lambda job: job['param_id'], self.state['finished_jobs'].values()))
    return list(filter(lambda param: get_param_id(param) not in param_ids, params))

  @rpc_export_function
  async def add(self, params, append=True):
    '''Add parameters as queued jobs.'''
    now = format_utc(utcnow())
    job_ids = []
    async with self.lock:
      new_jobs = []
      for param in params:
        param_id = get_param_id(param)

        job_id = await self._get_next_job_id()
        job_ids.append(job_id)

        new_jobs.append({
            'job_id': job_id,
            'param_id': param_id,
            'name': get_name(param),
            'command': get_command(param),
            'cwd': get_cwd(param) or os.getcwd(),
            'time_limit': get_time_limit(param),
            'param': param,
            'queued': now,
            'started': None,
            'finished': None,
            'duration': None,
            'resources': None,
            'pid': None,
            'status': None,
            'succeeded': None,
            })

      new_jobs_dict = self._make_ordered_dict(new_jobs)

      if append:
        self.state['queued_jobs'].update(new_jobs_dict)
      else:
        new_jobs_dict.update(self.state['queued_jobs'])
        self.state['queued_jobs'] = new_jobs_dict

      self.logger.info(f'Added {len(params)} jobs')
      self.lock.notify_all()
      self._schedule_dump()
    return job_ids

  async def set_started(self, job_id):
    '''Mark a queued job as started.'''
    now = format_utc(utcnow())
    async with self.lock:
      if job_id not in self.state['queued_jobs']:
        return False

      job = self.state['queued_jobs'][job_id]
      job['started'] = now

      self.state['started_jobs'][job_id] = job
      del self.state['queued_jobs'][job_id]

      self.logger.info(f'Started job {job_id}')
      self.lock.notify_all()
      self._schedule_dump()
      return True

  async def set_resources(self, job_id, resources):
    '''Update resources of a queued job.'''
    # Do not acuquire the lock because this is called by scheduler,
    # which holds the lock it its schedule() loop
    assert self.lock.locked()

    if job_id not in self.state['queued_jobs']:
      return False

    job = self.state['queued_jobs'][job_id]
    job['resources'] = resources
    self.logger.info(f'Updated job {job_id} resources')
    self._schedule_dump()
    return True

  async def set_pid(self, job_id, pid):
    '''Update pid of a started job.'''
    async with self.lock:
      if job_id not in self.state['started_jobs']:
        return False

      job = self.state['started_jobs'][job_id]
      job['pid'] = pid
      self.logger.info(f'Updated job {job_id} pid {pid}')
      self._schedule_dump()
      return True

  async def set_status(self, job_id, status):
    '''Update status of a started job.'''
    async with self.lock:
      if job_id not in self.state['started_jobs']:
        return False

      job = self.state['started_jobs'][job_id]
      job['status'] = status
      self.logger.info(f'Updated job {job_id} status')
      self.lock.notify_all()
      self._schedule_dump()
      return True

  async def set_finished(self, job_id, succeeded):
    '''Mark an started job as finished.'''
    now = format_utc(utcnow())
    async with self.lock:
      if job_id not in self.state['started_jobs']:
        return False

      job = self.state['started_jobs'][job_id]
      job['finished'] = now
      job['duration'] = \
          diff_sec(parse_utc(now), parse_utc(job['started']))
      job['pid'] = None
      job['succeeded'] = succeeded

      if succeeded:
        await self.history.update(job)

      self.state['finished_jobs'][job_id] = job
      del self.state['started_jobs'][job_id]

      if succeeded:
        self.logger.info(f'Finished job {job_id} [suceeded]')
      else:
        self.logger.warning(f'Finished job {job_id} [FAILED]')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return True

  @rpc_export_function
  async def move(self, job_ids, offset):
    '''Reorder queued jobs.'''
    if offset > 0:
      offset += 0.5
    elif offset < 0:
      offset -= 0.5

    affected_count = 0
    async with self.lock:
      current_job_ids = self.state['queued_jobs'].keys()
      job_count = len(self.state['queued_jobs'])

      order = dict(zip(current_job_ids, range(job_count)))

      for job_id in job_ids:
        if job_id in order:
          order[job_id] += offset
          affected_count += 1

      sort_key = lambda job: order[job['job_id']]
      self.state['queued_jobs'] = self._make_ordered_dict(
          sorted(self.state['queued_jobs'].values(), key=sort_key))

      self.logger.info(f'Reordered {affected_count} jobs')
      self.lock.notify_all()
      self._schedule_dump()
      return affected_count

  @rpc_export_function
  async def remove_finished(self, job_ids=None):
    '''Remove finished jobs.'''
    async with self.lock:
      prev_count = len(self.state['finished_jobs'])
      if job_ids is None:
        self.state['finished_jobs'].clear()
      else:
        job_ids = set(job_ids)
        filter_func = lambda job: job['job_id'] not in job_ids
        self.state['finished_jobs'] = self._make_ordered_dict(
            filter(filter_func, self.state['finished_jobs'].values()))
      new_count = len(self.state['finished_jobs'])
      self.logger.info(f'Removed {prev_count - new_count} finished jobs')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return prev_count - new_count

  @rpc_export_function
  async def remove_queued(self, job_ids=None):
    '''Remove queued jobs.'''
    async with self.lock:
      prev_count = len(self.state['queued_jobs'])
      if job_ids is None:
        self.state['queued_jobs'].clear()
      else:
        job_ids = set(job_ids)
        filter_func = lambda job: job['job_id'] not in job_ids
        self.state['queued_jobs'] = self._make_ordered_dict(
            filter(filter_func, self.state['queued_jobs'].values()))
      new_count = len(self.state['queued_jobs'])
      self.logger.info(f'Removed {prev_count - new_count} queued jobs')
      self.lock.notify_all()
      self._check_empty()
      self._schedule_dump()
      return prev_count - new_count

  def _check_empty(self):
    assert self.lock.locked()
    if not self.state['started_jobs'] and not self.state['queued_jobs']:
      self.logger.warning('Queue empty')
