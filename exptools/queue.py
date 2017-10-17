'''Provide the Queue class.'''

__all__ = ['Queue']

import asyncio
import json
import logging
import os

import aiofiles
import base58

from exptools.param import get_param_id, get_name, get_command, get_cwd
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

  async def run_forever(self):
    '''Manage the queue.'''
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

  def _load(self):
    '''Load the queue file.'''
    if os.path.exists(self.path):
      self.state = json.load(open(self.path))
      self.logger.info(f'Loaded queue state at {self.path}')
    else:
      self.state = {
          'finished_jobs': [],
          'started_jobs': [],
          'queued_jobs': [],
          'concurrency': 1.,
          'next_job_id': 0,
          }
      self.logger.info(f'Initialized new queue state')

    # Make all started jobs failed
    for job in list(self.state['started_jobs']):
      self.loop.run_until_complete(self.set_finished(job['job_id'], False))

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
      data = json.dumps(self.state, sort_keys=True, indent=2)
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
    return [dict(job) for job in jobs]

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
        param_ids.update(map(lambda job: get_param_id(job['param']), self.state['queued_jobs']))
      if started:
        param_ids.update(map(lambda job: get_param_id(job['param']), self.state['started_jobs']))
      if finished:
        param_ids.update(map(lambda job: get_param_id(job['param']), self.state['finished_jobs']))
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
            'param': param,
            'queued': now,
            'started': None,
            'finished': None,
            'duration': None,
            'pid': None,
            'succeeded': None,
            })

      if append:
        self.state['queued_jobs'] = self.state['queued_jobs'] + new_jobs
      else:
        self.state['queued_jobs'] = new_jobs + self.state['queued_jobs']

      self.logger.info(f'Added {len(params)} jobs')
      self.lock.notify_all()
      self._schedule_dump()
    return job_ids

  async def set_started(self, job_id):
    '''Mark a queued job as started.'''
    now = format_utc(utcnow())
    async with self.lock:
      for i, job in enumerate(self.state['queued_jobs']):
        if job['job_id'] == job_id:
          job['started'] = now
          job['pid'] = None

          self.state['started_jobs'].append(job)
          del self.state['queued_jobs'][i]

          self.logger.info(f'Started job {job_id}')
          self.lock.notify_all()
          self._schedule_dump()
          return True
    return False

  async def set_pid(self, job_id, pid):
    '''Update pid of a started job.'''
    async with self.lock:
      for job in self.state['started_jobs']:
        if job['job_id'] == job_id:
          job['pid'] = pid

          self.logger.info(f'Updated job {job_id} pid {pid}')
          self._schedule_dump()
          return True
    return False

  async def set_finished(self, job_id, succeeded):
    '''Mark an started job as finished.'''
    now = format_utc(utcnow())
    async with self.lock:
      for i, job in enumerate(self.state['started_jobs']):
        if job['job_id'] == job_id:
          job['finished'] = now
          job['duration'] = \
              diff_sec(parse_utc(now), parse_utc(job['started']))
          job['pid'] = None
          job['succeeded'] = succeeded

          await self.history.update(job)

          self.state['finished_jobs'].append(job)
          del self.state['started_jobs'][i]

          if succeeded:
            self.logger.info(f'Finished job {job_id} [suceeded]')
          else:
            self.logger.warning(f'Finished job {job_id} [FAILED]')
          self.lock.notify_all()
          self._check_empty()
          self._schedule_dump()
          return True
    return False

  @rpc_export_function
  async def reorder(self, job_ids):
    '''Reorder queued jobs.'''
    job_count = len(job_ids)
    order = dict(zip(job_ids, range(job_count)))
    async with self.lock:
      self.state['queued_jobs'].sort(key=lambda param: order.get(param['job_id'], job_count + 1))
      self.logger.info(f'Reordered {job_count} jobs')
      self.lock.notify_all()
    return job_count

  @rpc_export_function
  async def remove_finished(self, job_ids=None):
    '''Remove finished jobs.'''
    async with self.lock:
      prev_count = len(self.state['finished_jobs'])
      if job_ids is None:
        self.state['finished_jobs'] = []
      else:
        job_ids = set(job_ids)
        self.state['finished_jobs'] = [
            job for job in self.state['finished_jobs'] if job['job_id'] not in job_ids]
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
        self.state['queued_jobs'] = []
      else:
        job_ids = set(job_ids)
        self.state['queued_jobs'] = [
            job for job in self.state['queued_jobs'] if job['job_id'] not in job_ids]
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
