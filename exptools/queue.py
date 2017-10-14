'''Provide the Queue class.'''

__all__ = ['Queue']

import asyncio
import logging

from exptools.param import get_exec_id, get_param_id, get_name
from exptools.rpc_helper import rpc_export_function, rpc_export_generator
from exptools.time import diff_sec, utcnow, format_utc, parse_utc

# pylint: disable=too-many-instance-attributes
class Queue:
  '''Manage a job queue.'''

  def __init__(self, history, loop):
    self.history = history
    self.loop = loop

    self.lock = asyncio.Condition()
    self.finished_jobs = []
    self.started_jobs = []
    self.queued_jobs = []
    self.concurrency = 1.

    self.logger = logging.getLogger('exptools.Queue')

    asyncio.ensure_future(self.update_concurrency(), loop=loop)

  def _get_state(self):
    '''Get the job queue state.'''
    state = {
        'finished_jobs': list(self.finished_jobs),
        'started_jobs': list(self.started_jobs),
        'queued_jobs': list(self.queued_jobs),
        'concurrency': self.concurrency,
        }
    return state

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

  @rpc_export_function
  async def omit(self, params, *, queued=True, started=True, finished=True):
    '''Omit parameters that are already in the queue.'''
    exec_ids = set()
    async with self.lock:
      if queued:
        exec_ids.update(map(lambda job: get_exec_id(job['param']), self.queued_jobs))
      if started:
        exec_ids.update(map(lambda job: get_exec_id(job['param']), self.started_jobs))
      if finished:
        exec_ids.update(map(lambda job: get_exec_id(job['param']), self.finished_jobs))
    return list(filter(lambda param: param.exec_id not in exec_ids, params))

  @rpc_export_function
  async def add(self, params):
    '''Add parameters as queued jobs.'''
    now = format_utc(utcnow())
    job_ids = []
    async with self.lock:
      for param in params:
        exec_id = get_exec_id(param)
        await self.history.set_queued(exec_id, now)

        job_id = await self.history.get_next_job_id()
        job_ids.append(job_id)

        self.queued_jobs.append({
            'job_id': job_id,
            'param_id': get_param_id(param),
            'exec_id': exec_id,
            'name': get_name(param),
            'param': param,
            'queued': now,
            'started': None,
            'finished': None,
            'duration': None,
            'pid': None,
            'succeeded': None,
            })

      self.logger.info(f'Added {len(params)} jobs')
      self.lock.notify_all()
    return job_ids

  @rpc_export_function
  async def re_add(self, job_ids):
    '''Re-add finished jobs.'''
    params = []
    async with self.lock:
      # Iterate over job_ids so that we preserve the order
      for job_id in job_ids:
        for job in self.finished_jobs:
          if job['job_id'] == job_id:
            params.append(job['param'])
            break
        else:
          for job in self.started_jobs:
            if job['job_id'] == job_id:
              params.append(job['param'])

    return await self.add(params)

  @rpc_export_function
  async def set_started(self, job_id, pid):
    '''Mark a queued job as started.'''
    now = format_utc(utcnow())
    async with self.lock:
      for i, job in enumerate(self.queued_jobs):
        if job['job_id'] == job_id:
          exec_id = job['exec_id']
          await self.history.set_started(exec_id, now)

          job['started'] = now
          job['pid'] = pid

          self.started_jobs.append(job)
          del self.queued_jobs[i]

          self.logger.info(f'Started job {job_id}')
          self.lock.notify_all()
          return True
    return False

  @rpc_export_function
  async def set_finished(self, job_id, succeeded):
    '''Mark an started job as finished.'''
    now = format_utc(utcnow())
    async with self.lock:
      for i, job in enumerate(self.started_jobs):
        if job['job_id'] == job_id:
          exec_id = job['exec_id']
          await self.history.set_finished(exec_id, succeeded, now)

          job['finished'] = now
          job['duration'] = \
              diff_sec(parse_utc(now), parse_utc(job['started']))
          job['pid'] = None
          job['succeeded'] = succeeded

          self.finished_jobs.append(job)
          del self.started_jobs[i]

          if succeeded:
            self.logger.info(f'Finished job {job_id} [suceeded]')
          else:
            self.logger.warning(f'Finished job {job_id} [FAILED]')
          self._check_empty()
          self.lock.notify_all()
          return True
    return False

  @rpc_export_function
  async def reorder(self, job_ids):
    '''Reorder queued jobs.'''
    job_count = len(job_ids)
    order = dict(zip(job_ids, range(job_count)))
    async with self.lock:
      self.queued_jobs.sort(key=lambda job_id, param: order.get(job_id, job_count + 1))
      self.logger.info(f'Reordered {job_count} jobs')
      self.lock.notify_all()

  @rpc_export_function
  async def clear(self):
    '''Clear all but started jobs.'''
    async with self.lock:
      self.finished_jobs = []
      self.queued_jobs = []
      self.logger.info(f'Cleared jobs')
      self._check_empty()
      self.lock.notify_all()

  @rpc_export_function
  async def remove_finished(self, job_ids=None):
    '''Remove finished jobs.'''
    async with self.lock:
      prev_count = len(self.finished_jobs)
      if job_ids is None:
        self.finished_jobs = []
      else:
        job_ids = set(job_ids)
        self.finished_jobs = [job for job in self.finished_jobs if job['job_id'] not in job_ids]
      new_count = len(self.finished_jobs)
      self.logger.info(f'Removed {prev_count - new_count} finished jobs')
      self._check_empty()
      self.lock.notify_all()

  @rpc_export_function
  async def remove_queued(self, job_ids=None):
    '''Remove queued jobs.'''
    async with self.lock:
      prev_count = len(self.queued_jobs)
      if job_ids is None:
        self.queued_jobs = []
      else:
        job_ids = set(job_ids)
        self.queued_jobs = [job for job in self.queued_jobs if job['job_id'] not in job_ids]
      new_count = len(self.queued_jobs)
      self.logger.info(f'Removed {prev_count - new_count} queued jobs')
      self._check_empty()
      self.lock.notify_all()

  def _check_empty(self):
    assert self.lock.locked()
    if not self.started_jobs and not self.queued_jobs:
      self.logger.info('Queue empty')

  async def update_concurrency(self):
    '''Update the current concurrency.'''
    alpha = 0.9

    while True:
      async with self.lock:
        self.concurrency = max(
            1.,
            alpha * self.concurrency + (1. - alpha) * len(self.started_jobs))
      await asyncio.sleep(1)
