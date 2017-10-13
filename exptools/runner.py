'''Provide the Runner class.'''

__all__ = ['Runner']

import asyncio
import json
import logging
import os
import signal

from exptools.rpc_helper import rpc_export_function

#  def __str__(self):
#    '''Format the job queue state.'''
#    return f'succeeded_jobs={self.succeeded_jobs}, failed_jobs={self.failed_jobs}, ' + \
#           f'active_jobs={self.active_jobs}, pending_jobs={self.pending_jobs}, ' + \
#           f'concurrency={self.concurrency}'
#
#  def format(self):
#    '''Format the job queue state in detail.'''
#    output = f'Succeeded jobs ({len(self.succeeded_jobs)}):\n'
#    for job in self.succeeded_jobs:
#      output += f'  {job}  '
#      output += f'[elapsed: {self.runner.format_elapsed_time(job)}]\n'
#    output += '\n'
#
#    output += f'Failed jobs ({len(self.failed_jobs)}):\n'
#    for job in self.failed_jobs:
#      output += f'  {job}  '
#      output += f'[elapsed: {self.runner.format_elapsed_time(job)}]\n'
#    output += '\n'
#
#    partial_state = self.clone()
#    partial_state.active_jobs.clear()
#    partial_state.pending_jobs.clear()
#
#    output += f'Active jobs ({len(self.active_jobs)}):\n'
#    for job in self.active_jobs:
#      partial_state.active_jobs.append(job)
#      output += f'  {job}  '
#      output += f'[elapsed: {self.runner.format_elapsed_time(job)}] '
#      output += f'[remaining: {self.runner.format_remaining_time(partial_state)}]\n'
#    output += '\n'
#
#    output += f'Pending jobs ({len(self.pending_jobs)}):\n'
#    for job in self.pending_jobs:
#      partial_state.pending_jobs.append(job)
#      output += f'  {job}  '
#      output += f'[duration: {self.runner.format_duration(job)}] '
#      output += f'[remaining: {self.runner.format_remaining_time(partial_state)}]\n'
#
#    output += '\n'
#
#    output += f'Concurrency: {self.concurrency}'
#
#    return output

    #self.estimator = Estimator(hist)


class Runner:
  '''Run jobs with parameters.'''

  def __init__(self, queue, loop):
    self.queue = queue
    self.loop = loop

    self.logger = logging.getLogger('exptools.Runner')

    self.running = False

    asyncio.ensure_future(self._main(), loop=loop)
    asyncio.ensure_future(self.start(), loop=loop)

  @rpc_export_function
  async def is_running(self):
    '''Return True if running.'''
    return self.running

  @rpc_export_function
  async def start(self):
    '''Start the runner.'''
    if self.running:
      self.logger.error('Already started runner')
      return

    self.logger.info('Started Runner')
    self.running = True

    async with self.queue.lock:
      self.queue.lock.notify_all()

  @rpc_export_function
  async def stop(self):
    '''Stop the runner.'''
    if not self.running:
      self.logger.error('Already stopped runner')
      return

    self.running = False
    self.logger.info('Stopped Runner')

  async def _main(self):
    '''Run the queue.'''

    async for queue_state in self.queue.watch_state():
      if not self.running:
        continue

      if not queue_state['queued_jobs']:
        continue

      # XXX: No prerequisite support; avoid concurrent execution
      if queue_state['started_jobs']:
        continue

      # TODO: Possibly launch other jobs if prerequisites meet
      job = queue_state['queued_jobs'][0]

      asyncio.ensure_future(self._run(job), loop=self.loop)

  async def _run(self, job):
    '''Run a job.'''

    job_id = job['id']
    param = job['param']

    try:
      cmd = param['_cmd']
      cmd_on_failure = param.get('_cmd_on_failure', None)

      if not await self.queue.set_started(job_id, None):
        self.logger.info(f'Ignoring missing job {job_id}')
        return

      self.logger.info(f'Launching job {job_id} with command {cmd}')

      proc = await asyncio.create_subprocess_exec(
          cmd, stdin=asyncio.subprocess.PIPE, loop=self.loop)
      await self.queue.set_started(job_id, proc.pid)

      await proc.communicate(input=json.dumps(job).encode('utf-8'))

      if proc.returncode == 0:
        await self.queue.set_finished(job_id, True)
      else:
        if cmd_on_failure is not None:
          proc = await asyncio.create_subprocess_exec(
              cmd_on_failure, stdin=asyncio.subprocess.PIPE, loop=self.loop)
          await proc.communicate(input=json.dumps(job))
        await self.queue.set_finished(job_id, False)
    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id}')
      await self.queue.set_finished(job_id, False)

  @rpc_export_function
  async def kill(self, job_ids, force=False):
    '''Kill started jobs.'''
    if isinstance(job_ids, int):
      job_ids = [job_ids]

    job_ids = set(job_ids)

    queue_state = await self.queue.get_state()
    for job in queue_state['started_jobs']:
      if job['id'] in job_ids and job['pid'] is not None:
        if not force:
          self.logger.info(f'Killing job {job["id"]}')
          os.kill(job['pid'], signal.SIGINT)
        else:
          self.logger.info(f'Killing job {job["id"]} forcefully')
          os.kill(job['pid'], signal.SIGTERM)

  @rpc_export_function
  async def killall(self, force=False):
    '''Kill all started jobs.'''
    queue_state = await self.queue.get_state()
    await self.kill([job['id'] for job in queue_state['started_jobs']], force)

  #def format_elapsed_time(self, job):
  #  '''Format the elapsed time of a job.'''
  #  hist_entry = self.hist.get(job.param)
  #  started = hist_entry['started']
  #  finished = hist_entry['finished']
  #  if not started:
  #    return format_sec(0.)
  #  if not finished:
  #    return format_sec(diff_sec(utcnow(), started))
  #  return format_sec(diff_sec(finished, started))

  #def format_duration(self, job):
  #  '''Format the duration of a job.'''
  #  hist_entry = self.hist.get(job.param)
  #  duration = hist_entry['duration']
  #  if not duration:
  #    return format_sec(0.)
  #  return format_sec(duration)

  #def format_remaining_time(self, state=None):
  #  '''Format the elapsed time of jobs in the state.'''
  #  if state is None:
  #    state = self.state()
  #  return format_sec(self.estimator.estimate_remaining_time(state))

  #def format_estimated_time(self, work_list=None, params=None, concurrency=None):
  #  '''Format the estimated time to finish jobs with new parameters.'''
  #  state = self.state()

  #  if work_list is None:
  #    work_list = []
  #  if params is None:
  #    params = []
  #  if isinstance(params, Param):
  #    params = [params]
  #  if isinstance(work_list, Work):
  #    work_list = [work_list] * len(params)

  #  assert len(work_list) == len(params)

  #  with self.lock:
  #    if params is not None:
  #      new_jobs = [Job(self.next_job_id + i, work_list[i], params[i]) for i in range(len(params))]
  #      state.pending_jobs = \
  #          self._sort(self._dedup(state.pending_jobs + new_jobs))
  #    if concurrency is not None:
  #      state.concurrency = concurrency

  #  return self.estimator.format_estimated_time(state)

  #def _format_estimated_time(self):
  #  '''Format the estimated time to finish jobs.'''
  #  assert self.lock.locked() # pylint: disable=no-member
  #  return self.estimator.format_estimated_time(self._state)

  #def _succeeded(self, job):
  #  '''Report a succeeded job.'''
  #  assert self.lock.locked() # pylint: disable=no-member
  #  self.hist.finished(job.param, True)
  #  self._state.succeeded_jobs.append(job)
  #  #self.logger.info(termcolor.colored(
  #  #    f'Succeeded: {job} ' + \
  #  #    f'[elapsed: {self.format_elapsed_time(job)}]', 'green'))
  #  #self.logger.info(self._format_estimated_time())

  #def _failed_resource_error(self, job, exc):
  #  '''Report a failed job due to a resource error.'''
  #  assert self.lock.locked() # pylint: disable=no-member
  #  self.hist.finished(job.param, False)
  #  self._state.failed_jobs.append(job)
  #  #self.logger.error(termcolor.colored(
  #  #    f'Failed: {job} (resource error)' + \
  #  #    f'[elapsed: {self.format_elapsed_time(job)}]' + \
  #  #    '\n' + exc, 'red'))
  #  #self.logger.info(self._format_estimated_time())

  #def _failed_exception(self, job, exc):
  #  '''Report a failed job with an exception.'''
  #  assert self.lock.locked() # pylint: disable=no-member
  #  self.hist.finished(job.param, False)
  #  self._state.failed_jobs.append(job)
  #  exc = '  ' + exc.rstrip().replace('\n', '\n  ')
  #  #self.logger.error(termcolor.colored(
  #  #    f'Failed:   {job} (exception) ' + \
  #  #    f'[elapsed: {self.format_elapsed_time(job)}]' + \
  #  #    '\n' + exc, 'red'))
  #  #self.logger.info(self._format_estimated_time())

  #def _check_empty_queue(self):
  #  assert self.lock.locked() # pylint: disable=no-member
  #  if not (self._state.active_jobs or self._state.pending_jobs):
  #    self.logger.warning('Job queue empty')
