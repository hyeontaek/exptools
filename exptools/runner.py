'''Provide the Runner class.'''

__all__ = ['Runner']

import asyncio
import json
import logging
import os
import signal

from exptools.file import mkdirs, rmdirs, get_job_dir, get_exec_path
from exptools.rpc_helper import rpc_export_function

class Runner:
  '''Run jobs with parameters.'''

  def __init__(self, base_dir, queue, loop):
    self.base_dir = base_dir
    self.queue = queue
    self.loop = loop

    self.logger = logging.getLogger('exptools.Runner')

    self.running = False

    if not os.path.exists(self.base_dir):
      mkdirs(self.base_dir, ignore_errors=False)

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

  @staticmethod
  def _create_job_files(job, job_dir):
    '''Create job filles.'''

    with open(os.path.join(job_dir, 'job_id'), 'wt') as file:
      file.write(job['job_id'])
    with open(os.path.join(job_dir, 'param_id'), 'wt') as file:
      file.write(job['param_id'])
    with open(os.path.join(job_dir, 'exec_id'), 'wt') as file:
      file.write(job['exec_id'])
    with open(os.path.join(job_dir, 'job.json'), 'wt') as file:
      file.write(json.dumps(job))
    with open(os.path.join(job_dir, 'param.json'), 'wt') as file:
      file.write(json.dumps(job['param']))

  @staticmethod
  def _construct_env(job, job_dir):
    '''Construct environment variables.'''
    env = dict(os.environ)
    env['EXPTOOLS_JOB_DIR'] = job_dir
    env['EXPTOOLS_JOB_ID'] = job['job_id']
    env['EXPTOOLS_PARAM_ID'] = job['param_id']
    env['EXPTOOLS_EXEC_ID'] = job['exec_id']
    env['EXPTOOLS_JOB_JSON_PATH'] = os.path.join(job_dir, 'job.json')
    env['EXPTOOLS_PARAM_JSON_PATH'] = os.path.join(job_dir, 'param.json')
    return env

  async def _run(self, job):
    '''Run a job.'''

    job_id = job['job_id']
    exec_id = job['exec_id']

    name = job['name']
    param = job['param']

    try:
      cmd = param['cmd']

      if not await self.queue.set_started(job_id):
        self.logger.info(f'Ignoring missing job {job_id}')
        return

      self.logger.info(f'Launching job {job_id} for {exec_id}: {name}')

      job_dir = get_job_dir(self.base_dir, job)
      os.mkdir(job_dir)

      self._create_job_files(job, job_dir)
      env = self._construct_env(job, job_dir)

      exec_path = get_exec_path(self.base_dir, exec_id)
      if os.path.exists(exec_path + '_tmp'):
        os.unlink(exec_path + '_tmp')
      os.symlink(job_id, exec_path + '_tmp', target_is_directory=True)

      with open(os.path.join(job_dir, 'out'), 'wb', buffering=0) as stdout, \
           open(os.path.join(job_dir, 'err'), 'wb', buffering=0) as stderr:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=env,
            loop=self.loop)

        await self.queue.set_pid(job_id, proc.pid)

        await proc.communicate()

      if proc.returncode == 0:
        os.rename(exec_path + '_tmp', exec_path)
        await self.queue.set_finished(job_id, True)
      else:
        os.unlink(exec_path + '_tmp')
        await self.queue.set_finished(job_id, False)

    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id} ({exec_id}): {name}')
      await self.queue.set_finished(job_id, False)

  @rpc_export_function
  async def kill(self, job_ids=None, force=False):
    '''Kill started jobs.'''
    queue_state = await self.queue.get_state()

    if job_ids is None:
      job_ids = [job['job_id'] for job in queue_state['started_jobs']]

    count = 0
    job_ids = set(job_ids)
    for job in queue_state['started_jobs']:
      if job['job_id'] in job_ids and job['pid'] is not None:
        job_id = job['job_id']
        if not force:
          self.logger.info(f'Killing job {job_id}')
          os.kill(job['pid'], signal.SIGINT)
        else:
          self.logger.info(f'Terminating job {job_id}')
          os.kill(job['pid'], signal.SIGTERM)
        count += 1
    return count

  @rpc_export_function
  async def prune_absent(self, exec_ids):
    '''Prune directories that are not represented by given parameters.'''
    trash_dir = os.path.join(self.base_dir, 'trash')
    if not os.path.exists(trash_dir):
      os.mkdir(trash_dir)

    exec_ids = set(exec_ids)

    job_ids = set()
    for filename in os.listdir(self.base_dir):
      if filename.startswith('e-'):
        path = os.path.join(self.base_dir, filename)
        # accept both e-XXX and e-XXX_tmp
        if filename.partition('_')[0] in exec_ids:
          job_ids.add(os.readlink(path).strip('/'))
        else:
          new_path = os.path.join(trash_dir, filename)
          if os.path.exists(new_path):
            os.unlink(new_path)
          os.rename(path, new_path)
          self.logger.info(f'Moved {filename} to trash')

    for filename in os.listdir(self.base_dir):
      if filename.startswith('j-'):
        path = os.path.join(self.base_dir, filename)
        if filename not in job_ids:
          new_path = os.path.join(trash_dir, filename)
          if os.path.exists(new_path):
            rmdirs(new_path)
          os.rename(path, new_path)
          self.logger.info(f'Moved {filename} to trash')
