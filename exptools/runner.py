'''Provide the Runner class.'''

__all__ = ['Runner']

import asyncio
import json
import logging
import os
import signal

from exptools.file import mkdirs, rmdirs, get_job_dir, get_param_path
from exptools.rpc_helper import rpc_export_function

class Runner:
  '''Run jobs with parameters.'''

  def __init__(self, base_dir, queue, scheduler, loop):
    self.base_dir = base_dir
    self.queue = queue
    self.scheduler = scheduler
    self.loop = loop

    self.logger = logging.getLogger('exptools.Runner')

    if not os.path.exists(self.base_dir):
      mkdirs(self.base_dir, ignore_errors=False)

    asyncio.ensure_future(self._wait_for_schedule(), loop=loop)

  async def _wait_for_schedule(self):
    '''Request to run a scheduled job.'''

    async for job in self.scheduler.schedule():
      asyncio.ensure_future(self._run(job), loop=self.loop)

  @staticmethod
  def _create_job_files(job, job_dir):
    '''Create job filles.'''

    with open(os.path.join(job_dir, 'job.json'), 'wt') as file:
      file.write(json.dumps(job) + '\n')

    for key in ['job_id', 'param_id', 'name', 'cwd']:
      with open(os.path.join(job_dir, key), 'wt') as file:
        assert isinstance(job[key], str)
        file.write(job[key] + '\n')

    for key in ['command', 'param']:
      with open(os.path.join(job_dir, key + '.json'), 'wt') as file:
        file.write(json.dumps(job[key]) + '\n')

  @staticmethod
  def _construct_env(job, job_dir):
    '''Construct environment variables.'''
    env = dict(os.environ)
    env['EXPTOOLS_JOB_DIR'] = job_dir
    env['EXPTOOLS_JOB_ID'] = job['job_id']
    env['EXPTOOLS_param_id'] = job['param_id']
    env['EXPTOOLS_JOB_JSON_PATH'] = os.path.join(job_dir, 'job.json')
    env['EXPTOOLS_PARAM_JSON_PATH'] = os.path.join(job_dir, 'param.json')
    return env

  async def _run(self, job):
    '''Run a job.'''

    job_id = job['job_id']
    param_id = job['param_id']

    name = job['name']
    command = job['command']
    cwd = job['cwd']

    try:
      if not await self.queue.set_started(job_id):
        self.logger.info(f'Ignoring missing job {job_id}')
        return

      self.logger.info(f'Launching job {job_id} for {param_id}: {name}')

      job_dir = get_job_dir(self.base_dir, job)
      os.mkdir(job_dir)

      self._create_job_files(job, job_dir)
      env = self._construct_env(job, job_dir)

      param_path = get_param_path(self.base_dir, param_id)
      if os.path.exists(param_path + '_tmp'):
        os.unlink(param_path + '_tmp')
      os.symlink(job_id, param_path + '_tmp', target_is_directory=True)

      with open(os.path.join(job_dir, 'stdout'), 'wb', buffering=0) as stdout, \
           open(os.path.join(job_dir, 'stderr'), 'wb', buffering=0) as stderr:
        proc = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=env,
            loop=self.loop)

        await self.queue.set_pid(job_id, proc.pid)

        await proc.communicate()

      if proc.returncode == 0:
        os.rename(param_path + '_tmp', param_path)
        await self.queue.set_finished(job_id, True)
      else:
        os.unlink(param_path + '_tmp')
        await self.queue.set_finished(job_id, False)

    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id} ({param_id}): {name}')
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
  async def prune(self, param_ids, *, prune_matching=False, prune_mismatching=False):
    '''Prune output data.'''
    trash_dir = os.path.join(self.base_dir, 'trash')
    if not os.path.exists(trash_dir):
      os.mkdir(trash_dir)

    queue_state = await self.queue.get_state()
    started_job_param_ids = [job['param_id'] for job in queue_state['started_jobs']]

    # The below code must not use any coroutine so that
    # Runner does not create any new symlink or directory concurrently
    # by launching/finishing jobs
    filenames = os.listdir(self.base_dir)

    symlink_count = 0
    dir_count = 0

    param_ids = set(param_ids)
    valid_job_ids = set()
    for filename in filenames:
      if not filename.startswith('p-'):
        continue
      path = os.path.join(self.base_dir, filename)

      prune = False
      if filename.endswith('_tmp'):
        # prune p-*_tmp symlinks for any non-started jobs
        if filename.partition('_')[0] not in started_job_param_ids:
          prune = True
      elif (prune_matching and filename in param_ids) or \
         (prune_mismatching and filename not in param_ids):
        prune = True

      if prune:
        new_path = os.path.join(trash_dir, filename)
        if os.path.exists(new_path):
          os.unlink(new_path)
        os.rename(path, new_path)
        self.logger.info(f'Moved {filename} to trash')
        symlink_count += 1
      else:
        valid_job_ids.add(os.readlink(path).strip('/'))

    # Prune j-* directories if no symlinks point to it
    for filename in filenames:
      if not filename.startswith('j-'):
        continue
      path = os.path.join(self.base_dir, filename)

      if filename not in valid_job_ids:
        new_path = os.path.join(trash_dir, filename)
        if os.path.exists(new_path):
          rmdirs(new_path)
        os.rename(path, new_path)
        self.logger.info(f'Moved {filename} to trash')
        dir_count += 1

    return symlink_count, dir_count
