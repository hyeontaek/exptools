'''Provide the Runner class.'''

__all__ = ['Runner']

import asyncio
import base64
import concurrent
import json
import logging
import os
import signal

import aiofiles
import aionotify

from exptools.file import mkdirs, rmdirs, get_job_dir, get_param_path
from exptools.rpc_helper import rpc_export_function, rpc_export_generator

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

  async def run_forever(self):
    '''Run scheduled jobs.'''

    tasks = []
    try:
      async for job in self.scheduler.schedule():
        task = asyncio.ensure_future(self._run(job), loop=self.loop)
        tasks.append(task)

        new_tasks = []
        for task in tasks:
          if task.done():
            # task should not normally raise an exception
            await task
          else:
            new_tasks.append(task)
        tasks = new_tasks
    finally:
      for task in tasks:
        if not task.done():
          task.cancel()
        try:
          await task
        except concurrent.futures.CancelledError:
          # Ignore CancelledError because we caused it
          pass

  @staticmethod
  def _create_job_files(job, job_dir, expanded_command):
    '''Create job filles.'''

    # Dump job
    with open(os.path.join(job_dir, 'job.json'), 'wt') as file:
      file.write(json.dumps(job) + '\n')

    # Dump simple properties
    for key in ['job_id', 'param_id', 'name', 'cwd']:
      with open(os.path.join(job_dir, key), 'wt') as file:
        assert isinstance(job[key], str)
        file.write(job[key] + '\n')

    for key in ['time_limit']:
      with open(os.path.join(job_dir, key), 'wt') as file:
        assert isinstance(job[key], int) or isinstance(job[key], float)
        file.write(str(job[key]) + '\n')

    # Dump structured properties
    for key in ['command', 'param', 'resources']:
      with open(os.path.join(job_dir, key + '.json'), 'wt') as file:
        file.write(json.dumps(job[key]) + '\n')

    # Create a stub for status
    with open(os.path.join(job_dir, 'status.json'), 'wt') as file:
      status = {'progress': 0.}
      file.write(json.dumps(status) + '\n')

    # Dump expanded properties
    with open(os.path.join(job_dir, 'expanded_command.json'), 'wt') as file:
      file.write(json.dumps(expanded_command) + '\n')

  @staticmethod
  def _construct_env(job, job_dir):
    '''Construct environment variables.'''
    env = dict(os.environ)

    env['EXPTOOLS_JOB_JSON_PATH'] = os.path.join(job_dir, 'job.json')

    env['EXPTOOLS_JOB_DIR'] = job_dir
    env['EXPTOOLS_JOB_ID'] = job['job_id']
    env['EXPTOOLS_PARAM_ID'] = job['param_id']
    env['EXPTOOLS_NAME'] = job['name']
    env['EXPTOOLS_CWD'] = job['cwd']
    env['EXPTOOLS_TIME_LIMIT'] = str(job['time_limit'])

    env['EXPTOOLS_COMMAND_JSON_PATH'] = os.path.join(job_dir, 'command.json')
    env['EXPTOOLS_PARAM_JSON_PATH'] = os.path.join(job_dir, 'param.json')
    env['EXPTOOLS_RESOURCES_JSON_PATH'] = os.path.join(job_dir, 'resources.json')
    env['EXPTOOLS_EXPANDED_COMMAND_JSON_PATH'] = os.path.join(job_dir, 'expanded_command.json')

    env['EXPTOOLS_STATUS_JSON_PATH'] = os.path.join(job_dir, 'status.json')
    return env

  async def _run(self, job):
    '''Run a job.'''

    job_id = job['job_id']
    param_id = job['param_id']
    param = job['param']

    name = job['name']
    expanded_command = [arg.format(**param) for arg in job['command']]
    cwd = job['cwd']
    time_limit = job['time_limit']

    try:
      if not await self.queue.set_started(job_id):
        self.logger.info(f'Ignoring missing job {job_id}')
        return

      self.logger.info(f'Launching job {job_id} for {param_id}: {name}')

      job_dir = get_job_dir(self.base_dir, job)
      os.mkdir(job_dir)

      self._create_job_files(job, job_dir, expanded_command)
      env = self._construct_env(job, job_dir)

      param_path = get_param_path(self.base_dir, param_id)
      if os.path.exists(param_path + '_tmp'):
        os.unlink(param_path + '_tmp')
      os.symlink(job_id, param_path + '_tmp', target_is_directory=True)

      try:
        if os.path.exists(os.path.join(self.base_dir, 'last_tmp')):
          os.unlink(os.path.join(self.base_dir, 'last_tmp'))
        os.symlink(job_id, os.path.join(self.base_dir, 'last_tmp'), target_is_directory=True)

        os.rename(os.path.join(self.base_dir, 'last_tmp'), os.path.join(self.base_dir, 'last'))
      except IOError:
        pass

      with open(os.path.join(job_dir, 'stdout'), 'wb', buffering=0) as stdout, \
           open(os.path.join(job_dir, 'stderr'), 'wb', buffering=0) as stderr:
        proc = await asyncio.create_subprocess_exec(
            *expanded_command,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            env=env,
            loop=self.loop)

        await self.queue.set_pid(job_id, proc.pid)

        # Watch status changes
        status_task = asyncio.ensure_future(
            self._watch_status(job_id, job_dir), loop=self.loop)

        try:
          if time_limit <= 0:
            await proc.communicate()
          else:
            await asyncio.wait_for(proc.communicate(), time_limit, loop=self.loop)
        except Exception: # pylint: disable=broad-except
          # Do not use proc.kill() or terminate() to allow the job to exit gracefully
          try:
            proc.send_signal(signal.SIGINT)
          except Exception: # pylint: disable=broad-except
            self.logger.exception('Exception while killing process')
        finally:
          try:
            await proc.wait()
          except Exception: # pylint: disable=broad-except
            self.logger.exception('Exception while waiting for process')

          status_task.cancel()
          try:
            await status_task
          except concurrent.futures.CancelledError:
            # Ignore CancelledError because we caused it
            pass

        # Read before making the job finished
        await self._read_status(job_id, job_dir)

      if proc.returncode == 0:
        os.rename(param_path + '_tmp', param_path)
        await self.queue.set_finished(job_id, True)
      else:
        os.unlink(param_path + '_tmp')
        await self.queue.set_finished(job_id, False)

    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id} ({param_id}): {name}')

      # Read before making the job finished
      await self._read_status(job_id, job_dir)

      await self.queue.set_finished(job_id, False)
    finally:
      await self.scheduler.retire(job)

  async def _watch_status(self, job_id, job_dir):
    '''Watch the status file changes.'''
    status_path = os.path.join(job_dir, 'status.json')

    watcher = aionotify.Watcher()
    watcher.watch(status_path, aionotify.Flags.CLOSE_WRITE)
    await watcher.setup(self.loop)
    try:
      while True:
        try:
          await self._read_status(job_id, job_dir)
          await watcher.get_event()
          self.logger.debug(f'Detected status change for job {job_id}')
        except concurrent.futures.CancelledError:
          # Break loop (likely normal exit through task cancellation)
          break
        except Exception: # pylint: disable=broad-except
          self.logger.exception(f'Exception while watching status of job {job_id}')
    finally:
      watcher.unwatch(status_path)
      watcher.close()

  async def _read_status(self, job_id, job_dir):
    '''Read the status file and send it to the queue.'''
    status_path = os.path.join(job_dir, 'status.json')

    try:
      async with aiofiles.open(status_path) as file:
        status_json = await file.read()

      status = json.loads(status_json)

      # Validate common value types
      assert isinstance(status, dict)

      progress = status.get('progress', 0.)
      assert progress >= 0. and progress <= 1.

      assert isinstance(status.get('message', ''), str)

      await self.queue.set_status(job_id, status)
    except concurrent.futures.CancelledError:
      # Ignore (likely normal exit through task cancellation)
      pass
    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while reading status of job {job_id}')

  @rpc_export_function
  async def kill(self, job_ids=None, signal_type=None):
    '''Kill started jobs.'''
    queue_state = await self.queue.get_state()

    if job_ids is None:
      job_ids = [job['job_id'] for job in queue_state['started_jobs']]

    count = 0
    job_ids = set(job_ids)
    for job in queue_state['started_jobs']:
      if job['job_id'] in job_ids and job['pid'] is not None:
        job_id = job['job_id']
        if signal_type == 'int':
          self.logger.info(f'Interrupting job {job_id}')
          os.kill(job['pid'], signal.SIGINT)
        elif not signal_type or signal_type == 'term':
          self.logger.info(f'Terminating job {job_id}')
          os.kill(job['pid'], signal.SIGTERM)
        elif signal_type == 'kill':
          self.logger.info(f'Killing job {job_id}')
          os.kill(job['pid'], signal.SIGKILL)
        else:
          raise RuntimeError(f'Unknown signal: {signal_type}')
        count += 1
    return count

  @rpc_export_generator
  async def cat(self, job_id, read_stdout, extra_args):
    '''Run cat on the job output.'''
    async for data in self._read_output(job_id, read_stdout, 'cat', extra_args):
      yield data

  @rpc_export_generator
  async def head(self, job_id, read_stdout, extra_args):
    '''Run head on the job output.'''
    async for data in self._read_output(job_id, read_stdout, 'head', extra_args):
      yield data

  @rpc_export_generator
  async def tail(self, job_id, read_stdout, extra_args):
    '''Run tail on the job output.'''
    async for data in self._read_output(job_id, read_stdout, 'tail', extra_args):
      yield data

  async def _read_output(self, job_id, read_stdout, external_command, extra_args):
    '''Yield the output of an external command on the job output.'''
    assert job_id.startswith('j-') and job_id.find('/') == -1

    path = os.path.join(self.base_dir, job_id)
    if read_stdout:
      path = os.path.join(path, 'stdout')
    else:
      path = os.path.join(path, 'stderr')
    command = [external_command] + extra_args + [path]

    proc = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        loop=self.loop)

    try:
      while True:
        data = await proc.stdout.read(4096)
        if not data:
          break
        yield base64.b64encode(data).decode('ascii')
    except Exception: # pylint: disable=broad-except
      proc.terminate()
      raise
    finally:
      try:
        await proc.wait()
      except Exception: # pylint: disable=broad-except
        self.logger.exception('Exception while waiting for process')

  @rpc_export_function
  async def migrate(self, changes):
    '''Migrate symlinks for parameter ID changes.'''
    count = 0
    for old_param_id, new_param_id in changes:
      old_param_path = get_param_path(self.base_dir, old_param_id)
      new_param_path = get_param_path(self.base_dir, new_param_id)

      if not os.path.exists(old_param_path):
        self.logger.info(f'Ignoring missing symlink for old parameter {old_param_id}')
        continue

      if os.path.exists(new_param_path):
        self.logger.info(f'Ignoring existing symlink for new parameter {new_param_id}')
        continue

      job_id = os.readlink(old_param_path).strip('/')
      os.symlink(job_id, new_param_path, target_is_directory=True)
      self.logger.info(f'Migrated symlink of old parameter {old_param_id} ' + \
                       f'to new parameter {new_param_id}')
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
