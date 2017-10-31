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

from exptools.file import mkdirs, rmdirs
from exptools.param import (
    get_param_id, get_hash_id,
    get_name, get_command, get_cwd, get_retry, get_retry_delay, get_time_limit)
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
  def _create_job_files(job, job_dir):
    '''Create job filles.'''

    # Dump job
    with open(os.path.join(job_dir, 'job.json'), 'wt') as file:
      file.write(json.dumps(job) + '\n')

    # Dump structured properties
    for key in ['param', 'resources']:
      with open(os.path.join(job_dir, key + '.json'), 'wt') as file:
        file.write(json.dumps(job[key]) + '\n')

    # Create a stub for status
    with open(os.path.join(job_dir, 'status.json'), 'wt') as file:
      status = {'progress': 0.}
      file.write(json.dumps(status) + '\n')

  @staticmethod
  def _construct_env(job, job_dir):
    '''Construct environment variables.'''
    param = job['param']

    env = dict(os.environ)

    env['EXPTOOLS_JOB_DIR'] = job_dir
    env['EXPTOOLS_JOB_ID'] = job['job_id']
    env['EXPTOOLS_PARAM_ID'] = get_param_id(param)
    env['EXPTOOLS_HASH_ID'] = get_hash_id(param)
    env['EXPTOOLS_NAME'] = get_name(param)
    env['EXPTOOLS_CWD'] = get_cwd(param) or os.getcwd()
    env['EXPTOOLS_RETRY'] = str(get_retry(param))
    env['EXPTOOLS_RETRY_DELAY'] = str(get_retry_delay(param))
    env['EXPTOOLS_TIME_LIMIT'] = str(get_time_limit(param))

    env['EXPTOOLS_JOB_JSON_PATH'] = os.path.join(job_dir, 'job.json')
    env['EXPTOOLS_PARAM_JSON_PATH'] = os.path.join(job_dir, 'param.json')
    env['EXPTOOLS_RESOURCES_JSON_PATH'] = os.path.join(job_dir, 'resources.json')

    env['EXPTOOLS_STATUS_JSON_PATH'] = os.path.join(job_dir, 'status.json')
    return env

  def _make_tmp_symlinks(self, param_id, hash_id, job_id):
    param_path = os.path.join(self.base_dir, param_id)
    hash_path = os.path.join(self.base_dir, hash_id)

    if os.path.lexists(param_path + '_tmp'):
      os.unlink(param_path + '_tmp')
    os.symlink(job_id, param_path + '_tmp', target_is_directory=True)

    if os.path.lexists(hash_path + '_tmp'):
      os.unlink(hash_path + '_tmp')
    os.symlink(job_id, hash_path + '_tmp', target_is_directory=True)

    last_path = os.path.join(self.base_dir, 'last')
    if os.path.lexists(last_path):
      os.unlink(last_path)
    os.symlink(job_id, last_path, target_is_directory=True)

  def _make_symlinks(self, param_id, hash_id, job_id):
    param_path = os.path.join(self.base_dir, param_id)
    hash_path = os.path.join(self.base_dir, hash_id)

    if os.path.lexists(param_path):
      os.unlink(param_path)
    os.symlink(job_id, param_path, target_is_directory=True)

    if os.path.lexists(hash_path):
      os.unlink(hash_path)
    os.symlink(job_id, hash_path, target_is_directory=True)

  def _remove_tmp_symlinks(self, param_id, hash_id):
    param_path = os.path.join(self.base_dir, param_id)
    hash_path = os.path.join(self.base_dir, hash_id)

    if os.path.lexists(param_path + '_tmp'):
      os.unlink(param_path + '_tmp')
    if os.path.lexists(hash_path + '_tmp'):
      os.unlink(hash_path + '_tmp')

  async def _run(self, job):
    '''Run a job.'''

    job_id = job['job_id']

    if not await self.queue.set_started(job_id):
      self.logger.info(f'Ignoring missing job {job_id}')
      return True

    succeeded = False
    try:
      param = job['param']

      retry = get_retry(param)
      retry_delay = get_retry_delay(param)

      for i in range(retry + 1):
        succeeded = await self._try(job, job_id, param)

        if succeeded:
          break

        if i < retry:
          self.logger.info(f'Retrying {job_id}: {i + 1} / {retry}')
          await asyncio.sleep(retry_delay, loop=self.loop)

    finally:
      await self.scheduler.retire(job)

      await self.queue.set_finished(job_id, succeeded)

  async def _try(self, job, job_id, param):
    '''Run a job.'''

    param_id = get_param_id(param)
    hash_id = get_hash_id(param)

    name = get_name(param)
    expanded_command = [arg.format(**param) for arg in get_command(param)]
    cwd = get_cwd(param) or os.getcwd()
    time_limit = get_time_limit(param)

    succeeded = False

    try:
      self.logger.info(f'Launching job {job_id}: {name}')

      # Make the job directory
      job_dir = os.path.join(self.base_dir, job['job_id'])
      if os.path.exists(job_dir):
        rmdirs(job_dir)
      os.mkdir(job_dir)

      self._create_job_files(job, job_dir)
      env = self._construct_env(job, job_dir)

      with open(os.path.join(job_dir, 'stdout'), 'wb', buffering=0) as stdout, \
           open(os.path.join(job_dir, 'stderr'), 'wb', buffering=0) as stderr:

        self._make_tmp_symlinks(param_id, hash_id, job_id)

        # Launch process
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

        except asyncio.TimeoutError:
          self.logger.error(f'Timeout while waiting for job {job_id}')

        finally:
          status_task.cancel()
          try:
            await status_task
          except concurrent.futures.CancelledError:
            # Ignore CancelledError because we caused it
            pass

          if proc.returncode is None:
            try:
              proc.send_signal(signal.SIGTERM)
            except Exception: # pylint: disable=broad-except
              self.logger.exception('Exception while killing process')

            try:
              await asyncio.wait_for(proc.wait(), 10, loop=self.loop)
            except Exception: # pylint: disable=broad-except
              self.logger.exception('Exception while waiting for process')

          if proc.returncode is None:
            try:
              proc.send_signal(signal.SIGKILL)
            except Exception: # pylint: disable=broad-except
              self.logger.exception('Exception while killing process')

            try:
              await proc.wait()
            except Exception: # pylint: disable=broad-except
              self.logger.exception('Exception while waiting for process')

      # Read status before making the job finished
      await self._read_status(job_id, job_dir)

      if proc.returncode == 0:
        self._make_symlinks(param_id, hash_id, job_id)

        succeeded = True

    except concurrent.futures.CancelledError:
      # Pass through
      raise

    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id}')

    finally:
      self._remove_tmp_symlinks(param_id, hash_id)

    return succeeded

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
      raise
    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while reading status of job {job_id}')

  @rpc_export_function
  async def kill(self, job_ids=None, signal_type=None):
    '''Kill started jobs.'''
    queue_state = await self.queue.get_state()

    if job_ids is None:
      job_ids = [job['job_id'] for job in queue_state['started_jobs']]

    count = 0
    job_ids_set = set(job_ids)
    for job in queue_state['started_jobs']:
      if job['job_id'] in job_ids_set and job['pid'] is not None:
        job_id = job['job_id']
        if signal_type == 'int':
          self.logger.info(f'Interrupting job {job_id}')
          os.kill(job['pid'], signal.SIGINT)
        elif signal_type == 'kill':
          self.logger.info(f'Killing job {job_id}')
          os.kill(job['pid'], signal.SIGKILL)
        elif not signal_type or signal_type == 'term':
          self.logger.info(f'Terminating job {job_id}')
          os.kill(job['pid'], signal.SIGTERM)
        else:
          raise RuntimeError(f'Unknown signal: {signal_type}')
        count += 1
    return job_ids

  @rpc_export_function
  async def migrate(self, param_id_pairs, hash_id_pairs):
    '''Migrate symlinks for parameter and hash ID changes.'''

    migrated_param_id_pairs = []
    migrated_hash_id_pairs = []
    for i, (old_id, new_id) in enumerate(list(param_id_pairs) + list(hash_id_pairs)):
      old_path = os.path.join(self.base_dir, old_id)
      new_path = os.path.join(self.base_dir, new_id)

      if not os.path.lexists(old_path):
        self.logger.info(f'Ignoring missing symlink for old parameter {old_id}')
        continue

      if os.path.lexists(new_path):
        self.logger.info(f'Ignoring existing symlink for new parameter {new_id}')
        continue

      job_id = os.readlink(old_path).strip('/')
      os.symlink(job_id, new_path, target_is_directory=True)
      self.logger.info(f'Migrated symlink {old_id} to {new_id}')

      if i < len(param_id_pairs):
        migrated_param_id_pairs.append((old_id, new_id))
      else:
        migrated_hash_id_pairs.append((old_id, new_id))

    return migrated_param_id_pairs, migrated_hash_id_pairs

  @rpc_export_function
  async def job_ids(self):
    '''Return job IDs in the output directory.'''
    job_ids = []
    for filename in os.listdir(self.base_dir):
      if filename.startswith('j-'):
        job_ids.append(filename)
    return job_ids

  @rpc_export_function
  async def param_ids(self):
    '''Return parameter IDs in the output directory.
    Outputs may include temporary symlinks (*_tmp).'''
    param_ids = []
    for filename in os.listdir(self.base_dir):
      if filename.startswith('p-'):
        param_ids.append(filename)
    return param_ids

  @rpc_export_function
  async def hash_ids(self):
    '''Return hash IDs in the output directory.
    Outputs may include temporary symlinks (*_tmp).'''
    hash_ids = []
    for filename in os.listdir(self.base_dir):
      if filename.startswith('h-'):
        hash_ids.append(filename)
    return hash_ids

  def _remove_output(self, trash_dir, param_ids, hash_ids):
    '''Remove job output directories that (mis)matches given job IDs.'''

    removed_outputs = []
    for id_ in list(param_ids) + list(hash_ids):
      path = os.path.join(self.base_dir, id_)
      if not os.path.lexists(path):
        continue

      new_path = os.path.join(trash_dir, id_)
      if os.path.lexists(new_path):
        if os.path.isdir(new_path):
          rmdirs(new_path)
        else:
          os.unlink(new_path)

      os.rename(path, new_path)
      self.logger.info(f'Moved {id_} to trash')
      removed_outputs.append(id_)

    return removed_outputs

  def _remove_dangling_noref(self, trash_dir):
    '''Remove dangling last, p-*, or h-* symlinks and not referenced j-* directories in output.'''
    removed_outputs = []
    filenames = os.listdir(self.base_dir)
    filenames = set(filenames)

    valid_job_ids = set()
    for filename in sorted(filenames):
      if filename != 'last' and not filename.startswith('p-') and not filename.startswith('h-'):
        continue

      path = os.path.join(self.base_dir, filename)

      job_id = os.readlink(path).strip('/')
      if job_id in filenames:
        if filename != 'last':
          valid_job_ids.add(job_id)
        continue

      new_path = os.path.join(trash_dir, filename)
      if os.path.lexists(new_path):
        os.unlink(new_path)

      os.rename(path, new_path)
      self.logger.info(f'Moved {filename} to trash')
      removed_outputs.append(filename)

    for filename in sorted(filenames):
      if not filename.startswith('j-'):
        continue

      if filename in valid_job_ids:
        continue

      path = os.path.join(self.base_dir, filename)

      new_path = os.path.join(trash_dir, filename)
      if os.path.lexists(new_path):
        rmdirs(new_path)

      os.rename(path, new_path)
      self.logger.info(f'Moved {filename} to trash')
      removed_outputs.append(filename)
    return removed_outputs

  @rpc_export_function
  async def remove_output(self, param_ids, hash_ids):
    '''Remove job output data that match given IDs.'''
    trash_dir = os.path.join(self.base_dir, 'trash')
    if not os.path.exists(trash_dir):
      os.mkdir(trash_dir)

    queue_state = await self.queue.get_state()

    # Keep symlinks related to started jobs
    param_ids = set(param_ids)
    param_ids -= set([get_param_id(job['param']) for job in queue_state['started_jobs']])

    hash_ids = set(hash_ids)
    hash_ids -= set([get_hash_id(job['param']) for job in queue_state['started_jobs']])

    removed_output = self._remove_output(trash_dir, param_ids, hash_ids)
    removed_output += self._remove_dangling_noref(trash_dir)
    # Second pass to ensure deleting "last" if needed
    removed_output += self._remove_dangling_noref(trash_dir)
    return removed_output

  async def _read_output(self, id_, read_stdout, external_command, extra_args):
    '''Yield the output of an external command on the job output.'''
    assert id_.find('/') == -1 and id_.find('\\') == -1

    path = os.path.join(self.base_dir, id_)
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

  @rpc_export_generator
  async def cat_like(self, id_, read_stdout, external_command, extra_args):
    '''Run an external command on the job output.'''
    assert external_command in ['cat', 'head', 'tail']
    async for data in self._read_output(id_, read_stdout, external_command, extra_args):
      yield data
