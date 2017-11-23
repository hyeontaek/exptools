"""Provide the Runner class."""

__all__ = ['Runner']

import asyncio
import concurrent
import json
import logging
import os
import signal

import aiofiles
import aionotify

from exptools.param import (
  get_param_id, get_hash_id,
  get_name, get_command, get_cwd, get_retry, get_retry_delay, get_time_limit)
from exptools.rpc_helper import rpc_export_function


class Runner:
  """Run jobs with parameters."""

  def __init__(self, queue, scheduler, output, loop):
    self.queue = queue
    self.scheduler = scheduler
    self.output = output
    self.loop = loop

    self.logger = logging.getLogger('exptools.Runner')

  async def run_forever(self):
    """Run scheduled jobs."""

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
  def _construct_env(job, job_paths):
    """Construct environment variables."""
    param = job['param']

    env = dict(os.environ)

    env['EXPTOOLS_JOB_DIR'] = job_paths['job_dir']
    env['EXPTOOLS_JOB_ID'] = job['job_id']
    env['EXPTOOLS_PARAM_ID'] = get_param_id(param)
    env['EXPTOOLS_HASH_ID'] = get_hash_id(param)
    env['EXPTOOLS_NAME'] = get_name(param)
    env['EXPTOOLS_CWD'] = get_cwd(param) or os.getcwd()
    env['EXPTOOLS_RETRY'] = str(get_retry(param))
    env['EXPTOOLS_RETRY_DELAY'] = str(get_retry_delay(param))
    env['EXPTOOLS_TIME_LIMIT'] = str(get_time_limit(param))

    env['EXPTOOLS_JOB_JSON_PATH'] = job_paths['job.json']
    env['EXPTOOLS_PARAM_JSON_PATH'] = job_paths['param.json']
    env['EXPTOOLS_RESOURCES_JSON_PATH'] = job_paths['resources.json']

    env['EXPTOOLS_STATUS_JSON_PATH'] = job_paths['status.json']
    return env

  async def _run(self, job):
    """Run a job."""

    job_id = job['job_id']

    if not await self.queue.set_started(job_id):
      self.logger.info(f'Ignoring missing job {job_id}')
      await self.scheduler.retire(job)
      return

    succeeded = False
    try:
      param = job['param']

      retry = get_retry(param)
      retry_delay = get_retry_delay(param)

      while True:
        current_retry = job['retry']
        if current_retry > retry:
          break

        if current_retry > 0:
          self.logger.info(f'Retrying {job_id}: {current_retry} / {retry}')
          await asyncio.sleep(retry_delay, loop=self.loop)

        succeeded = await self._try(job, job_id, param, current_retry)

        if succeeded:
          break

        await self.queue.increment_retry(job_id)

    finally:
      await self.scheduler.retire(job)

      await self.queue.set_finished(job_id, succeeded)

  async def _try(self, job, job_id, param, current_retry):
    """Run a job."""

    param_id = get_param_id(param)
    hash_id = get_hash_id(param)

    name = get_name(param)
    expanded_command = [arg.format(**param) for arg in get_command(param)]
    cwd = get_cwd(param) or os.getcwd()
    time_limit = get_time_limit(param)

    succeeded = False

    try:
      self.logger.info(f'Launching job {job_id}: {name}')

      job_paths = await self.output.make_job_directory(job, current_retry)
      job_paths = await self.output.create_job_files(job, job_paths)

      env = self._construct_env(job, job_paths)

      with self.output.open_job_stdio(job_paths) as stdio:
        stdout, stderr = stdio

        await self.output.make_tmp_symlinks(param_id, hash_id, job_paths)

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
          self._watch_status(job_id, job_paths), loop=self.loop)

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
            except Exception:  # pylint: disable=broad-except
              self.logger.exception('Exception while killing process')

            try:
              await asyncio.wait_for(proc.wait(), 10, loop=self.loop)
            except Exception:  # pylint: disable=broad-except
              self.logger.exception('Exception while waiting for process')

          if proc.returncode is None:
            try:
              proc.send_signal(signal.SIGKILL)
            except Exception:  # pylint: disable=broad-except
              self.logger.exception('Exception while killing process')

            try:
              await proc.wait()
            except Exception:  # pylint: disable=broad-except
              self.logger.exception('Exception while waiting for process')

      # Read status before making the job finished
      await self._read_status(job_id, job_paths)

      if proc.returncode == 0:
        await self.output.make_symlinks(param_id, hash_id, job_paths)

        succeeded = True

    except concurrent.futures.CancelledError:
      # Pass through
      raise

    except Exception:  # pylint: disable=broad-except
      self.logger.exception(f'Exception while running job {job_id}')

    finally:
      await self.output.remove_tmp_symlinks(param_id, hash_id)

    return succeeded

  async def _watch_status(self, job_id, job_paths):
    """Watch the status file changes."""
    status_path = job_paths['status.json']

    watcher = aionotify.Watcher()
    watcher.watch(status_path, aionotify.Flags.CLOSE_WRITE)
    await watcher.setup(self.loop)
    try:
      while True:
        try:
          await self._read_status(job_id, job_paths)
          await watcher.get_event()
          self.logger.debug(f'Detected status change for job {job_id}')
        except concurrent.futures.CancelledError:
          # Break loop (likely normal exit through task cancellation)
          break
        except Exception:  # pylint: disable=broad-except
          self.logger.exception(f'Exception while watching status of job {job_id}')
    finally:
      watcher.unwatch(status_path)
      watcher.close()

  async def _read_status(self, job_id, job_paths):
    """Read the status file and send it to the queue."""
    status_path = job_paths['status.json']

    try:
      async with aiofiles.open(status_path) as file:
        status_json = await file.read()

      status = json.loads(status_json)

      # Validate common value types
      assert isinstance(status, dict)

      progress = status.get('progress', 0.)
      assert 0. <= progress <= 1.

      assert isinstance(status.get('message', ''), str)

      await self.queue.set_status(job_id, status)
    except concurrent.futures.CancelledError:
      # Ignore (likely normal exit through task cancellation)
      raise
    except Exception:  # pylint: disable=broad-except
      self.logger.exception(f'Exception while reading status of job {job_id}')

  @rpc_export_function
  async def kill(self, job_ids=None, signal_type=None):
    """Kill started jobs."""
    assert signal_type in [None, 'int', 'kill', 'term']
    queue_state = await self.queue.get_state()

    if job_ids is None:
      job_ids = [job['job_id'] for job in queue_state['started_jobs']]

    count = 0
    job_ids_set = set(job_ids)
    for job in queue_state['started_jobs']:
      if job['job_id'] in job_ids_set and job['pid'] is not None:
        job_id = job['job_id']
        await self.queue.increment_retry(job_id, set_to_max=True)
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
          assert False
        count += 1
    return job_ids
