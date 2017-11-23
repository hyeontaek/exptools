"""Provide the Output class."""

__all__ = ['Output']

import asyncio
import base64
import contextlib
import json
import logging
import os
import random

import aiofiles

from exptools.file import mkdirs, rmdirs
from exptools.param import get_param_id, get_hash_id
from exptools.rpc_helper import rpc_export_function, rpc_export_generator


class Output:
  """Store and manage job output data."""

  def __init__(self, path, queue, loop):
    self.path = path
    self.queue = queue
    self.loop = loop

    self.logger = logging.getLogger('exptools.Output')

    self.lock = asyncio.Condition(loop=self.loop)

    if not os.path.exists(self.path):
      mkdirs(self.path, ignore_errors=False)

  async def run_forever(self):
    """Manage job output data."""
    try:
      while True:
        await asyncio.sleep(60, loop=self.loop)
    finally:
      pass

  async def make_job_directory(self, job, current_retry):
    """Make a unique job directory."""

    async with self.lock:
      job_dir_basename = job['job_id']
      if current_retry > 0:
        job_dir_basename += f'_{current_retry}'
      while True:
        job_dir = os.path.join(self.path, job_dir_basename)
        if not os.path.exists(job_dir):
          break
        job_dir_basename += f'_{random.randint(0, 9999)}'

      os.mkdir(job_dir)

    job_paths = {
      'job_dir_basename': job_dir_basename,
      'job_dir': job_dir,
      }

    return job_paths

  @staticmethod
  async def create_job_files(job, job_paths):
    """Create job files."""

    job_dir = job_paths['job_dir']
    job_paths = dict(job_paths)

    # Dump job
    job_paths['job.json'] = os.path.join(job_dir, 'job.json')
    async with aiofiles.open(job_paths['job.json'], 'wt') as file:
      await file.write(json.dumps(job) + '\n')

    # Dump structured properties
    for key in ['param', 'resources']:
      job_paths[key + '.json'] = os.path.join(job_dir, key + '.json')
      async with aiofiles.open(job_paths[key + '.json'], 'wt') as file:
        await file.write(json.dumps(job[key]) + '\n')

    # Create a stub for status
    job_paths['status.json'] = os.path.join(job_dir, 'status.json')
    async with aiofiles.open(job_paths['status.json'], 'wt') as file:
      status = {'progress': 0.}
      await file.write(json.dumps(status) + '\n')

    return job_paths

  async def make_tmp_symlinks(self, param_id, hash_id, job_paths):
    """Make temporary symlinks for param_id and hash_id pointing to the job's output data."""
    job_dir_basename = job_paths['job_dir_basename']

    param_path = os.path.join(self.path, param_id)
    hash_path = os.path.join(self.path, hash_id)

    async with self.lock:
      if os.path.lexists(param_path + '_tmp'):
        os.unlink(param_path + '_tmp')
      os.symlink(job_dir_basename, param_path + '_tmp', target_is_directory=True)

      if os.path.lexists(hash_path + '_tmp'):
        os.unlink(hash_path + '_tmp')
      os.symlink(job_dir_basename, hash_path + '_tmp', target_is_directory=True)

      last_path = os.path.join(self.path, 'last')
      if os.path.lexists(last_path):
        os.unlink(last_path)
      os.symlink(job_dir_basename, last_path, target_is_directory=True)

  async def make_symlinks(self, param_id, hash_id, job_paths):
    """Make permanent symlinks for param_id and hash_id pointing to the job's output data."""
    job_dir_basename = job_paths['job_dir_basename']

    param_path = os.path.join(self.path, param_id)
    hash_path = os.path.join(self.path, hash_id)

    async with self.lock:
      if os.path.lexists(param_path):
        os.unlink(param_path)
      os.symlink(job_dir_basename, param_path, target_is_directory=True)

      if os.path.lexists(hash_path):
        os.unlink(hash_path)
      os.symlink(job_dir_basename, hash_path, target_is_directory=True)

  async def remove_tmp_symlinks(self, param_id, hash_id):
    """Remove temporary symlinks."""
    param_path = os.path.join(self.path, param_id)
    hash_path = os.path.join(self.path, hash_id)

    async with self.lock:
      if os.path.lexists(param_path + '_tmp'):
        os.unlink(param_path + '_tmp')
      if os.path.lexists(hash_path + '_tmp'):
        os.unlink(hash_path + '_tmp')

  @staticmethod
  @contextlib.contextmanager
  def open_job_stdio(job_paths):
    """Open files to store the standard output and error."""
    job_dir = job_paths['job_dir']
    # Simply open() files because these handlers will be used for asyncio.create_subprocess_exec()
    with open(os.path.join(job_dir, 'stderr'), 'wb', buffering=0) as stderr:
      with open(os.path.join(job_dir, 'stdout'), 'wb', buffering=0) as stdout:
        yield (stdout, stderr)

  @rpc_export_function
  async def migrate(self, param_id_pairs, hash_id_pairs):
    """Migrate symlinks for parameter and hash ID changes."""

    async with self.lock:
      migrated_param_id_pairs = []
      migrated_hash_id_pairs = []
      for i, (old_id, new_id) in enumerate(list(param_id_pairs) + list(hash_id_pairs)):
        old_path = os.path.join(self.path, old_id)
        new_path = os.path.join(self.path, new_id)

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
    """Return job IDs in the output directory."""
    job_ids = []
    for filename in os.listdir(self.path):
      if filename.startswith('j-'):
        job_ids.append(filename)
    return job_ids

  @rpc_export_function
  async def param_ids(self):
    """Return parameter IDs in the output directory.
    Outputs may include temporary symlinks (*_tmp)."""
    param_ids = []
    for filename in os.listdir(self.path):
      if filename.startswith('p-'):
        param_ids.append(filename)
    return param_ids

  @rpc_export_function
  async def hash_ids(self):
    """Return hash IDs in the output directory.
    Outputs may include temporary symlinks (*_tmp)."""
    hash_ids = []
    for filename in os.listdir(self.path):
      if filename.startswith('h-'):
        hash_ids.append(filename)
    return hash_ids

  def _remove_job_output(self, trash_dir, param_ids, hash_ids):
    """Remove job output directories that (mis)matches given job IDs."""
    assert self.lock.locked()

    removed_outputs = []
    for id_ in list(param_ids) + list(hash_ids):
      path = os.path.join(self.path, id_)
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
    """Remove dangling last, p-*, or h-* symlinks and not referenced j-* directories in output."""
    assert self.lock.locked()

    removed_outputs = []
    filenames = os.listdir(self.path)
    filenames = set(filenames)

    valid_job_ids = set()
    for filename in sorted(filenames):
      if filename != 'last' and not filename.startswith('p-') and not filename.startswith('h-'):
        continue

      path = os.path.join(self.path, filename)

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

      path = os.path.join(self.path, filename)

      new_path = os.path.join(trash_dir, filename)
      if os.path.lexists(new_path):
        rmdirs(new_path)

      os.rename(path, new_path)
      self.logger.info(f'Moved {filename} to trash')
      removed_outputs.append(filename)
    return removed_outputs

  @rpc_export_function
  async def remove(self, param_ids, hash_ids):
    """Remove job output data that match given IDs."""
    async with self.lock:
      trash_dir = os.path.join(self.path, 'trash')
      if not os.path.exists(trash_dir):
        os.mkdir(trash_dir)

      queue_state = await self.queue.get_state()

      # Keep symlinks related to started/queued jobs
      jobs = queue_state['started_jobs'] + queue_state['queued_jobs']

      param_ids = set(param_ids)
      param_ids -= set([get_param_id(job['param']) for job in jobs])
      param_ids -= set([get_param_id(job['param']) + '_tmp' for job in jobs])

      hash_ids = set(hash_ids)
      hash_ids -= set([get_hash_id(job['param']) for job in jobs])
      hash_ids -= set([get_hash_id(job['param']) + '_tmp' for job in jobs])

      removed_output = self._remove_job_output(trash_dir, param_ids, hash_ids)
      removed_output += self._remove_dangling_noref(trash_dir)
      # Second pass to ensure deleting "last" if needed
      removed_output += self._remove_dangling_noref(trash_dir)
      return removed_output

  async def _read_output(self, id_, read_stdout, external_command, extra_args):
    """Yield the output of an external command on the job output."""
    assert id_.find('/') == -1 and id_.find('\\') == -1

    path = os.path.join(self.path, id_)
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
    except Exception:  # pylint: disable=broad-except
      proc.terminate()
      raise
    finally:
      try:
        await proc.wait()
      except Exception:  # pylint: disable=broad-except
        self.logger.exception('Exception while waiting for process')

  @rpc_export_generator
  async def cat_like(self, id_, read_stdout, external_command, extra_args):
    """Run an external command on the job output."""
    assert external_command in ['cat', 'head', 'tail']
    async for data in self._read_output(id_, read_stdout, external_command, extra_args):
      yield data
