"""Provide the Magic class."""

__all__ = ['Magic']

import asyncio
import concurrent
import gc
import logging
import os
import sys

import aiofiles
import aionotify


class Magic:
  """Handle magic commands."""

  def __init__(self, path, loop):
    self.path = path
    self.loop = loop

    self.logger = logging.getLogger(f'exptools.Magic')
    self.logger.info(f'Using magic at {self.path}')

  async def run_forever(self):
    '''Process magic commands.'''
    open(self.path, 'w').close()

    watcher = aionotify.Watcher()
    watcher.watch(self.path, aionotify.Flags.CLOSE_WRITE)
    await watcher.setup(self.loop)
    try:
      while True:
        try:
          await watcher.get_event()
          await self._handle()
          self.logger.debug(f'Detected status change for magic file')
        except concurrent.futures.CancelledError:
          # Break loop (likely normal exit through task cancellation)
          break
        except Exception:  # pylint: disable=broad-except
          self.logger.exception(f'Exception while watching magic file')
    finally:
      watcher.unwatch(self.path)
      watcher.close()

  async def _handle(self):
    async with aiofiles.open(self.path) as file:
      cmd = await file.read()
      cmd = cmd.strip().split(' ')

      if cmd[0] == 'gc':
        print('garbage collected', file=sys.stderr)
        gc.collect()

      elif cmd[0] == 'stack':
        tasks = asyncio.Task.all_tasks(loop=self.loop)
        if cmd[1:]:
          if cmd[1] == 'running':
            tasks = [task for task in tasks if not task.done()]
          elif cmd[1] == 'done':
            tasks = [task for task in tasks if task.done()]
          elif cmd[1] == 'cancelled':
            tasks = [task for task in tasks if task.cancelled()]

        print('=' * 20, file=sys.stderr)
        print('', file=sys.stderr)
        for task in tasks:
          task.print_stack(file=sys.stderr)
          print('', file=sys.stderr)
        print('=' * 20, file=sys.stderr)

      else:
        print(f'Unrecognized: {cmd}', file=sys.stderr)
