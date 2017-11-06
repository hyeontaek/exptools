"""Provide the Magic class."""

__all__ = ['Magic']

import asyncio
import gc
import logging
import os
import sys

import aiofiles


class Magic:
  """Handle magic commands."""

  def __init__(self, path, loop):
    self.path = path
    self.loop = loop

    self.logger = logging.getLogger(f'exptools.Magic')
    self.logger.info(f'Using magic at {self.path}')

  async def run_forever(self):
    '''Process magic commands.'''
    if os.path.exists(self.path):
      os.unlink(self.path)

    while True:
      if not os.path.exists(self.path):
        await asyncio.sleep(10, loop=self.loop)
        continue

      try:
        async with aiofiles.open(self.path) as file:
          cmd = await file.read()
          cmd = cmd.strip()

          if cmd == 'gc':
            print('garbage collected', file=sys.stderr)
            gc.collect()

          elif cmd == 'stack':
            tasks = asyncio.Task.all_tasks(loop=self.loop)
            print('=' * 20, file=sys.stderr)
            print('', file=sys.stderr)
            for task in tasks:
              task.print_stack(file=sys.stderr)
              print('', file=sys.stderr)
            print('=' * 20, file=sys.stderr)

          else:
            self.logger.error(f'Unrecognized: {cmd}', file=sys.stderr)

      finally:
        try:
          os.unlink(self.path)
        except OSError:
          self.logger.exception('Cannot unlink magic file')
