'''Define a scheduler interface and implement basic schedulers.'''

__all__ = [
    'Scheduler',
    'SerialScheduler',
    ]

import asyncio
import logging

from exptools.rpc_helper import rpc_export_function

# pylint: disable=too-few-public-methods
class Scheduler:
  '''A scheduler interface.'''

  def __init__(self, queue, loop):
    self.queue = queue
    self.loop = loop

    self.logger = logging.getLogger('exptools.Scheduler')

    self.running = False
    self.oneshot = False

    asyncio.ensure_future(self.start(), loop=loop)

  @rpc_export_function
  async def is_running(self):
    '''Return True if running.'''
    return self.running

  @rpc_export_function
  async def is_oneshot(self):
    '''Return True if oneshot mode is enabled.'''
    return self.oneshot

  @rpc_export_function
  async def start(self):
    '''Start the scheduler.'''
    if self.running:
      self.logger.error('Already started')
      return False

    self.logger.info('Started')
    self.running = True
    self.oneshot = False

    await self.queue.notify()
    return True

  @rpc_export_function
  async def set_oneshot(self):
    '''Start the scheduler and stop it after scheduling one job.'''
    self.logger.info('Using oneshot mode')
    self.running = True
    self.oneshot = True

    await self.queue.notify()
    return True

  @rpc_export_function
  async def stop(self):
    '''Stop the runner.'''
    if not self.running:
      self.logger.error('Already stopped')
      return False

    self.running = False
    self.oneshot = False
    self.logger.info('Stopped')
    return True

  async def schedule(self):
    '''Schedule a job.'''
    raise NotImplementedError()

  @rpc_export_function
  async def add_resource(self, key, value):
    '''Add a resource.'''
    raise NotImplementedError()

  @rpc_export_function
  async def remove_resource(self, key, value):
    '''Remove a resource.'''
    raise NotImplementedError()

# pylint: disable=too-few-public-methods
class SerialScheduler(Scheduler):
  '''A scheduler that runs a single job at a time in order.'''

  #def __init__(self, *args, **kwargs):
  #  super().__init__(*args, **kwargs)
  #  self.logger = logging.getLogger('exptools.SerialScheduler')

  async def schedule(self):
    '''Schedule a job.'''
    async for queue_state in self.queue.watch_state():
      # Ignore the queue change if not running
      if not self.running:
        continue

      # Nothing to schedule
      if not queue_state['queued_jobs']:
        continue

      # Avoid concurrent execution
      if queue_state['started_jobs']:
        continue

      if self.oneshot:
        self.running = False

      # Choose the first queued job
      yield queue_state['queued_jobs'][0]

  async def add_resource(self, key, value):
    '''Add a resource.'''
    pass

  async def remove_resource(self, key, value):
    '''Remove a resource.'''
    pass
