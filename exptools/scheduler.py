'''Define a scheduler interface and implement basic schedulers.'''

__all__ = [
    'Scheduler',
    'SerialScheduler',
    'get_scheduler',
    ]

import asyncio
import concurrent
import json
import logging
import os
import re

from exptools.rpc_helper import rpc_export_function

# pylint: disable=too-few-public-methods
class Scheduler:
  '''A scheduler interface.'''

  def __init__(self, initial_mode, path, history, queue, loop):
    self.initial_mode = initial_mode
    self.path = path
    self.history = history
    self.queue = queue
    self.loop = loop

    self.lock = asyncio.Condition(loop=self.loop)

    self.logger = logging.getLogger('exptools.Scheduler')

    self.running = False
    self.oneshot = False

    self._load_conf()

  def _load_conf(self):
    if os.path.exists(self.path):
      self.conf = json.load(open(self.path))
      self.logger.info(f'Loaded scheduler configuration at {self.path}')
    else:
      self.conf = None
      self.logger.warning(f'No configuration found at {self.path}')

  async def run_forever(self):
    '''Run the scheduler.  Note that scheduling jobs is done by schedule().'''
    try:
      if self.initial_mode == 'start':
        await self.start()
      elif self.initial_mode == 'stop':
        pass
      elif self.initial_mode == 'oneshot':
        await self.set_oneshot()
      else:
        assert False

      # Sleep forever
      while True:
        await asyncio.sleep(60, loop=self.loop)
    except concurrent.futures.CancelledError:
      pass

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

  async def retire(self, job):
    '''Return resources of a finished job.'''
    pass

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
  '''A scheduler that chooses one job one at a time in order.'''

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

  async def retire(self, job):
    '''Return resources of a finished job.'''
    pass

  @rpc_export_function
  async def add_resource(self, key, value):
    '''Add a resource.'''
    return False

  @rpc_export_function
  async def remove_resource(self, key, value):
    '''Remove a resource.'''
    return False

class GreedyScheduler(Scheduler):
  '''A scheduler that chooses the first job if it can run.'''
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    if self.conf:
      self.resources = self.conf['resources']
    else:
      self.resources = {}

    for key, value in self.resources.items():
      assert isinstance(key, str), 'Key must be a string.'
      assert isinstance(value, int), 'Only integer amount is supported'
      assert value >= 0

    self.logger.info(f'Resources: {self.resources}')

  async def schedule(self):
    '''Schedule a job.'''
    import pyomo.environ
    import pyomo.opt

    async for queue_state in self.queue.watch_state():
      # Ignore the queue change if not running
      if not self.running:
        continue

      # Nothing to schedule
      if not queue_state['queued_jobs']:
        continue

      if self.oneshot:
        self.running = False

      try:
        # Get resource types
        resource_names = list(self.resources.keys())

        # Choose the first queued job
        job = queue_state['queued_jobs'][0]
        job_id = job['job_id']
        param_id = job['param_id']
        param = job['param']
        meta = param.get('_', None)

        if meta and 'demands' in meta:
          demands = list(meta['demands'].items())
        else:
          demands = list(self.resources.items())
        demand_count = len(demands)

        model = pyomo.environ.ConcreteModel()

        # Parameter: Set of resource types
        model.r = pyomo.environ.Set(initialize=resource_names, doc='Resources')

        # Parameter: Set of resource types
        model.d = pyomo.environ.RangeSet(0, demand_count - 1, doc='Demand indices')

        # Variable: x[r]: Job uses Resource r by x[r]
        model.x = pyomo.environ.Var(model.r, within=pyomo.environ.NonNegativeIntegers)

        # Constraint: Supply
        # pylint: disable=invalid-name
        # pylint: disable=cell-var-from-loop
        def _supply_rule(model, r):
          return model.x[r] <= self.resources[r]

        model.supply = pyomo.environ.Constraint(model.r, rule=_supply_rule, doc='Supply')

        # Constraint: Demand
        # pylint: disable=invalid-name
        # pylint: disable=cell-var-from-loop
        def _demand_rule(model, d):
          resource_pattern, requirement = demands[d]

          candidate_resources = [
              r for r in model.r \
              if re.fullmatch(resource_pattern, r) is not None]

          if not candidate_resources:
            raise RuntimeError(f'No suitable resource found for {job_id} {param_id}: ' + \
                               f'{resource_pattern}')

          return sum([model.x[r] for r in candidate_resources]) == requirement

        model.demand = pyomo.environ.Constraint(model.d, rule=_demand_rule, doc='Demand')

        # Objective
        def _objective_rule(model):
          return sum([model.x[r] for r in model.r])

        model.objective = pyomo.environ.Objective(
            rule=_objective_rule, sense=pyomo.environ.minimize)

        # Solve
        opt = pyomo.opt.SolverFactory('cbc')
        results = opt.solve(model)

        #model.pprint()
        #model.x.display()

        # pylint: disable=no-member
        if results.solver.status != pyomo.opt.SolverStatus.ok or \
          results.solver.termination_condition != pyomo.opt.TerminationCondition.optimal:
          # No suitable solution found
          self.logger.warning(f'Insufficient resources for {job_id} {param_id}')
          continue

        # Resource allocation possible
        allocation = {}
        for r in model.r:
          x_r = pyomo.environ.value(model.x[r])
          if x_r > 0:
            allocation[r] = x_r
            self.resources[r] -= x_r

        # Store the resource allocation in the job object and also in the queue state
        # because they have separate objects
        job['resources'] = allocation
        await self.queue.set_resources(job_id, allocation)

        self.logger.info(f'Scheduled {job_id} {param_id}: {allocation}')
        yield job

      except Exception: # pylint: disable=broad-except
        self.logger.exception(f'Exception while scheduling {job_id} {param_id}')

  async def retire(self, job):
    '''Return resources of a finished job.'''
    try:
      job_id = job['job_id']
      param_id = job['param_id']
      allocation = job['resources']
      async with self.lock:
        for key, value in allocation.items():
          self.resources[key] = self.resources.get(key, 0) + value
        self.logger.info(f'Retired {job_id} {param_id}: {allocation}')
    except Exception: # pylint: disable=broad-except
      self.logger.exception(f'Exception while retiring {job_id} {param_id}')

  @rpc_export_function
  async def add_resource(self, key, value):
    '''Add a resource.'''
    assert isinstance(key, str)
    assert isinstance(value, int)
    assert value >= 0
    with self.lock:
      self.resources[key] = self.resources.get(key, 0) + value
    return True

  @rpc_export_function
  async def remove_resource(self, key, value):
    '''Remove a resource.'''
    assert isinstance(key, str)
    assert isinstance(value, int)
    assert value >= 0
    with self.lock:
      assert self.resources.get(key, 0) >= value
      self.resources[key] = self.resources.get(key, 0) - value
    return True

def get_scheduler(scheduler):
  '''Return a matching scheduler.'''
  schedulers = {
      None: SerialScheduler,
      'serial': SerialScheduler,
      'greedy': GreedyScheduler,
      }
  return schedulers[scheduler]
