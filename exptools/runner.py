'''Provides Runner.'''

from threading import Lock, Condition, Thread
from collections import namedtuple
import time
from .estimator import Estimator

__all__ = ['Runner']

class Runner:
  '''Run jobs with params.'''

  job_type = namedtuple('job_type', ['job_id', 'param'])
  state_type = namedtuple('state_type', [
      'succeeded_jobs', 'failed_jobs', 'active_jobs', 'pending_jobs', 'concurrency',
      ])

  def __init__(self, job_defs, init_resources=None, history_mgr=None):
    self.job_defs = job_defs
    self.history_mgr = history_mgr
    if init_resources is not None:
      self.resources = dict(init_resources)
    else:
      self.resources = {}

    self.lock = Lock()
    self.cond = Condition(self.lock)

    self.next_job_id = 0
    self.succeeded_jobs = []
    self.failed_jobs = []
    self.active_jobs = []
    self.pending_jobs = []
    self.concurrency = 1.

    self.main_t = None
    self.concurrency_update_t = None
    self.running = False
    self.stopping = False

    self.est = Estimator(history_mgr)

  def run(self):
    '''Run params.'''
    while True:
      with self.lock:
        if self.stopping:
          self.running = False
          break

        if not self.pending_jobs:
          self.cond.wait()
          continue

        job = self.pending_jobs[0]
        param = job.param
        demand = self.job_defs[param[0]].demand(param)

        if not self._is_available(demand):
          if not self.active_jobs:
            # Drop the job
            self.pending_jobs.pop(0)

            self._failed_demand(job, demand)

            self._check_empty_queue()
          else:
            # Retry when some job returns resources
            self.cond.wait()
          continue

        self.pending_jobs.pop(0)

        self._launch(job, param, demand)

  def start(self):
    '''Start the runner.'''
    assert not self.running

    with self.lock:
      self.running = True
      self.stopping = False

    assert self.main_t is None
    self.main_t = Thread(target=self.run, daemon=True)
    self.main_t.start()

    assert self.concurrency_update_t is None
    self.concurrency_update_t = Thread(target=self._update_concurrency, daemon=True)
    self.concurrency_update_t.start()

  def wait(self):
    '''Wait for the runner to process all jobs.'''
    # Wait for all jobs to finish
    while True:
      with self.lock:
        if not (self.active_jobs or self.pending_jobs):
          break
      time.sleep(1)

  def stop(self):
    '''Stop the runner.'''
    assert not self.stopping

    # Begin stopping
    with self.lock:
      self.stopping = True

    # Wake up the runner
    with self.lock:
      self.cond.notify()

    while True:
      with self.lock:
        if not self.running:
          break
      time.sleep(1)

    self.wait()

    # Join all threads
    self.main_t.join()
    self.main_t = None

    self.concurrency_update_t.join()
    self.concurrency_update_t = None

    self.stopping = False

  def _state(self):
    '''Get the runner state (locking assumed).'''
    assert self.lock.locked()
    return self.state_type(
        list(self.succeeded_jobs), list(self.failed_jobs),
        list(self.active_jobs), list(self.pending_jobs),
        self.concurrency,
        )

  def state(self):
    '''Get the runner state.'''
    with self.lock:
      return self._state()

  def pending(self):
    '''Return True if any param is running/to be run.'''
    current_state = self.state()
    return bool(current_state.active_jobs or current_state.pending_jobs)

  def add(self, params):
    '''Add new params to the job queue.  Duplicate params are ignored.'''
    with self.lock:
      new_jobs = [self.job_type(self.next_job_id + i, param) for i, param in enumerate(params)]
      job_ids = [job.job_id for job in new_jobs]
      self.next_job_id += len(params)

      self.pending_jobs = self._dedup(self.pending_jobs + new_jobs)
      self.cond.notify()
    return job_ids

  def remove(self, job_ids):
    '''Remove pending jobs from the job queue.'''
    job_ids = set(job_ids)
    with self.lock:
      self.pending_jobs = [job for job in self.pending_jobs if job.job_id not in job_ids]
      self.cond.notify()

  def _dedup(self, jobs):
    '''Deduplicate jobs with params that share the same hash.'''
    h_set = set()

    new_jobs = []
    for job in jobs:
      param = job.param

      param_hash = self.job_defs[param[0]].hash(param)
      if param_hash not in h_set:
        h_set.add(param_hash)
        new_jobs.append(job)

    return new_jobs

  def _launch(self, job, param, demand):
    '''Start the job with param in a per-param thread.'''
    assert self.lock.locked()

    thread = Thread(target=self._run_param, args=(job, param, demand), daemon=True)

    self._take(demand)
    self.active_jobs.append(job)

    if self.history_mgr is not None:
      self.history_mgr.started(param)
    print(f'Started:   job={job}')
    print(self.est.format_estimated_time(self._state()))

    thread.start()

  def _run_param(self, job, param, demand):
    '''Execute the job with param and wait for it to finish.'''
    success = None
    exception = None
    try:
      success = self.job_defs[param[0]].run(param)
    except Exception as exception_:
      exception = exception_
      raise
    finally:
      with self.lock:
        self._return(demand)

        for i, active_job in enumerate(self.active_jobs):
          if job.job_id == active_job.job_id:
            del self.active_jobs[i]
            break

        if exception is not None:
          self._failed_error(job, exception)
        if success:
          self._succeeded(job)
        else:
          self._failed_result(job)

        self._check_empty_queue()

        self.cond.notify()

  def _succeeded(self, job):
    '''Report a succeeded job.'''
    assert self.lock.locked()
    if self.history_mgr is not None:
      self.history_mgr.finished(job.param, True)
    self.succeeded_jobs.append(job)
    print(f'Succeeded: job={job}')
    print(self.est.format_estimated_time(self._state()))

  def _failed_result(self, job):
    '''Report a failed job due to a failed result.'''
    assert self.lock.locked()
    if self.history_mgr is not None:
      self.history_mgr.finished(job.param, False)
    self.failed_jobs.append(job)
    print(f'Failed:   job={job} (failed result)')
    print(self.est.format_estimated_time(self._state()))

  def _failed_demand(self, job, demand):
    '''Report a failed job due to unsatisfiable demand.'''
    assert self.lock.locked()
    if self.history_mgr is not None:
      self.history_mgr.finished(job.param, False)
    self.failed_jobs.append(job)
    print(f'Failed: job={job} (unable to satisfy demand: {demand})')
    print(self.est.format_estimated_time(self._state()))

  def _failed_error(self, job, error):
    '''Report a failed job due to an error.'''
    assert self.lock.locked()
    if self.history_mgr is not None:
      self.history_mgr.finished(job.param, False)
    self.failed_jobs.append(job)
    print(f'Failed:   job={job} (error: {error})')
    print(self.est.format_estimated_time(self._state()))

  def _check_empty_queue(self):
    assert self.lock.locked()
    if not (self.active_jobs or self.pending_jobs):
      print('Job queue empty')

  def _is_available(self, demand):
    '''Check if required resources are available.'''
    assert self.lock.locked()

    for res, req in demand.items():
      if self.resources[res] < req:
        return False
    return True

  def _take(self, demand):
    '''Acquire required resources.'''
    assert self.lock.locked()

    for res, req in demand.items():
      assert self.resources[res] >= req
      self.resources[res] -= req

  def _return(self, demand):
    '''Release required resources.'''
    assert self.lock.locked()

    for res, req in demand.items():
      self.resources[res] += req

  def _update_concurrency(self):
    '''Update the current concurrency.'''
    alpha = 0.9

    while self.running:
      with self.lock:
        self.concurrency = max(1., alpha * self.concurrency + (1. - alpha) * len(self.active_jobs))
      time.sleep(1)
