'''Provide the Runner class.'''

__all__ = ['Runner']

import logging
from threading import Lock, Condition, Thread
import traceback
import weakref
import termcolor
from exptools.estimator import Estimator
from exptools.history import History
from exptools.job import Job
from exptools.param import Param
from exptools.time import diff_sec, format_sec, utcnow
from exptools.work import ResourceError, Work

class RunnerState:
  '''Store the state of Runner.'''

  def __init__(self, runner):
    self.runner = runner
    self.succeeded_jobs = []
    self.failed_jobs = []
    self.active_jobs = []
    self.pending_jobs = []
    self.concurrency = 1.

  def reset(self):
    '''Clear all jobs except active jobs.'''
    self.succeeded_jobs = []
    self.failed_jobs = []
    self.pending_jobs = []

  def reset_pending(self):
    '''Clear pending jobs.'''
    self.pending_jobs = []

  def clone(self):
    '''Clone the job queue state.'''
    state = RunnerState(self.runner)
    state.succeeded_jobs = list(self.succeeded_jobs)
    state.failed_jobs = list(self.failed_jobs)
    state.active_jobs = list(self.active_jobs)
    state.pending_jobs = list(self.pending_jobs)
    state.concurrency = self.concurrency
    return state

  def __str__(self):
    '''Format the job queue state.'''
    return f'succeeded_jobs={self.succeeded_jobs}, failed_jobs={self.failed_jobs}, ' + \
           f'active_jobs={self.active_jobs}, pending_jobs={self.pending_jobs}, ' + \
           f'concurrency={self.concurrency}'

  def format(self):
    '''Format the job queue state in detail.'''
    output = f'Succeeded jobs ({len(self.succeeded_jobs)}):\n'
    for job in self.succeeded_jobs:
      output += f'  {job} '
      output += f'[elapsed: {self.runner.format_elapsed_time(job)}]\n'
    output += '\n'

    output += f'Failed jobs ({len(self.failed_jobs)}):\n'
    for job in self.failed_jobs:
      output += f'  {job} '
      output += f'[elapsed: {self.runner.format_elapsed_time(job)}]\n'
    output += '\n'

    output += f'Active jobs ({len(self.active_jobs)}):\n'
    for job in self.active_jobs:
      output += f'  {job} '
      output += f'[elapsed: {self.runner.format_elapsed_time(job)}]\n'
    output += '\n'

    output += f'Pending jobs ({len(self.pending_jobs)}):\n'
    for job in self.pending_jobs:
      output += f'  {job}\n'
    output += '\n'

    output += f'Concurrency: {self.concurrency}'

    return output

class Runner:
  '''Run jobs with params.'''

  def __init__(self, hist=None):
    if hist is not None:
      self.hist = hist
    else:
      self.hist = History(path=None)

    self.lock = Lock()
    self.queue_update_cond = Condition(self.lock)
    self.sleep_cond = Condition()

    self.next_job_id = 0
    self._state = RunnerState(self)

    self.main_t = None
    self.concurrency_update_t = None
    self.active_job_t = {}
    self.joinable_job_t = []

    self.running = False
    self.stopping = False

    self.estimator = Estimator(hist)

    self.logger = logging.getLogger('exptools.Runner')

  def __del__(self):
    self.kill_all()

    if self.running:
      self.stop()

  def run(self):
    '''Run params.'''
    assert not self.stopping
    assert not self.running

    self.running = True
    self._run(weakref.ref(self))
    self.running = False

  # pylint: disable=protected-access
  @staticmethod
  def _run(weak_self):
    '''Run params.  This is intended for internal use because it takes a weak reference to self.'''

    while True:
      self = weak_self()
      if self is None:
        break

      with self.lock:
        if self.stopping:
          self.running = False
          break

        if not self._state.pending_jobs:
          queue_update_cond = self.queue_update_cond
          del self
          queue_update_cond.wait()
          continue

        job = self._state.pending_jobs.pop(0)

        try:
          job.work.setup(job)
        except ResourceError:
          if not self._state.active_jobs:
            # Drop the job if there was no active jobs
            exc = traceback.format_exc()
            self._failed_resource_error(job, exc)

            self._check_empty_queue()
            self.queue_update_cond.notify_all()
          else:
            # Retry when some job finishes
            self._state.pending_jobs.insert(0, job)

            queue_update_cond = self.queue_update_cond
            del self
            queue_update_cond.wait()
          continue
        except Exception: # pylint: disable=broad-except
          # Drop the job
          exc = traceback.format_exc()
          self._failed_exception(job, exc)

          self._check_empty_queue()
          self.queue_update_cond.notify_all()

        self._launch(job)

        while self.joinable_job_t:
          self.joinable_job_t.pop().join()

  def start(self):
    '''Start the runner.'''
    assert not self.running

    self.logger.info('Started Runner')

    with self.lock:
      self.running = True
      self.stopping = False

    assert self.main_t is None
    self.main_t = Thread(target=Runner._run, args=(weakref.ref(self),), name='Runner.main_t')
    self.main_t.start()

    assert self.concurrency_update_t is None
    self.concurrency_update_t = Thread(target=Runner._update_concurrency, args=(weakref.ref(self),),
                                       name='Runner.concurrency_update_t')
    self.concurrency_update_t.start()

  def wait(self, active_only=False):
    '''Wait for the runner to process all jobs.'''
    while True:
      with self.lock:
        if active_only:
          if not self._state.active_jobs:
            break
        else:
          if not (self._state.active_jobs or self._state.pending_jobs):
            break
        self.queue_update_cond.wait(1)

  def stop(self):
    '''Stop the runner.'''
    assert not self.stopping

    # Begin stopping
    with self.lock:
      self.stopping = True

    # Wake up threads
    with self.lock:
      self.queue_update_cond.notify_all()

    with self.sleep_cond:
      self.sleep_cond.notify()

    # Join runner threads
    self.main_t.join()
    self.main_t = None

    self.concurrency_update_t.join()
    self.concurrency_update_t = None

    # Join job threads
    self.wait(active_only=True)

    with self.lock:
      while self.joinable_job_t:
        self.joinable_job_t.pop().join()
      assert not self.active_job_t

    self.stopping = False

    self.logger.info('Stopped Runner')

  def reset(self):
    '''Clear all jobs except active jobs.'''
    with self.lock:
      self._state.reset()

  def state(self):
    '''Get the runner state.'''
    with self.lock:
      return self._state.clone()

  def pending(self):
    '''Return True if any param is running/to be run.'''
    current_state = self.state()
    return bool(current_state.active_jobs or current_state.pending_jobs)

  def add(self, work_list, params, keep_newer=True):
    '''Add new params to the job queue with optionally a new priority.
    Duplicate params with the same execution ID in the pending jobs are ignored.
    New params are kept if keep_newer is True; old params are kept otherwise.'''
    if isinstance(params, Param):
      params = [params]
    if isinstance(work_list, Work):
      work_list = [work_list] * len(params)

    assert len(work_list) == len(params)

    with self.lock:
      new_jobs = [Job(self.next_job_id + i, work_list[i], params[i]) for i in range(len(params))]
      job_ids = [job.job_id for job in new_jobs]
      self.next_job_id += len(params)

      prev_pending_count = len(self._state.pending_jobs)
      self._state.pending_jobs = \
          self._sort(self._dedup(self._state.pending_jobs + new_jobs, keep_newer))
      new_pending_count = len(self._state.pending_jobs)
      self.queue_update_cond.notify_all()
    self.logger.info(f'Added {new_pending_count - prev_pending_count} jobs')
    return job_ids

  def remove(self, job_ids):
    '''Remove pending jobs from the job queue.'''
    if isinstance(job_ids, int):
      job_ids = [job_ids]
    job_ids = set(job_ids)
    with self.lock:
      prev_pending_count = len(self._state.pending_jobs)
      self._state.pending_jobs = \
          [job for job in self._state.pending_jobs if job.job_id not in job_ids]
      new_pending_count = len(self._state.pending_jobs)
      self.queue_update_cond.notify_all()
    self.logger.info(f'Removed {prev_pending_count - new_pending_count} jobs')

  def remove_all(self):
    '''Remove all pending jobs.'''
    with self.lock:
      prev_pending_count = len(self._state.pending_jobs)
      self._state.reset_pending()
      new_pending_count = len(self._state.pending_jobs)
      self.queue_update_cond.notify_all()
    self.logger.info(f'Removed {prev_pending_count - new_pending_count} jobs')

  def kill(self, job_ids):
    '''Kill active jobs.'''
    if isinstance(job_ids, int):
      job_ids = [job_ids]
    job_ids = set(job_ids)
    with self.lock:
      for job in self._state.active_jobs:
        if job.job_id in job_ids:
          job.work.kill(job)

  def kill_all(self):
    '''Kill all active jobs.'''
    with self.lock:
      for job in self._state.active_jobs:
        job.work.kill(job)

  @staticmethod
  def _dedup(jobs, keep_newer):
    '''Deduplicate jobs with params that share the same execution ID.'''
    id_to_loc = {}

    new_jobs = []
    for job in jobs:
      exec_id = job.param.exec_id
      if exec_id not in id_to_loc:
        id_to_loc[exec_id] = len(new_jobs)
        new_jobs.append(job)
      else:
        if keep_newer and new_jobs[id_to_loc[exec_id]].param.param_id != job.param.param_id:
          # Replace an existing param with a new param,
          # but keep the old param if it is identical to the new param
          # (e.g., no priority changes)
          new_jobs[id_to_loc[exec_id]] = job

    return new_jobs

  @staticmethod
  def _sort(jobs):
    '''Sort jobs based on their priority and job ID.'''
    return sorted(jobs, key=Job.sort_key)

  def _launch(self, job):
    '''Start a job in a per-param thread.'''
    assert self.lock.locked() # pylint: disable=no-member

    thread = Thread(target=self._job_main, args=(job,),
                    name=f'Runner.job-{job.job_id}')

    self._state.active_jobs.append(job)
    self.active_job_t[job.job_id] = thread

    self.hist.started(job.param)
    self.logger.info(termcolor.colored(f'Started:   {job}', 'blue'))
    self.logger.info(self._format_estimated_time())

    thread.start()

  def _job_main(self, job):
    '''Execute a job and wait for it to finish.'''
    exc = None
    try:
      job.work.run(job)
    except Exception: # pylint: disable=broad-except
      exc = traceback.format_exc()
    finally:
      with self.lock:
        for i, active_job in enumerate(self._state.active_jobs):
          if job.job_id == active_job.job_id:
            del self._state.active_jobs[i]
            break
        else:
          raise RuntimeError(f'Missing active job for {job}')

        if exc is not None:
          self._failed_exception(job, exc)
        else:
          self._succeeded(job)

        self._check_empty_queue()
        self.queue_update_cond.notify_all()

        self.joinable_job_t.append(self.active_job_t[job.job_id])
        del self.active_job_t[job.job_id]

        job.work.cleanup(job)

  def format_elapsed_time(self, job):
    '''Format the elapsed time of a job.'''
    hist_entry = self.hist.get(job.param)
    started = hist_entry['started']
    finished = hist_entry['finished']
    if not started:
      return format_sec(0.)
    if not finished:
      return format_sec(diff_sec(utcnow(), started))
    return format_sec(diff_sec(finished, started))

  def format_estimated_time(self, work_list=None, params=None, concurrency=None):
    '''Format the estimated time to finish jobs.'''
    state = self.state()

    if work_list is None:
      work_list = []
    if params is None:
      params = []
    if isinstance(params, Param):
      params = [params]
    if isinstance(work_list, Work):
      work_list = [work_list] * len(params)

    assert len(work_list) == len(params)

    with self.lock:
      if params is not None:
        new_jobs = [Job(self.next_job_id + i, work_list[i], params[i]) for i in range(len(params))]
        state.pending_jobs = \
            self._sort(self._dedup(state.pending_jobs + new_jobs, keep_newer=False))
      if concurrency is not None:
        state.concurrency = concurrency

    return self.estimator.format_estimated_time(state)

  def _format_estimated_time(self):
    '''Format the estimated time to finish jobs.'''
    assert self.lock.locked() # pylint: disable=no-member
    return self.estimator.format_estimated_time(self._state)

  def _succeeded(self, job):
    '''Report a succeeded job.'''
    assert self.lock.locked() # pylint: disable=no-member
    self.hist.finished(job.param, True)
    self._state.succeeded_jobs.append(job)
    self.logger.info(termcolor.colored(
        f'Succeeded: {job} ' + \
        f'[elapsed: {self.format_elapsed_time(job)}]', 'green'))
    self.logger.info(self._format_estimated_time())

  def _failed_resource_error(self, job, exc):
    '''Report a failed job due to a resource error.'''
    assert self.lock.locked() # pylint: disable=no-member
    self.hist.finished(job.param, False)
    self._state.failed_jobs.append(job)
    self.logger.error(termcolor.colored(
        f'Failed: {job} (resource error)' + \
        f'[elapsed: {self.format_elapsed_time(job)}]' + \
        '\n' + exc, 'red'))
    self.logger.info(self._format_estimated_time())

  def _failed_exception(self, job, exc):
    '''Report a failed job with an exception.'''
    assert self.lock.locked() # pylint: disable=no-member
    self.hist.finished(job.param, False)
    self._state.failed_jobs.append(job)
    exc = '  ' + exc.rstrip().replace('\n', '\n  ')
    self.logger.error(termcolor.colored(
        f'Failed:   {job} (exception) ' + \
        f'[elapsed: {self.format_elapsed_time(job)}]' + \
        '\n' + exc, 'red'))
    self.logger.info(self._format_estimated_time())

  def _check_empty_queue(self):
    assert self.lock.locked() # pylint: disable=no-member
    if not (self._state.active_jobs or self._state.pending_jobs):
      self.logger.warning('Job queue empty')

  # pylint: disable=protected-access
  @staticmethod
  def _update_concurrency(weak_self):
    '''Update the current concurrency.'''
    alpha = 0.9

    while True:
      self = weak_self()
      if self is None:
        break

      with self.lock:
        if not self.running:
          break

        self._state.concurrency = max(1., alpha * self._state.concurrency + \
                                      (1. - alpha) * len(self._state.active_jobs))

      sleep_cond = self.sleep_cond
      del self
      with sleep_cond:
        sleep_cond.wait(1)
