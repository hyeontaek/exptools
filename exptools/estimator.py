'''Provide Estimator.'''

__all__ = ['Estimator']

import datetime
import termcolor
from exptools.time import diff_sec, format_local, format_sec, utcnow

class Estimator:
  '''Estimate the remaining time.'''

  def __init__(self, hist=None):
    self.hist = hist

  def estimate_remaining_time(self, runner_state):
    '''Estimate the remaining time using Runner's state.'''

    now = utcnow()
    state = runner_state

    # Future parallelism cannot be higher than the remaining job count
    concurrency = max(1., min(runner_state.concurrency,
                              len(state.active_jobs) + len(state.pending_jobs)))

    hist_data = self.hist.get_all()

    # Estimate average per-job duration
    known_duration = 0.
    known_count = 0

    for hist_entry in hist_data.values():
      if hist_entry['duration'] is not None:
        known_duration += hist_entry['duration']
        known_count += 1
      elif hist_entry['started'] is not None:
        known_duration += diff_sec(now, hist_entry['started'])
        known_count += 1

    avg_duration = known_duration / max(known_count, 1)

    # Calculate active jobs' remaining time
    remaining_duration = 0.
    for job in state.active_jobs:
      hist_entry = self.hist.get(job.param)

      if hist_entry['started'] is None:
        started = now
      else:
        started = hist_entry['started']

      if hist_entry['duration'] is None:
        remaining_duration += max(avg_duration - diff_sec(now, started), 0.)
      else:
        remaining_duration += max(hist_entry['duration'] - diff_sec(now, started), 0.)

    # Calculate pending jobs' remaining time
    for job in state.pending_jobs:
      hist_entry = self.hist.get(job.param)

      if hist_entry['duration'] is None:
        remaining_duration += avg_duration
      else:
        remaining_duration += hist_entry['duration']

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time

  @staticmethod
  def _format_job_count(runner_state):
    '''Format job count.'''
    succeeded = len(runner_state.succeeded_jobs)
    failed = len(runner_state.failed_jobs)
    active = len(runner_state.active_jobs)
    pending = len(runner_state.pending_jobs)

    output = 'S:'
    output += termcolor.colored(str(succeeded), 'green')
    output += termcolor.colored('/', 'blue')
    output += 'F:'
    if failed == 0:
      output += termcolor.colored(str(failed), 'green')
      output += termcolor.colored('/', 'blue')
    else:
      output += termcolor.colored(str(failed), 'red')
      output += termcolor.colored('/', 'blue')
    output += 'A:'
    output += termcolor.colored(str(active), 'yellow')
    output += termcolor.colored('/', 'blue')
    output += 'P:'
    output += termcolor.colored(str(pending), 'cyan')
    return output

  def format_estimated_time(self, runner_state):
    '''Format the estimated time with colors.'''
    remaining_time = self.estimate_remaining_time(runner_state)
    remaining_str = format_sec(remaining_time)

    current_time = utcnow()

    finish_by = current_time + datetime.timedelta(seconds=remaining_time)
    finish_by_local_str = format_local(finish_by)

    output = termcolor.colored('[', 'blue')
    output += self._format_job_count(runner_state)
    output += termcolor.colored(
        f'] remaining: {remaining_str}; ' + \
        f'finish by: {finish_by_local_str}; ' + \
        f'concurrency: {runner_state.concurrency}', 'blue')
    return output
