'''Provides Estimator.'''

import datetime
import pytz
import termcolor
import tzlocal

__all__ = ['Estimator']

class Estimator:
  '''Estimate the remaining time.'''

  def __init__(self, hist_mgr=None):
    self.hist_mgr = hist_mgr

  def estimate_remaining_time(self, runner_state):
    '''Estimate the remaining time using Runner's state.'''

    now = datetime.datetime.utcnow()
    state = runner_state

    # Future parallelism cannot be higher than the remaining job count
    concurrency = max(1., min(runner_state.concurrency,
                              len(state.active_jobs) + len(state.pending_jobs)))

    # Obtain known duration/count and unknown count
    known_duration = 0.
    known_count = 0

    unknown_count = 0

    for job in state.succeeded_jobs + state.failed_jobs + state.active_jobs + state.pending_jobs:
      if self.hist_mgr is not None:
        hist_entry = self.hist_mgr.get(job.param)
        if hist_entry['duration'] is not None:
          known_duration += hist_entry['duration']
          known_count += 1
        else:
          unknown_count += 1
      else:
        unknown_count += 1

    # Interpolate known duration to estimate total duration
    interpolated_duration = known_duration / max(known_count, 1) * (known_count + unknown_count)

    # Obtain done jobs' duration
    known_done_duration = 0.

    for job in state.succeeded_jobs + state.failed_jobs:
      if self.hist_mgr is not None:
        hist_entry = self.hist_mgr.get(job.param)
        if hist_entry['duration'] is not None:
          known_done_duration += hist_entry['duration']

    # Obtain active jobs' duration
    known_active_duration = 0.

    for job in state.active_jobs:
      if self.hist_mgr is not None:
        hist_entry = self.hist_mgr.get(job.param)
        if hist_entry['started'] is not None:
          known_active_duration += (now - hist_entry['started']).total_seconds()

    # Estimate remaining duration
    remaining_duration = (interpolated_duration - known_done_duration - known_active_duration)
    remaining_duration = max(0., remaining_duration)

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time


  # output formatting
  @staticmethod
  def _format_sec(time):
    '''Format seconds as human-readable shorthands.'''
    time = round(time)
    output = ''
    if time >= 86400:
      value = int(time / 86400)
      time -= value * 86400
      output += '%d day%s ' % (value, 's' if value != 1 else '')
    if time >= 3600:
      value = int(time / 3600)
      time -= value * 3600
      output += '%d hour%s ' % (value, 's' if value != 1 else '')
    if time >= 60:
      value = int(time / 60)
      time -= value * 60
      output += '%d minute%s ' % (value, 's' if value != 1 else '')
    value = time
    if value > 0 or output == '':
      output += '%d second%s ' % (value, 's' if value != 1 else '')
    return output.rstrip()

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
    remaining_str = self._format_sec(remaining_time)

    current_time = datetime.datetime.utcnow()

    finish_by = current_time + datetime.timedelta(seconds=remaining_time)
    finish_by_local_str = finish_by.replace(
        tzinfo=pytz.utc).astimezone(
            tzlocal.get_localzone()).strftime('%Y-%m-%d %H:%M:%S')

    output = termcolor.colored('[', 'blue')
    output += self._format_job_count(runner_state)
    output += termcolor.colored(
        f'] remaining: {remaining_str}; ' + \
        f'finish by: {finish_by_local_str}; ' + \
        f'concurrency: {runner_state.concurrency}', 'blue')
    return output
