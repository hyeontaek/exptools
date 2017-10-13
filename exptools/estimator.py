'''Provide Estimator.'''

__all__ = ['Estimator']

import datetime

import termcolor

from exptools.param import get_exec_id
from exptools.time import diff_sec, format_local, format_sec, utcnow, parse_utc

class Estimator:
  '''Estimate the remaining time.'''

  def __init__(self, history):
    self.history = history

  async def estimate_remaining_time(self, state):
    '''Estimate the remaining time using the queue state.'''

    now = utcnow()

    # Future parallelism cannot be higher than the remaining job count
    concurrency = max(1., min(state['concurrency'],
                              len(state['started_jobs']) + len(state['queued_jobs'])))

    history_data = await self.history.get_all()

    # Estimate average per-job duration
    known_duration = 0.
    known_count = 0

    for history_entry in history_data.values():
      if history_entry['duration'] is not None:
        known_duration += history_entry['duration']
        known_count += 1
      #elif history_entry['started'] is not None:
      #  known_duration += diff_sec(now, history_entry['started'])
      #  known_count += 1

    avg_duration = known_duration / max(known_count, 1)

    # Calculate started jobs' remaining time
    remaining_duration = 0.
    for job in state['started_jobs']:
      history_entry = await self.history.get(get_exec_id(job['param']))

      if history_entry['started'] is None:
        started = now
      else:
        started = parse_utc(history_entry['started'])

      if history_entry['duration'] is None:
        remaining_duration += max(avg_duration - diff_sec(now, started), 0.)
      else:
        remaining_duration += max(history_entry['duration'] - diff_sec(now, started), 0.)

    # Calculate queued jobs' remaining time
    for job in state['queued_jobs']:
      history_entry = await self.history.get(get_exec_id(job['param']))

      if history_entry['duration'] is None:
        remaining_duration += avg_duration
      else:
        remaining_duration += history_entry['duration']

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time

  @staticmethod
  def _format_job_count(state):
    '''Format job count.'''
    succeeded = len(filter(lambda job: job['succeeded'], state['finished_jobs']))
    failed = len(filter(lambda job: not job['succeeded'], state['finished_jobs']))
    started = len(state['started_jobs'])
    queued = len(state['queued_jobs'])

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
    output += termcolor.colored(str(started), 'yellow')
    output += termcolor.colored('/', 'blue')
    output += 'Q:'
    output += termcolor.colored(str(queued), 'cyan')
    return output

  def format_estimated_time(self, state):
    '''Format the estimated time with colors.'''
    remaining_time = self.estimate_remaining_time(state)
    remaining_str = format_sec(remaining_time)

    current_time = utcnow()

    finish_by = current_time + datetime.timedelta(seconds=remaining_time)
    finish_by_local_str = format_local(finish_by)

    output = termcolor.colored('[', 'blue')
    output += self._format_job_count(state)
    output += termcolor.colored(
        f'] remaining: {remaining_str}; ' + \
        f'finish by: {finish_by_local_str}; ' + \
        f'concurrency: {state.concurrency}', 'blue')
    return output
