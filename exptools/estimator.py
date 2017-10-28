'''Provide Estimator.'''

__all__ = ['Estimator']

from exptools.param import get_hash_id
from exptools.time import diff_sec, utcnow, parse_utc

# pylint: disable=too-few-public-methods
class Estimator:
  '''Estimate the remaining time.'''

  def __init__(self, history):
    self.history = history

  async def estimate_remaining_time(self, state, oneshot):
    '''Estimate the remaining time using the queue state.'''

    now = utcnow()

    epsilon = 0.001  # Potential understimation until the progress reaches 0.1%

    # Future parallelism cannot be higher than the remaining job count
    concurrency = max(1., min(state['concurrency'],
                              len(state['started_jobs']) + len(state['queued_jobs'])))

    hash_ids = await self.history.hash_ids()
    history_list = await self.history.history_list(hash_ids)
    history_map = dict(zip(hash_ids, history_list))

    # Estimate average per-job duration
    known_hash_ids = set()
    known_duration = 0.
    known_count = 0

    # Consider recent jobs first (in case some jobs have duplicate hash_id)
    for job in reversed(state['started_jobs']):
      hash_id = get_hash_id(job['param'])
      if hash_id in known_hash_ids:
        continue
      known_hash_ids.add(hash_id)

      if job['started'] is None:
        started = now
      else:
        started = parse_utc(job['started'])

      if job.get('status', None) and job['status'].get('progress') >= epsilon:
        known_duration += diff_sec(now, started) / job['status']['progress']
        known_count += 1

    for hash_id, history in history_map.items():
      if hash_id in known_hash_ids:
        continue
      known_hash_ids.add(hash_id)

      if history['duration'] is not None:
        known_duration += history['duration']
        known_count += 1

    avg_duration = known_duration / max(known_count, 1)

    remaining_time_map = {}

    for job in state['finished_jobs']:
      remaining_time_map[job['job_id']] = 0.

    # Calculate started jobs' remaining time
    remaining_duration = 0.
    for job in state['started_jobs']:
      hash_id = get_hash_id(job['param'])
      history = history_map.get(hash_id, None)

      if job['started'] is None:
        started = now
      else:
        started = parse_utc(job['started'])

      if job.get('status', None) and job['status'].get('progress') >= epsilon:
        exp_duration = diff_sec(now, started) / job['status']['progress']
        remaining_duration += max(exp_duration - diff_sec(now, started), 0.)
      elif history and history['duration'] is not None:
        remaining_duration += max(history['duration'] - diff_sec(now, started), 0.)
      else:
        remaining_duration += max(avg_duration - diff_sec(now, started), 0.)

      # Take into account concurrency
      remaining_time_map[job['job_id']] = remaining_duration / concurrency

    # Calculate queued jobs' remaining time
    if not oneshot:
      for job in state['queued_jobs']:
        hash_id = get_hash_id(job['param'])
        history = history_map.get(hash_id, None)

        if history and history['duration'] is not None:
          remaining_duration += history['duration']
        else:
          remaining_duration += avg_duration

        # Take into account concurrency
        remaining_time_map[job['job_id']] = remaining_duration / concurrency
    else:
      for job in state['queued_jobs']:
        remaining_time_map[job['job_id']] = remaining_duration / concurrency

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time, remaining_time_map
