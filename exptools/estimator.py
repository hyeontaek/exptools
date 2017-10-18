'''Provide Estimator.'''

__all__ = ['Estimator']

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

    history_data = await self.history.get_all()

    # Estimate average per-job duration
    known_param_ids = set()
    known_duration = 0.
    known_count = 0

    # Consider recent jobs first (in case some jobs have duplicate param_id)
    for job in reversed(state['started_jobs']):
      if job['param_id'] in known_param_ids:
        continue
      known_param_ids.add(job['param_id'])

      if job['started'] is None:
        started = now
      else:
        started = parse_utc(job['started'])

      if job.get('status', None) and 'progress' in job['status']:
        known_duration += diff_sec(now, started) / max(job['status']['progress'], epsilon)
        known_count += 1

    for param_id, history_entry in history_data.items():
      if param_id in known_param_ids:
        continue
      known_param_ids.add(param_id)

      if history_entry['duration'] is not None and history_entry['succeeded']:
        known_duration += history_entry['duration']
        known_count += 1

    avg_duration = known_duration / max(known_count, 1)

    # Calculate started jobs' remaining time
    remaining_duration = 0.
    for job in state['started_jobs']:
      history_entry = await self.history.get(job['param_id'])

      if job['started'] is None:
        started = now
      else:
        started = parse_utc(job['started'])

      if job.get('status', None) and 'progress' in job['status']:
        exp_duration = diff_sec(now, started) / max(job['status']['progress'], epsilon)
        remaining_duration += max(exp_duration - diff_sec(now, started), 0.)
      elif history_entry['duration'] is not None and history_entry['succeeded']:
        remaining_duration += max(history_entry['duration'] - diff_sec(now, started), 0.)
      else:
        remaining_duration += max(avg_duration - diff_sec(now, started), 0.)

    # Calculate queued jobs' remaining time
    if not oneshot:
      for job in state['queued_jobs']:
        history_entry = await self.history.get(job['param_id'])

        if history_entry['duration'] is not None and history_entry['succeeded']:
          remaining_duration += history_entry['duration']
        else:
          remaining_duration += avg_duration

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time
