"""Provide Estimator."""

__all__ = ['Estimator']

from exptools.param import get_hash_id
from exptools.time import diff_sec, utcnow, parse_utc


class Estimator:
  """Estimate the remaining time."""

  def __init__(self, registry, history):
    self.registry = registry
    self.history = history

  @staticmethod
  def _extract_fvec(fid_mapping, param):
    """Extract a feature vector from a parameter."""
    import numpy as np

    fvec = np.zeros([len(fid_mapping)], dtype='b')
    for key, value in param.items():
      if key.startswith('_'):
        continue
      if isinstance(value, dict):
        continue

      value_hash = hash(key + '___' + str(value))
      if value_hash not in fid_mapping:
        continue

      fvec[fid_mapping[value_hash]] = 1

    return fvec

  async def _make_fid_mapping(self, history_map, hash_ids):
    """Make a value to feature index mapping,
    and make a feature vector list for existig parameters."""
    param_ids_list = await self.registry.param_ids_by_hash_ids(hash_ids)
    param_ids = [param_ids[0] for param_ids in param_ids_list if param_ids]
    params = await self.registry.params(param_ids)

    fid_mapping = {}
    fvec_map = {}

    succeeded_params = []
    for param in params:
      hash_id = get_hash_id(param)

      if hash_id not in history_map:
        assert False
      if not history_map[hash_id]['succeeded']:
        assert False
      if history_map[hash_id]['duration'] is None:
        assert False

      for key, value in param.items():
        if key.startswith('_'):
          continue
        if isinstance(value, dict):
          continue

        value_hash = hash(key + '___' + str(value))
        if value_hash not in fid_mapping:
          fid_mapping[value_hash] = len(fid_mapping)

      succeeded_params.append((hash_id, param))

    for hash_id, param in succeeded_params:
      fvec_map[hash_id] = self._extract_fvec(fid_mapping, param)

    return fid_mapping, fvec_map

  async def _find_closest(self, fid_mapping, fvec_map, param):
    """Return hash IDs of parameters whose feature vector is
    close to that of the queried parameter."""
    import numpy as np

    lookup_feature = self._extract_fvec(fid_mapping, param)

    min_distance = len(fid_mapping) + 1
    closest = []
    for hash_id, feature in fvec_map.items():
      distance = np.count_nonzero(feature != lookup_feature)

      if min_distance > distance:
        min_distance = distance
        closest = [hash_id]
      elif min_distance == distance:
        closest.append(hash_id)

    return closest

  async def _find_closest_duration(self, history_map, fid_mapping, fvec_map, param):
    """Calculate the duration of jobs whose parameter are close to the queried parameters."""
    closest = await self._find_closest(fid_mapping, fvec_map, param)
    if not closest:
      return None
    return sum([history_map[hash_id]['duration'] for hash_id in closest]) / len(closest)

  async def estimate_remaining_time(self, state, oneshot, use_similar):
    """Estimate the remaining time using the queue state."""

    now = utcnow()

    epsilon = 0.001  # Potential underestimation until the progress reaches 0.1%

    # Future parallelism cannot be higher than the remaining job count
    concurrency = max(1., min(state['concurrency'],
                              len(state['started_jobs']) + len(state['queued_jobs'])))

    hash_ids = await self.history.hash_ids()
    history_list = await self.history.history_list(hash_ids)
    history_map = dict(zip(hash_ids, history_list))

    if use_similar:
      fid_mapping, fvec_map = await self._make_fid_mapping(history_map, hash_ids)

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
        if use_similar:
          exp_duration = (
            await self._find_closest_duration(history_map, fid_mapping, fvec_map, job['param']))
          if exp_duration is None:
            exp_duration = avg_duration
        else:
          exp_duration = avg_duration

        remaining_duration += max(exp_duration - diff_sec(now, started), 0.)

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
          if use_similar:
            exp_duration = (
              await self._find_closest_duration(history_map, fid_mapping, fvec_map, job['param']))
            if exp_duration is None:
              exp_duration = avg_duration
          else:
            exp_duration = avg_duration
          remaining_duration += exp_duration

        # Take into account concurrency
        remaining_time_map[job['job_id']] = remaining_duration / concurrency
    else:
      for job in state['queued_jobs']:
        remaining_time_map[job['job_id']] = remaining_duration / concurrency

    # Take into account concurrency
    remaining_time = remaining_duration / concurrency

    return remaining_time, remaining_time_map
