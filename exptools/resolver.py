"""Implement the Resolver class."""

import re

from exptools.param import get_param_id, get_hash_id, get_name, make_unique_id
from exptools.rpc_helper import rpc_export_function


class Resolver:
  """Filter parameters."""

  def __init__(self, registry, history, queue, runner, loop):
    self.registry = registry
    self.history = history
    self.queue = queue
    self.runner = runner
    self.loop = loop

  @rpc_export_function
  async def select(self, ids):
    """Select parameters in the registry."""
    params = []
    for id_ in ids:
      if id_ == 'all':
        param_ids = await self.registry.param_ids()
        params.extend(await self.registry.params(param_ids))

      elif id_.startswith('p-'):
        params.append(await self.registry.param(param_id=id_))

      elif id_.startswith('h-'):
        param_ids = await self.registry.param_ids_by_hash_id(hash_id=id_)
        params.extend(await self.registry.params(param_ids))

      elif id_.startswith('j-'):
        job = await self.queue.job(job_id=id_)
        params.append(job['param'])

      else:
        param_ids = await self.registry.paramset(paramset=id_)
        params.extend(await self.registry.params(param_ids))

    return params

  @rpc_export_function
  async def filter_augment(self, params):
    """Augment parameters with their history."""
    hash_ids = [get_hash_id(param) for param in params]

    history_list = await self.history.history_list(hash_ids)

    for param, history in zip(params, history_list):
      param['_'].update(history)

    return params

  @rpc_export_function
  async def filter_grep(self, params, filter_expr,
                        ignore_case=False, invert_match=False, line_regexp=False):
    """Filter parameters using a regular expression on parameter names."""

    flags = 0
    if ignore_case:
      flags |= re.I

    pat = re.compile(filter_expr, flags)

    if not line_regexp:
      pat_func = pat.search
    else:
      pat_func = pat.fullmatch

    selected_params = []

    if not invert_match:
      for param in params:
        mat = pat_func(get_name(param))
        if mat is not None:
          selected_params.append(param)
    else:
      for param in params:
        mat = pat_func(get_name(param))
        if mat is None:
          selected_params.append(param)

    return selected_params

  @rpc_export_function
  async def filter_yaql(self, params, filter_expr):
    """Filter parameters using a YAQL expression."""
    # load yaql lazily for fast startup
    import yaql
    return yaql.eval(f'$.where({filter_expr})', data=params)

  @rpc_export_function
  async def filter_pandas_query(self, params, filter_expr):
    """Filter parameters using a pandas query expression."""
    # load pandas lazily for fast startup
    import pandas
    df = pandas.DataFrame(params, index=map(get_param_id, params))
    selected_df = df.query(filter_expr)
    selected_param_ids = set(selected_df.index)
    return [param for param in params if get_param_id(param) in selected_param_ids]

  @rpc_export_function
  async def filter_omit(self, params, types):
    """Omit parameters of specified types"""
    valid_types = [
      'succeeded', 'failed', 'finished', 'started', 'queued',
      'identical', 'duplicate', 'has_output']
    for type_ in types:
      assert type_ in valid_types

    hash_ids = [get_hash_id(param) for param in params]

    if 'succeeded' in types or 'failed' in types or 'finished' in types:
      history_list = await self.history.history_list(hash_ids)

      new_params = []
      for param, history in zip(params, history_list):
        if 'succeeded' in types and history['succeeded']:
          continue
        if 'failed' in types and not history['succeeded']:
          continue
        if 'finished' in types and history['finished'] is not None:
          continue
        new_params.append(param)
      params = new_params

    if 'started' in types or 'queued' in types:
      started_jobs = await self.queue.jobs(await self.queue.job_ids(['started']))
      queued_jobs = await self.queue.jobs(await self.queue.job_ids(['queued']))

      new_params = []
      for param, hash_id in zip(params, hash_ids):
        if 'started' in types:
          matching_jobs = [job for job in started_jobs if get_hash_id(job['param']) == hash_id]
          if matching_jobs:
            continue
        if 'queued' in types:
          matching_jobs = [job for job in queued_jobs if get_hash_id(job['param']) == hash_id]
          if matching_jobs:
            continue
        new_params.append(param)

      params = new_params

    if 'identical' in types:
      seen_unique_ids = set()
      new_params = []
      for param in params:
        unique_id = make_unique_id(param)
        if unique_id not in seen_unique_ids:
          seen_unique_ids.add(unique_id)
          new_params.append(param)
      params = new_params

    if 'duplicate' in types:
      seen_hash_ids = set()
      new_params = []
      for param in params:
        hash_id = get_hash_id(param)
        if hash_id not in seen_hash_ids:
          seen_hash_ids.add(hash_id)
          new_params.append(param)
      params = new_params

    if 'has_output' in types:
      output_hash_ids = await self.runner.hash_ids()
      new_params = []
      for param in params:
        hash_id = get_hash_id(param)
        if hash_id not in output_hash_ids:
          new_params.append(param)
      params = new_params

    return params

  @rpc_export_function
  async def filter_only(self, params, types):
    """Only select parameters of specified types"""
    omitted_params = await self.filter_omit(params, types)
    omitted_param_ids = set([get_param_id(param) for param in omitted_params])

    new_params = []
    for param in params:
      if get_param_id(param) not in omitted_param_ids:
        new_params.append(param)
    return new_params

  @rpc_export_function
  async def filter_sort(self, params, sort_key, reverse=False):
    """Sort parameters by given key"""
    key_path = sort_key.split('.')

    def _key_func(param):
      current_obj = param
      for key in key_path:
        if current_obj and key in current_obj:
          current_obj = current_obj[key]
        else:
          current_obj = None
      if current_obj is None:
        return ''
      return str(current_obj)

    return list(sorted(params, key=_key_func, reverse=reverse))

  @rpc_export_function
  async def get_params(self, params):
    """Return parameters."""
    return params

  @rpc_export_function
  async def get_params_with_history(self, params):
    """Return parameters with history."""
    return params

  @rpc_export_function
  async def get_param_ids(self, params):
    """Return parameter IDs."""
    return [get_param_id(param) for param in params]

  @rpc_export_function
  async def get_hash_ids(self, params):
    """Return hash IDs."""
    return [get_hash_id(param) for param in params]

  @rpc_export_function
  async def filter_params(self, chain):
    """Filter parameters using an operation chain."""
    data = None
    for i, (operation, args, kwargs) in enumerate(chain):
      # Source
      if operation == 'select':
        if i != 0:
          raise RuntimeError('Parameters already loaded')
        data = await self.select(*args, **kwargs)

      # Filter
      elif operation == 'augment':
        data = await self.filter_augment(data, *args, **kwargs)
      elif operation == 'grep':
        data = await self.filter_grep(data, *args, **kwargs)
      elif operation == 'yaql':
        data = await self.filter_yaql(data, *args, **kwargs)
      elif operation == 'pandas_query':
        data = await self.filter_pandas_query(data, *args, **kwargs)
      elif operation == 'omit':
        data = await self.filter_omit(data, *args, **kwargs)
      elif operation == 'only':
        data = await self.filter_only(data, *args, **kwargs)
      elif operation == 'sort':
        data = await self.filter_sort(data, *args, **kwargs)

      # Sink
      elif operation == 'get_params':
        if i != len(chain) - 1:
          raise RuntimeError(f'Excessive operation after {operation}')
        return await self.get_params(data, *args, **kwargs)
      elif operation == 'get_param_ids':
        if i != len(chain) - 1:
          raise RuntimeError(f'Excessive operation after {operation}')
        return await self.get_param_ids(data, *args, **kwargs)
      elif operation == 'get_hash_ids':
        if i != len(chain) - 1:
          raise RuntimeError(f'Excessive operation after {operation}')
        return await self.get_hash_ids(data, *args, **kwargs)

      else:
        assert False, f'Unrecognized operation: {operation}'

    assert False, 'The operation chain must end with a get_* operation'
