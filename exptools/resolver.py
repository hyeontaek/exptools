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
    # Load yaql lazily for fast startup
    import yaql
    return yaql.eval(f'$.where({filter_expr})', data=params)

  @rpc_export_function
  async def filter_pandas_query(self, params, filter_expr):
    """Filter parameters using a pandas query expression."""
    # Load pandas lazily for fast startup
    import pandas
    params_df = pandas.DataFrame(params, index=map(get_param_id, params))
    selected_df = params_df.query(filter_expr, local_dict={}, global_dict={})
    selected_param_ids = set(selected_df.index)
    return [param for param in params if get_param_id(param) in selected_param_ids]

  @rpc_export_function
  async def filter_asteval(self, params, filter_expr):
    """Filter parameters using an asteval expression."""
    # Load asteval lazily for fast startup
    import asteval

    aeval = asteval.Interpreter(no_print=True)

    new_params = []
    for param in params:
      # Note that by exposing param without copying,
      # the user-supplied filter_expr may modify the content of param.
      # However, we allow it because the modification is only visible via get_param(),
      # which is used by dump commands and no other commands such as add or enqueue.
      aeval.symtable['param'] = aeval.symtable['p'] = param

      result = aeval.eval(filter_expr, show_errors=False)
      if result:
        new_params.append(param)
    return new_params

  @rpc_export_function
  async def filter_omit(self, params, types):
    """Omit parameters of specified types"""
    valid_types = [
      'succeeded', 'failed', 'finished', 'started', 'queued',
      'identical', 'duplicate', 'has_output']
    for type_ in types:
      assert type_ in valid_types

    hash_ids = [get_hash_id(param) for param in params]

    if 'succeeded' in types:
      params = [param for param in params if param['_']['succeeded'] != True]

    if 'failed' in types:
      params = [param for param in params if param['_']['succeeded'] != False]

    if 'finished' in types:
      params = [param for param in params if param['_']['finished'] is None]

    if 'started' in types:
      started_jobs = await self.queue.jobs(await self.queue.job_ids(['started']))
      started_job_hash_ids = set([get_hash_id(job['param']) for job in started_jobs])

      new_params = []
      for param, hash_id in zip(params, hash_ids):
        if hash_id in started_job_hash_ids:
          continue
        new_params.append(param)
      params = new_params

    if 'queued' in types:
      queued_jobs = await self.queue.jobs(await self.queue.job_ids(['queued']))
      queued_job_hash_ids = set([get_hash_id(job['param']) for job in queued_jobs])

      new_params = []
      for param, hash_id in zip(params, hash_ids):
        if hash_id in queued_job_hash_ids:
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

    def _compare_func(obj1, obj2):
      # None is smaller than any value
      if obj1 is None:
        if obj2 is not None:
          return -1
        return 0
      else:
        if obj2 is None:
          return 1

      # str is compared directly
      if isinstance(obj1, str):  # assumed: isinstance(y, str) == True
        if obj1 < obj2:
          return -1
        elif obj1 == obj2:
          return 0
        return 1

      # others are assumed to be numeric
      return obj1 - obj2

    # Adapted from functools.cmp_to_key()
    class _Key:
      def __init__(self, obj):
        for key in key_path:
          if isinstance(obj, dict) and key in obj:
            obj = obj[key]
          else:
            obj = None
        self.obj = obj
      def __lt__(self, other):
        return _compare_func(self.obj, other.obj) < 0
      def __gt__(self, other):
        return _compare_func(self.obj, other.obj) > 0
      def __eq__(self, other):
        return _compare_func(self.obj, other.obj) == 0
      def __le__(self, other):
        return _compare_func(self.obj, other.obj) <= 0
      def __ge__(self, other):
        return _compare_func(self.obj, other.obj) >= 0
      def __ne__(self, other):
        return _compare_func(self.obj, other.obj) != 0

    return list(sorted(params, key=_Key, reverse=reverse))

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
      elif operation == 'asteval':
        data = await self.filter_asteval(data, *args, **kwargs)
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
