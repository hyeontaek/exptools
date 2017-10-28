'''Implement the Resolver class.'''

import re

from exptools.param import get_param_id, get_hash_id, get_name
from exptools.rpc_helper import rpc_export_function

class Resolver:
  '''Filter parameters.'''

  def __init__(self, registry, history, queue, loop):
    self.registry = registry
    self.history = history
    self.queue = queue
    self.loop = loop

  async def _augment(self, params):
    hash_ids = [get_hash_id(param) for param in params]

    history_list = await self.history.history_list(hash_ids)

    for param, history in zip(params, history_list):
      param['_'].update(history)

  @rpc_export_function
  async def select_all(self):
    '''Select all parameters in the registry.'''
    param_ids = await self.registry.param_ids()
    params = await self.registry.params(param_ids)
    await self._augment(params)
    return params

  @rpc_export_function
  async def select_paramset(self, paramset):
    '''Select parameters in the parameter set.'''
    param_ids = await self.registry.paramset(paramset)
    params = await self.registry.params(param_ids)
    await self._augment(params)
    return params

  @rpc_export_function
  async def select_ids(self, ids):
    '''Select parameters indicated by IDs.'''

    param_ids = []
    for id_ in ids:
      if id_.startswith('p-'):
        param_ids.append(id_)
      elif id_.startswith('h-'):
        param_ids.extend(self.registry.param_ids_by_hash_id(hash_id=id_))
      elif id_.startswith('j-'):
        job = await self.queue.job(job_id=id_)
        param_ids.append(get_param_id(job['param']))
      else:
        raise RuntimeError(f'Unrecognized ID: {id_}')

    params = await self.registry.params(param_ids)
    await self._augment(params)
    return params

  @rpc_export_function
  async def filter_grep(self, params, filter_expr,
                        ignore_case=False, invert_match=False, line_regexp=False):
    '''Filter parameters using a regular expression on parameter names.'''

    flags = 0
    if ignore_case:
      flags |= re.I

    pat = re.compile(filter_expr, flags)

    if not line_regexp:
      pat_punc = pat.search
    else:
      pat_punc = pat.fullmatch

    selected_params = []

    if not invert_match:
      for param in params:
        mat = pat_punc(get_name(param))
        if mat is not None:
          selected_params.append(param)
    else:
      for param in params:
        mat = pat_punc(get_name(param))
        if mat is None:
          selected_params.append(param)

    return selected_params

  @rpc_export_function
  async def filter_yaql(self, params, filter_expr):
    '''Filter parameters using a YAQL expression.'''
    # load yaql lazily for fast startup
    import yaql
    return yaql.eval(filter_expr, data=params)

  @rpc_export_function
  async def filter_omit(self, params, types):
    '''Omit parameters of specified types'''
    valid_types = ['succeeded', 'failed', 'finished', 'started', 'queued', 'duplicate']
    for type_ in types:
      assert type_ in valid_types

    hash_ids = [get_hash_id(param) for param in params]

    if 'succeeded' in types or 'failed' in types:
      history_list = await self.history.history_list(hash_ids)

      new_params = []
      for param, history in zip(params, history_list):
        if 'succeeded' in types and history['succeeded'] is not None:
          continue
        if 'failed' in types and history['succeeded'] is not None:
          continue
        new_params.append(param)
      params = new_params

    if 'finished' in types or 'started' in types or 'queued' in types:
      jobs = await self.queue.jobs(await self.queue.job_ids())

      new_params = []
      for param, hash_id in zip(params, hash_ids):
        matching_jobs = [job for job in jobs if get_hash_id(job['param']) == hash_id]

        for job in matching_jobs:
          if 'finished' in types and job['finished'] is not None:
            break
          if 'started' in types and job['started'] is not None:
            break
          if 'queued' in types and job['queued'] is not None:
            break
        else:
          # No omit condition met
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

    return params

  @rpc_export_function
  async def filter_only(self, params, types):
    '''Only select parameters of specified types'''
    omitted_params = await self.filter_omit(params, types)
    omitted_param_ids = set([get_param_id(param) for param in omitted_params])

    new_params = []
    for param in params:
      if get_param_id(param) not in omitted_param_ids:
        new_params.append(param)
    return new_params

  @rpc_export_function
  async def filter_sort(self, params, sort_key, reverse=False):
    '''Sort parameters by given key'''
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
    '''Return parameters.'''
    return params

  @rpc_export_function
  async def get_param_ids(self, params):
    '''Return parameter IDs.'''
    return [get_param_id(param) for param in params]

  @rpc_export_function
  async def get_hash_ids(self, params):
    '''Return hash IDs.'''
    return [get_hash_id(param) for param in params]

  @rpc_export_function
  async def filter_params(self, chain):
    '''Filter parameters using an operation chain.'''
    data = None
    for i, (operation, args, kwargs) in enumerate(chain):
      # Source
      if operation == 'all':
        if i != 0:
          raise RuntimeError('Parameters already loaded')
        data = await self.select_all(*args, **kwargs)
      elif operation == 'paramset':
        if i != 0:
          raise RuntimeError('Parameters already loaded')
        data = await self.select_paramset(*args, **kwargs)
      elif operation == 'ids':
        if i != 0:
          raise RuntimeError('Parameters already loaded')
        data = await self.select_ids(*args, **kwargs)

      # Filter
      elif operation == 'grep':
        data = await self.filter_grep(data, *args, **kwargs)
      elif operation == 'yaql':
        data = await self.filter_yaql(data, *args, **kwargs)
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
