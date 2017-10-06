'''Provide pretty print functions for parameters.'''

__all__ = ['pformat', 'pprint']

from pprint import PrettyPrinter
from exptools.param import Param

pp = PrettyPrinter()

def pformat(params, verbose=False):
  '''Pretty-format parameters.'''

  org_params = params

  # single parameter
  if isinstance(params, Param):
    param = params
    if not verbose:
      line = f'{param.exec_id} {param.name}'
    else:
      line = f'{param.exec_id}:\n  {pp.pformat(param)}'
    return line

  # parameter list
  params = list(params)
  if isinstance(params, list):
    if not verbose:
      lines = [f'{param.exec_id} {param.name}' for param in params]
    else:
      lines = [f'{param.exec_id}:\n  {pp.pformat(param)}' for param in params]
    return '\n'.join(lines)

  # fall back
  return pp.pformat(org_params)
  
def pprint(params, stream=None, *args, **kwargs):
  '''Pretty-print parameters.'''
  if stream is None:
    print(pformat(params, *args, **kwargs))
  else:
    print(pformat(params, *args, **kwargs), file=stream)
