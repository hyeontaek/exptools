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
    if verbose:
      param = list(param.items())
    return pp.pformat(param)

  # parameter list
  params = list(params)
  if isinstance(params, list):
    if verbose:
      params = [list(param.items()) for param in params]
    return pp.pformat(params)

  # fall back
  return pp.pformat(org_params)
  
def pprint(params, stream=None, *args, **kwargs):
  '''Pretty-print parameters.'''
  if stream is None:
    print(pformat(params, *args, **kwargs))
  else:
    print(pformat(params, *args, **kwargs), file=stream)
