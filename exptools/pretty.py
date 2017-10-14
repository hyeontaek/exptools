'''Provide pretty print functions for parameters.'''

__all__ = ['pformat', 'pprint']

from pprint import PrettyPrinter

from exptools.param import get_exec_id, get_name

_PP = PrettyPrinter()

def pformat(params, verbose=False):
  '''Pretty-format parameters.'''

  output = ''

  if isinstance(params, dict):
    params = [params]

  for param in params:
    if not verbose:
      line = f'{get_exec_id(param)}  {get_name(param)}\n'
    else:
      line = f'{get_exec_id(param)}:\n  {_PP.pformat(param)}\n\n'
    output += line

  return output.strip()

def pprint(params, stream=None, *args, **kwargs):
  '''Pretty-print parameters.'''
  if stream is None:
    print(pformat(params, *args, **kwargs))
  else:
    print(pformat(params, *args, **kwargs), file=stream)
