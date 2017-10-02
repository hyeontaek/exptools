'''Example parameters.'''

from exptools.param import Param

def _enum_params():
  '''Enumerate params.'''
  for i in range(4):
    yield Param({
      'i': i,
      '_name': f'i={i}',
      '_demand': {'ps': 1, 'worker': 2},
      })

def enum_params():
  '''Enumerate params as a list.'''
  return list(_enum_params())
