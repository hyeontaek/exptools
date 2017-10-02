'''Provide the Param class.'''

__all__ = ['Param']

from collections import OrderedDict
import hashlib
import json

class Param(OrderedDict):
  '''Represent a parameter.'''

  _hash_func = hashlib.blake2b # pylint: disable=no-member
  _empty_dict = {}

  @property
  def name(self):
    '''Return the name.'''
    return self.get('_name', super().__str__())

  @property
  def demand(self):
    '''Return the demand.'''
    return self.get('_demand', self._empty_dict)

  @property
  def priority(self):
    '''Return the priority.'''
    return self.get('_priority', 0)

  @property
  def param_id(self):
    '''Return the parameter ID of a parameter.'''
    param_str = json.dumps(self, sort_keys=True)
    return self._hash_func(param_str.encode('utf-8')).hexdigest()[:16]

  @property
  def exec_id(self):
    '''Return the execution ID of a parameter.'''
    filtered_param = {key: value for key, value in self.items() if not key.startswith('_')}
    param_str = json.dumps(filtered_param, sort_keys=True)
    return self._hash_func(param_str.encode('utf-8')).hexdigest()[:16]

  def with_new_priority(self, new_priority):
    '''Return a new parameter with a new priority.'''
    data = OrderedDict(self)
    data['_priority'] = new_priority
    return Param(data)

  def with_adjusted_priority(self, priority_delta):
    '''Return a new parameter with an adjusted priority.'''
    data = OrderedDict(self)
    data['_priority'] = self.priority + priority_delta
    return Param(data)

  def __str__(self):
    '''Format a parameter.'''
    return self.name
