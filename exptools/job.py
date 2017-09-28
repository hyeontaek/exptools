'''Provides Job.'''

from collections import OrderedDict
import hashlib
import json

__all__ = ['JobDef', 'OrderedDict']

class JobDef:
  '''Define a job.'''

  def hash_filter_func(self, key):
    '''Return True if the key is included in the hash.'''
    # pylint: disable=unused-argument, no-self-use
    return True

  def hash(self, param):
    '''Hash param to generate a unique ID.'''
    filtered_param = {key: param[1][key] for key in param[1] if self.hash_filter_func(key)}

    param_str = param[0] + '__' + json.dumps(filtered_param, sort_keys=True)
    return hashlib.blake2b(param_str.encode('utf-8')).hexdigest()[:16]

  def demand(self, param):
    '''Return resource requirements.'''
    # pylint: disable=unused-argument, no-self-use
    return {}

  def run(self, param):
    '''Execute the job with param and return the result (True for a success).'''
    raise NotImplementedError()
