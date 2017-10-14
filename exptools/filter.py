'''Implement the Filter class.'''

from exptools.rpc_helper import rpc_export_function

# pylint: disable=too-few-public-methods
class Filter:
  '''Filter parameters.'''

  def __init__(self, loop):
    self.loop = loop

  @rpc_export_function
  async def filter(self, filter_expr, params):
    '''Filter parameters using a YAQL expression,'''
    # load yaql lazily for fast client startup
    import yaql
    return yaql.eval(filter_expr, data=params)
