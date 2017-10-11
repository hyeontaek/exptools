'''Implement the RPC server and client.'''

__all__ = ['Server', 'Client']

import json
from jsonrpc import JSONRPCResponseManager, Dispatcher
import requests
from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from exptools.history import History
from exptools.runner import Runner

class Server:
  '''Implement a RPC server that exposes History and Runner objects.'''

  def __init__(self, host, port, hist, runner):
    self.host = host
    self.port = port

    self.hist = hist
    self.runner = runner

    self._init_method_map()

  def _init_method_map(self):
    '''Initialize the method map.'''
    self.dispatcher = Dispatcher()

    for object_name, obj in [('hist', self.hist), ('runner', self.runner)]:
      for method_name in dir(obj):
        if method_name.startswith('_'):
          continue
        self.dispatcher.add_method(
            self._invoke_functor(getattr(obj, method_name)),
            f'{object_name}_{method_name}')
    #print(list(self.dispatcher.keys()))

  def serve_forever(self):
    '''Serve HTTP requests forever.'''
    run_simple(self.host, self.port, self.get_application())

  def get_application(self):
    '''Returns a WSGI application handling an RPC request.'''
    @Request.application
    def _application(request):
      #print(request.data)
      response = JSONRPCResponseManager.handle(request.data, self.dispatcher)
      #print(response.json)
      return Response(response.json, mimetype='application/json')
    return _application

  @staticmethod
  def _invoke_functor(func):
    '''Returns a function that calls func with positional and keyward arguments.'''
    def _func(args, kwargs):
      #print(args, kwargs)
      return func(*args, **kwargs)
    return _func

# pylint: disable=too-few-public-methods
class Client:
  '''Implement a RPC client that provides access to remote History and Runner objects.'''

  def __init__(self, host, port):
    self.host = host
    self.port = port

    self.session = requests.Session()

    self.hist = ObjectProxy(self, 'hist', History)
    self.runner = ObjectProxy(self, 'runner', Runner)

# pylint: disable=too-few-public-methods
class ObjectProxy:
  '''Serve as a proxy to an object.'''

  def __init__(self, client, object_name, class_):
    self.client = client
    self.object_name = object_name

    self.method_proxies = {}
    for method_name in dir(class_):
      if method_name.startswith('_'):
        continue
      self.method_proxies[method_name] = MethodProxy(self, method_name)

  def __getattribute__(self, name):
    '''Return a method proxy to the remote object.'''

    method_proxies = object.__getattribute__(self, 'method_proxies')
    if name not in method_proxies:
      return object.__getattribute__(self, name)
    return method_proxies[name]

  def __dir__(self):
    '''Return available attributes of the remote object.'''
    return list(object.__dir__(self)) + list(self.method_proxies.keys())

# pylint: disable=too-few-public-methods
class MethodProxy:
  '''Serve as a proxy to a object method.'''

  def __init__(self, object_proxy, method_name):
    self.object_proxy = object_proxy
    self.method_name = method_name

  def __call__(self, *args, **kwargs):
    '''Perform an RPC request to call a method on the server.'''

    url = f'http://{self.object_proxy.client.host}:{self.object_proxy.client.port}/jsonrpc'
    headers = {'content-type': 'application/json'}

    payload = {
        'jsonrpc': '2.0',
        'id': 0,
        'method': f'{self.object_proxy.object_name}_{self.method_name}',
        'params': [args, kwargs],
    }
    #print(payload)
    response = self.object_proxy.client.session.post(
        url, data=json.dumps(payload), headers=headers).json()
    return response
