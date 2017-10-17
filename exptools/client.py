'''Implement the RPC client.'''

__all__ = ['Client']

import asyncio
import base64
import hashlib
import hmac
import json

import websockets

from exptools.estimator import Estimator
from exptools.filter import Filter
from exptools.history import History
from exptools.queue import Queue
from exptools.runner import Runner
from exptools.scheduler import Scheduler

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-instance-attributes
class Client:
  '''Implement a RPC client that provides access to remote objects.'''

  def __init__(self, host, port, secret, loop=asyncio.get_event_loop()):
    self.host = host
    self.port = port
    self.secret = secret
    self.loop = loop

    self.history = ObjectProxy(self, 'history', History)
    self.filter = ObjectProxy(self, 'filter', Filter)
    self.queue = ObjectProxy(self, 'queue', Queue)
    self.runner = ObjectProxy(self, 'runner', Runner)
    self.scheduler = ObjectProxy(self, 'scheduler', Scheduler)

    self.estimator = Estimator(self.history)

    self.next_id = 0
    self._connect()

  def _connect(self):
    '''Connect to the server.'''
    connect_task = asyncio.ensure_future(
        websockets.connect(f'ws://{self.host}:{self.port}'), loop=self.loop)
    self.loop.run_until_complete(connect_task)
    self.websocket = connect_task.result()

    auth = asyncio.ensure_future(self._authenticate(self.websocket), loop=self.loop)
    self.loop.run_until_complete(auth)
    assert auth.result(), 'Authentication failed'

  async def _authenticate(self, websocket):
    '''Perform authentication.'''
    token = await websocket.recv()

    secret = self.secret.encode('ascii')
    signature = base64.b64encode(hmac.new(secret, token, digestmod=hashlib.sha256).digest())

    await websocket.send(signature)

    response = await websocket.recv()
    if response != b'Authenticated':
      return False

    return True

  def get_next_id(self):
    '''Return the next request ID.'''
    next_id = self.next_id
    self.next_id += 1
    return next_id

# pylint: disable=too-few-public-methods
class ObjectProxy:
  '''Serve as a proxy to an object.'''

  def __init__(self, client, object_name, class_):
    self.client = client
    self.object_name = object_name
    self.class_ = class_

    for method_name in dir(class_):
      method = getattr(class_, method_name)
      if not hasattr(method, 'rpc_export'):
        continue
      if getattr(method, 'rpc_export') == 'function':
        setattr(self, method_name, FunctionProxy(self, method_name))
      elif getattr(method, 'rpc_export') == 'generator':
        setattr(self, method_name, GeneratorProxy(self, method_name))
      else:
        assert False

# pylint: disable=too-few-public-methods
class FunctionProxy:
  '''Serve as a proxy to a function.'''

  def __init__(self, object_proxy, method_name):
    self.client = object_proxy.client
    self.method = f'function:{object_proxy.object_name}.{method_name}'

  async def __call__(self, *args, **kwargs):
    '''Perform an RPC request to call a method on the server.'''

    request = {
        'id': self.client.get_next_id(),
        'method': self.method,
        'args': args,
        'kwargs': kwargs,
    }
    await self.client.websocket.send(json.dumps(request))

    while True:
      response = json.loads(await self.client.websocket.recv())
      if response == 'ping':
        continue

      break
    assert response['id'] == request['id']

    if 'result' in response:
      return response['result']
    else:
      raise RuntimeError(response['error'], response['data'])

# pylint: disable=too-few-public-methods
class GeneratorProxy:
  '''Serve as a proxy to a generator.'''

  def __init__(self, object_proxy, method_name):
    self.client = object_proxy.client
    self.method = f'generator:{object_proxy.object_name}.{method_name}'

  async def __call__(self, *args, **kwargs):
    '''Perform an RPC request to call a method on the server.'''

    request = {
        'id': self.client.get_next_id(),
        'method': self.method,
        'args': args,
        'kwargs': kwargs,
    }
    await self.client.websocket.send(json.dumps(request))

    while True:
      response = json.loads(await self.client.websocket.recv())
      if response == 'ping':
        continue

      assert response['id'] == request['id']

      if 'result' in response:
        yield response['result']
      elif response['error'] == 'StopAsyncIteration':
        return
      else:
        raise RuntimeError(response['error'], response['data'])
