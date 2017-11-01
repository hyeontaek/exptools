"""Implement the RPC client."""

__all__ = ['Client']

import asyncio
import base64
import hashlib
import hmac
import json
import math
import sys

import websockets

from exptools.history import History
from exptools.queue import Queue
from exptools.registry import Registry
from exptools.resolver import Resolver
from exptools.runner import Runner
from exptools.scheduler import Scheduler


class Client:
  """Implement a RPC client that provides access to remote objects."""

  def __init__(self, host, port, secret, loop=asyncio.get_event_loop()):
    self.host = host
    self.port = port
    self.secret = secret
    self.loop = loop

    self.registry = ObjectProxy(self, 'registry', Registry)
    self.history = ObjectProxy(self, 'history', History)
    self.queue = ObjectProxy(self, 'queue', Queue)
    self.resolver = ObjectProxy(self, 'resolver', Resolver)
    self.scheduler = ObjectProxy(self, 'scheduler', Scheduler)
    self.runner = ObjectProxy(self, 'runner', Runner)

    self.max_size = 1048576
    self.max_chunk_size = self.max_size // 2

    self.next_id = 0
    self.websocket = None

  async def connect(self):
    """Connect to the server."""
    self.websocket = (
      await websockets.connect(f'ws://{self.host}:{self.port}', max_size=self.max_size))

    authed = await self._authenticate(self.websocket)
    assert authed, 'Authentication failed'

  async def _authenticate(self, websocket):
    """Perform authentication."""
    token = await websocket.recv()

    secret = self.secret.encode('ascii')
    signature = base64.b64encode(hmac.new(secret, token, digestmod=hashlib.sha256).digest())

    await websocket.send(signature)

    response = await websocket.recv()
    if response != b'Authenticated':
      return False

    return True

  def get_next_id(self):
    """Return the next request ID."""
    next_id = self.next_id
    self.next_id += 1
    return next_id

  async def send_data(self, websocket, data):
    """Send data."""
    chunk_count = max(int(math.ceil(len(data) / self.max_chunk_size)), 1)

    for i in range(chunk_count):
      chunk = data[i * self.max_chunk_size:(i + 1) * self.max_chunk_size]
      if i < chunk_count - 1:
        await websocket.send('1' + chunk)
      else:
        await websocket.send('2' + chunk)

  @staticmethod
  async def recv_data(websocket):
    """Receive data."""
    data = ''
    while True:
      raw_data = await websocket.recv()
      if raw_data[0] == '0':
        # Ignore ping
        # Sending a pong may cause a deadlock if the server sends lots of data
        continue
      else:
        data += raw_data[1:]
        if raw_data[0] == '2':
          break
        assert raw_data[0] == '1'
    return data


class ObjectProxy:
  """Serve as a proxy to an object."""

  def __init__(self, client, object_name, class_):
    self.client = client
    self.object_name = object_name
    self.class_ = class_

    for method_name in dir(class_):
      method = getattr(class_, method_name)

      if not hasattr(method, 'rpc_export'):
        continue
      method_type = getattr(method, 'rpc_export')

      if method_type == 'function':
        setattr(self, method_name, FunctionProxy(self, method_name))
      elif method_type == 'generator':
        setattr(self, method_name, GeneratorProxy(self, method_name))
      else:
        assert False


class FunctionProxy:
  """Serve as a proxy to a function."""

  def __init__(self, object_proxy, method_name):
    self.client = object_proxy.client
    self.method = f'function:{object_proxy.object_name}.{method_name}'

  async def __call__(self, *args, **kwargs):
    """Perform an RPC request to call a method on the server."""

    websocket = self.client.websocket
    request = {
      'id': self.client.get_next_id(),
      'method': self.method,
      'args': args,
      'kwargs': kwargs,
    }
    await self.client.send_data(websocket, json.dumps(request))

    response = json.loads(await self.client.recv_data(websocket))
    assert response['id'] == request['id']

    if 'error' in response:
      # sys.stderr.write(response['error'] + '\n' + response['data'] + '\n')
      sys.stderr.write(response['data'] + '\n')
      raise RuntimeError('Exception returned from the server')
    return response['result']


class GeneratorProxy:
  """Serve as a proxy to a generator."""

  def __init__(self, object_proxy, method_name):
    self.client = object_proxy.client
    self.method = f'generator:{object_proxy.object_name}.{method_name}'

  async def __call__(self, *args, **kwargs):
    """Perform an RPC request to call a method on the server."""

    websocket = self.client.websocket
    request = {
      'id': self.client.get_next_id(),
      'method': self.method,
      'args': args,
      'kwargs': kwargs,
    }
    await self.client.send_data(websocket, json.dumps(request))

    while True:
      response = json.loads(await self.client.recv_data(websocket))
      assert response['id'] == request['id']

      if 'error' in response:
        if response['error'] == 'StopAsyncIteration':
          break
        else:
          # sys.stderr.write(response['error'] + '\n' + response['data'] + '\n')
          sys.stderr.write(response['data'] + '\n')
          raise RuntimeError('Exception returned from the server')
      yield response['result']
