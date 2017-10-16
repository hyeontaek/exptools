'''Implement the RPC server.'''

__all__ = ['Server']

import asyncio
import base64
import concurrent
import hashlib
import hmac
import json
import logging
import secrets
import traceback

import websockets

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-instance-attributes
class Server:
  '''Implement a RPC server that exposes internal objects.'''

  # pylint: disable=too-many-arguments
  def __init__(self, host, port, secret, history, queue, scheduler, runner, filter_, loop):
    self.host = host
    self.port = port
    self.secret = secret
    self.history = history
    self.queue = queue
    self.scheduler = scheduler
    self.runner = runner
    self.filter = filter_
    self.loop = loop

    self.logger = logging.getLogger('exptools.Server')

    self._init_exports()

  def _init_exports(self):
    '''Initialize the method map.'''
    self.exports = {}

    for object_name, obj in [
        ('history', self.history),
        ('queue', self.queue),
        ('scheduler', self.scheduler),
        ('runner', self.runner),
        ('filter', self.filter),
        ]:
      for method_name in dir(obj):
        method = getattr(obj, method_name)
        if not hasattr(method, 'rpc_export'):
          continue
        if getattr(method, 'rpc_export') == 'function':
          self.exports[f'function:{object_name}.{method_name}'] = method
        elif getattr(method, 'rpc_export') == 'generator':
          self.exports[f'generator:{object_name}.{method_name}'] = method
        else:
          assert False
    self.logger.debug('Exported: %s', list(self.exports.keys()))

  async def _authenticate(self, websocket):
    '''Authenticate a client.'''

    token = secrets.token_hex().encode('ascii')
    await websocket.send(token)

    secret = self.secret.encode('ascii')
    signature = base64.b64encode(hmac.new(secret, token, digestmod=hashlib.sha256).digest())

    client_signature = await websocket.recv()

    if not secrets.compare_digest(signature, client_signature):
      await websocket.send(b'Not authenticated')
      return False

    await websocket.send(b'Authenticated')
    return True

  async def _send_pings(self, websocket):
    ping = json.dumps('ping')
    try:
      while True:
        await asyncio.sleep(10, loop=self.loop)
        await websocket.send(ping)

    except concurrent.futures.CancelledError:
      # Ignore cancelled task
      pass
    except websockets.exceptions.ConnectionClosed:
      # Ignore closed connection
      pass

  async def _handle_request(self, websocket, request):
    '''Handle a request.'''
    try:
      request = json.loads(request)
      self.logger.debug('Request: %s', request)

      id_ = request['id']
      method = request['method']
      args = request['args']
      kwargs = request['kwargs']

      if method.startswith('function:'):
        result = await self.exports[method](*args, **kwargs)
        await websocket.send(json.dumps({'id': id_, 'result': result}))

      elif method.startswith('generator:'):
        async for result in self.exports[method](*args, **kwargs):
          await websocket.send(json.dumps({'id': id_, 'result': result}))
        await websocket.send(json.dumps({'id': id_, 'error': 'StopAsyncIteration', 'data': None}))

      else:
        await websocket.send(json.dumps({'id': id_, 'error': 'InvalidMethod', 'data': None}))

    except concurrent.futures.CancelledError:
      # Pass through
      raise
    except websockets.exceptions.ConnectionClosed:
      # Pass through
      raise

    except Exception as exc: # pylint: disable=broad-except
      self.logger.exception('Exception while handling request')
      await websocket.send(json.dumps({
          'id': id_,
          'error': exc.__class__.__name__,
          'data': traceback.format_exc(),
          }))

  async def _handle_requests(self, websocket):
    try:
      while True:
        request = await websocket.recv()
        await self._handle_request(websocket, request)
    except concurrent.futures.CancelledError:
      # Ignore cancelled task
      pass
    except websockets.exceptions.ConnectionClosed:
      # Ignore closed connection
      pass

  def serve_forever(self):
    '''Serve websocket requests forever.'''
    # pylint: disable=unused-argument
    async def _serve(websocket, path):
      try:
        if not await self._authenticate(websocket):
          return

        done, pending = await asyncio.wait([
            asyncio.ensure_future(self._send_pings(websocket), loop=self.loop),
            asyncio.ensure_future(self._handle_requests(websocket), loop=self.loop),
            ], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
          task.cancel()

      except websockets.exceptions.ConnectionClosed:
        self.logger.debug('Connection closed')

    start_server = websockets.serve(_serve, self.host, self.port)
    self.logger.info(f'Listening on ws://{self.host}:{self.port}/')

    self.loop.run_until_complete(start_server)
    self.loop.run_forever()
