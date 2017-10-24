'''Implement the RPC server.'''

__all__ = ['Server']

import asyncio
import base64
import concurrent
import hashlib
import hmac
import json
import logging
import math
import secrets
import traceback

import websockets

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-instance-attributes
class Server:
  '''Implement a RPC server that exposes internal objects.'''

  # pylint: disable=too-many-arguments
  def __init__(self, host, port, secret,
               history, queue, scheduler, runner, filter_,
               ready_event, loop):
    self.host = host
    self.port = port
    self.secret = secret
    self.history = history
    self.queue = queue
    self.scheduler = scheduler
    self.runner = runner
    self.filter = filter_
    self.ready_event = ready_event
    self.loop = loop

    self.max_size = 1048576
    self.max_chunk_size = self.max_size // 2

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
    try:
      while True:
        await asyncio.sleep(10, loop=self.loop)
        # Use custom unidirectional pings
        # The server often does not use recv() to consume client Pong
        # while it sends a stream/large amount of data to clients slowly,
        # which may cause a deadlock
        await websocket.send('0')

    except concurrent.futures.CancelledError:
      pass
    except websockets.exceptions.ConnectionClosed:
      pass

  async def _send_data(self, websocket, data):
    '''Send data.'''
    chunk_count = max(math.ceil(len(data) / self.max_chunk_size), 1)

    for i in range(chunk_count):
      chunk = data[i * self.max_chunk_size:(i + 1) * self.max_chunk_size]
      if i < chunk_count - 1:
        await websocket.send('1' + chunk)
      else:
        await websocket.send('2' + chunk)

  async def _recv_data(self, websocket):
    '''Receive data.'''
    data = ''
    while True:
      raw_data = await websocket.recv()
      if raw_data[0] == '0':
        # Ignore ping
        continue
      else:
        data += raw_data[1:]
        if raw_data[0] == '2':
          break
        assert raw_data[0] == '1'
    return data

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
        await self._send_data(websocket, json.dumps({'id': id_, 'result': result}))

      elif method.startswith('generator:'):
        async for result in self.exports[method](*args, **kwargs):
          await self._send_data(websocket, json.dumps({'id': id_, 'result': result}))
        await self._send_data(
            websocket, json.dumps({'id': id_, 'error': 'StopAsyncIteration', 'data': None}))

      else:
        await self._send_data(
            websocket, json.dumps({'id': id_, 'error': 'InvalidMethod', 'data': None}))

    except concurrent.futures.CancelledError:
      # Pass through
      raise
    except websockets.exceptions.ConnectionClosed:
      # Pass through
      raise

    except Exception as exc: # pylint: disable=broad-except
      self.logger.exception('Exception while handling request')
      await self._send_data(websocket, json.dumps({
          'id': id_,
          'error': exc.__class__.__name__,
          'data': traceback.format_exc(),
          }))

  async def _handle_requests(self, websocket):
    try:
      while True:
        request = await self._recv_data(websocket)
        await self._handle_request(websocket, request)
    except concurrent.futures.CancelledError:
      pass

  def _get_serve(self):
    # pylint: disable=unused-argument
    async def _serve(websocket, path):
      try:
        try:
          if not await asyncio.wait_for(
              self._authenticate(websocket), timeout=10, loop=self.loop):
            # authentication failed
            return
        except asyncio.TimeoutError:
          # authentication timeout
          return

        tasks = [
            asyncio.ensure_future(self._send_pings(websocket), loop=self.loop),
            asyncio.ensure_future(self._handle_requests(websocket), loop=self.loop),
            ]
        try:
          # Stop waiting if any of two handlers fails
          await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, loop=self.loop)
        finally:
          for task in tasks:
            task.cancel()
          await asyncio.gather(*tasks, loop=self.loop)

      except websockets.exceptions.ConnectionClosed:
        self.logger.debug('Connection closed')
    return _serve

  async def run_forever(self):
    '''Serve websocket requests.'''
    try:
      server = await websockets.serve(
          self._get_serve(), self.host, self.port, max_size=self.max_size, loop=self.loop)
      try:
        self.logger.info(f'Listening on ws://{self.host}:{self.port}/')

        if self.ready_event:
          self.ready_event.set()

        # Sleep forever
        while True:
          await asyncio.sleep(60, loop=self.loop)
      finally:
        # Close the server
        server.close()
        await server.wait_closed()
    except concurrent.futures.CancelledError:
      pass
    except Exception: # pylint: disable=broad-except
      self.logger.exception('Exception while initializing server')
    finally:
      if self.ready_event:
        if not self.ready_event.is_set():
          self.ready_event.set()

  async def wait_for_ready(self):
    '''Wait until the server becomes ready (or fails to initialize.'''
    await self.ready_event.wait()
