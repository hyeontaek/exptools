"""Implement the RPC server."""

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


class Server:
  """Implement a RPC server that exposes internal objects."""

  def __init__(self, host, port, secret,
               registry, history, queue, resolver, scheduler, runner,
               ready_event, loop):
    self.host = host
    self.port = port
    self.secret = secret
    self.registry = registry
    self.history = history
    self.queue = queue
    self.resolver = resolver
    self.scheduler = scheduler
    self.runner = runner
    self.ready_event = ready_event
    self.loop = loop

    self.max_size = 1048576
    self.max_chunk_size = self.max_size // 2

    self.logger = logging.getLogger('exptools.Server')

    self._init_exports()

  def _init_exports(self):
    """Initialize the method map."""
    self.exports = {}

    object_map = [
      ('registry', self.registry),
      ('history', self.history),
      ('queue', self.queue),
      ('resolver', self.resolver),
      ('scheduler', self.scheduler),
      ('runner', self.runner),
    ]

    for object_name, obj in object_map:
      for method_name in dir(obj):
        method = getattr(obj, method_name)

        if not hasattr(method, 'rpc_export'):
          continue
        method_type = getattr(method, 'rpc_export')

        if method_type == 'function':
          self.exports[f'function:{object_name}.{method_name}'] = method
        elif method_type == 'generator':
          self.exports[f'generator:{object_name}.{method_name}'] = method
        else:
          assert False

    self.logger.debug('Exported: %s', list(self.exports.keys()))

  async def _authenticate(self, websocket):
    """Authenticate a client."""

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
    while True:
      await asyncio.sleep(10, loop=self.loop)
      # Use custom unidirectional pings
      # The server often does not use recv() to consume client Pong
      # while it sends a stream/large amount of data to clients slowly,
      # which may cause a deadlock
      await websocket.send('0')

  async def _send_data(self, websocket, data):
    """Send data."""
    chunk_count = max(int(math.ceil(len(data) / self.max_chunk_size)), 1)

    for i in range(chunk_count):
      chunk = data[i * self.max_chunk_size:(i + 1) * self.max_chunk_size]
      if i < chunk_count - 1:
        await websocket.send('1' + chunk)
      else:
        await websocket.send('2' + chunk)

  @staticmethod
  async def _recv_data(websocket):
    """Receive data."""
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
    """Handle a request."""
    id_ = None
    try:
      request = json.loads(request)
      self.logger.debug('Request: %s', request)

      id_ = request['id']
      method = request['method']
      args = request['args']
      kwargs = request['kwargs']

      if method.startswith('function:'):
        coro = self.exports[method](*args, **kwargs)
        result = await coro
        await self._send_data(websocket, json.dumps({'id': id_, 'result': result}))

      elif method.startswith('generator:'):
        coro = self.exports[method](*args, **kwargs)
        async for result in coro:
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
    except Exception as exc:  # pylint: disable=broad-except
      self.logger.exception('Exception while handling request')
      await self._send_data(websocket, json.dumps({
        'id': id_,
        'error': exc.__class__.__name__,
        'data': traceback.format_exc(),
      }))

  async def _handle_requests(self, websocket):
    while True:
      request = await self._recv_data(websocket)
      await self._handle_request(websocket, request)

  def _get_serve(self):
    async def _serve(websocket, path):  # pylint: disable=unused-argument
      try:
        try:
          if not await asyncio.wait_for(self._authenticate(websocket), timeout=10, loop=self.loop):
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
          await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION, loop=self.loop)
        finally:
          for task in tasks:
            task.cancel()
            try:
              await task
            except concurrent.futures.CancelledError:
              # Ignore CancelledError because we caused it
              pass
            except websockets.exceptions.ConnectionClosed:
              pass

      except websockets.exceptions.ConnectionClosed:
        self.logger.debug('Connection closed')

    return _serve

  async def run_forever(self):
    """Serve websocket requests."""
    try:
      server = await websockets.serve(
        self._get_serve(), self.host, self.port, max_size=self.max_size, loop=self.loop)
    except Exception:  # pylint: disable=broad-except
      # Set self.ready_event anyway to wake up waiters
      if self.ready_event:
        self.ready_event.set()
      raise

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

  async def wait_for_ready(self):
    """Wait until the server becomes ready (or fails to initialize."""
    await self.ready_event.wait()
