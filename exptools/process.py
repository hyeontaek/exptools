'''Provide process execution functions.'''

__all__ = [
    'sync',
    'async_run_cmd', 'run_cmd',
    'async_run_ssh_cmd', 'run_ssh_cmd',
    'async_wait_for_procs', 'wait_for_procs',
    'kill_procs',
    'all_success_returncode',
    ]

import asyncio
import concurrent
import signal
import traceback

def sync(cor, loop=None):
  '''Wait for a task and return the result.'''
  if loop is None:
    loop = asyncio.get_event_loop()
  return loop.run_until_complete(cor)

async def async_run_cmd(cmd, loop=None, **kwargs):
  '''Run a command.'''
  return await asyncio.create_subprocess_exec(*cmd, loop=loop, **kwargs)

def run_cmd(*args, **kwargs):
  '''Run a command.'''
  return sync(async_run_cmd(*args, **kwargs))

async def async_run_ssh_cmd(host, cmd, loop=None, **kwargs):
  '''Run a remote command using ssh.'''

  ssh_cmd = ['ssh']
  ssh_cmd += ['-o', 'ServerAliveInterval=10']
  ssh_cmd += ['-o', 'ServerAliveCountMax=6']
  ssh_cmd += ['-T']
  if host.find(':') != -1:
    host, _, port = host.partition(':')
    ssh_cmd += ['-p', port]
  ssh_cmd += [host]
  ssh_cmd += ['bash', '-l']

  proc = await async_run_cmd(ssh_cmd, stdin=asyncio.subprocess.PIPE, loop=loop, **kwargs)
  proc.stdin.write(cmd.encode('utf-8'))
  proc.stdin.close()

  return proc

def run_ssh_cmd(*args, **kwargs):
  '''Run a remote command using ssh.'''
  return sync(async_run_ssh_cmd(*args, **kwargs))

async def async_wait_for_procs(procs, timeout=None, loop=None):
  '''Wait for processes to terminate.'''

  if not procs:
    return True, []

  async def _wait(proc):
    await proc.wait()
    if proc.returncode != 0:
      raise RuntimeError(f'Got failure returncode={proc.returncode}')

  tasks = [_wait(proc) for proc in procs]
  done, pending = await asyncio.wait(
      tasks, timeout=timeout, return_when=asyncio.FIRST_EXCEPTION, loop=loop)

  for task in pending:
    task.cancel()

  for task in list(done) + list(pending):
    try:
      await task
    except concurrent.futures.CancelledError:
      # Ignore CancelledError because we caused it
      pass
    except Exception: # pylint: disable=broad-except
      traceback.print_exc()

  success = not pending
  returncode_list = [proc.returncode for proc in procs]
  return success, returncode_list

def wait_for_procs(*args, **kwargs):
  '''Wait for processes to terminate.'''
  return sync(async_wait_for_procs(*args, **kwargs))

def kill_procs(procs, signal_type=None):
  '''Kill processes.'''
  for proc in procs:
    try:
      if signal_type == 'int':
        # Ctrl-C
        proc.send_signal(signal.SIGINT)
      elif not signal_type or signal_type == 'term':
        # Can be caught/ignored (default)
        proc.send_signal(signal.SIGTERM)
      elif signal_type == 'kill':
        # Cannot be ignored
        proc.send_signal(signal.SIGKILL)
      else:
        raise RuntimeError(f'Unknown signal: {signal_type}')
    except ProcessLookupError:
      traceback.print_exc()

def all_success_returncode(returncode_list):
  '''Check if all of the returncode indicates a sucess code.'''
  for returncode in returncode_list:
    if returncode != 0:
      return False
  return True
