'''Provide process execution functions.'''

__all__ = [
    'sync',
    'async_run_cmd', 'run_cmd',
    'async_run_ssh_cmd', 'run_ssh_cmd'
    'async_wait_for_procs', 'wait_for_procs',
    'kill_procs',
    'all_success_returncode',
    ]

import asyncio

def sync(co, loop=None):
  '''Wait for a task and return the result.'''
  if loop is None:
    loop = asyncio.get_event_loop()
  task = asyncio.ensure_future(co, loop=loop)
  loop.run_until_complete(task)
  return task.result()

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

  tasks = [proc.wait() for proc in procs]
  _, pending = await asyncio.wait(tasks, timeout=timeout, loop=loop)

  success = not pending
  returncode_list = [proc.returncode for proc in procs]
  return success, returncode_list

def wait_for_procs(*args, **kwargs):
  '''Wait for processes to terminate.'''
  return sync(async_wait_for_procs(*args, **kwargs))

def kill_procs(procs, force=False):
  '''Kill processes.'''
  for proc in procs:
    if not force:
      proc.kill()
    else:
      proc.terminate()

def all_success_returncode(returncode_list):
  '''Check if all of the returncode indicates a sucess code.'''
  for returncode in returncode_list:
    if returncode != 0:
      return False
  return True
