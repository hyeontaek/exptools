'''Provide process execution functions.'''

__all__ = ['wait_for_procs', 'all_success_returncode', 'kill_procs', 'run_cmd', 'run_ssh_cmd']

import asyncio

def wait_for_procs(procs, timeout=None, loop=None):
  '''Wait for processes to terminate.'''

  if not loop:
    loop = asyncio.get_event_loop()

  tasks = [proc.wait() for proc in procs]

  wait_task = asyncio.ensure_future(asyncio.wait(tasks, timeout=timeout, loop=loop), loop=loop)
  loop.run_until_complete(wait_task)

  _, pending = wait_task.result()

  success = not pending
  returncode_list = [proc.returncode for proc in procs]
  return success, returncode_list

def all_success_returncode(returncode_list):
  '''Check if all of the returncode indicates a sucess code.'''
  for returncode in returncode_list:
    if returncode != 0:
      return False
  return True

def kill_procs(procs, force=False):
  '''Kill processes.'''
  for proc in procs:
    if not force:
      proc.kill()
    else:
      proc.terminate()

def run_cmd(cmd, loop=None, **kwargs):
  if not loop:
    loop = asyncio.get_event_loop()

  co = asyncio.create_subprocess_exec(*cmd, stdin=asyncio.subprocess.DEVNULL, loop=loop, **kwargs)
  task = asyncio.ensure_future(co, loop=loop)

  loop.run_until_complete(task)
  proc = task.result()

  return proc

def run_ssh_cmd(host, cmd, loop=None, **kwargs):
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

  proc = run(ssh_cmd, stdin=subprocess.PIPE, loop=loop, **kwargs)
  proc.stdin.write(cmd.encode('utf-8'))
  proc.stdin.close()

  return proc
