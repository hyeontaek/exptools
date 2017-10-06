'''Provide process execution functions.'''

__all__ = ['wait_for_procs', 'kill_procs', 'run_ssh_cmd']

import logging
import subprocess
import time

def wait_for_procs(procs, timeout=None):
  '''Wait for processes to terminate.'''
  wait_start = time.time()
  success = True
  pending = [True] * len(procs)

  while any(pending):
    for i, proc in enumerate(procs):
      if not pending[i]:
        continue

      try:
        # check the status every minute
        if proc.wait(timeout=60) == 0:
          pending[i] = False
        else:
          logging.getLogger('exptools.wait_for_procs').error('Failed execution')
          success = False
          pending = [False] * len(procs)
          break

      except subprocess.TimeoutExpired:
        if timeout is not None and time.time() - wait_start > timeout:
          # too long run time; give up
          logging.getLogger('exptools.wait_for_procs')\
              .error('Timeout after %d seconds', time.time() - wait_start)
          success = False
          pending = [False] * len(procs)
          break

  return success

def kill_procs(procs):
  '''Forcefully kill processes.'''
  for proc in procs:
    try:
      proc.kill()
    except subprocess.SubprocessError:
      logging.getLogger('exptools.kill_procs').exception('Exception while killing processes')

def run_ssh_cmd(host, cmd, **kwargs):
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

  proc = subprocess.Popen(ssh_cmd, stdin=subprocess.PIPE, **kwargs)
  proc.stdin.write(cmd.encode('utf-8'))
  proc.stdin.close()

  return proc
