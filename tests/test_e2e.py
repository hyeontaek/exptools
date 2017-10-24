import asynctest.mock
import pytest

import subprocess
import tempfile
import time

@pytest.fixture(scope='module')
def server():
  with tempfile.TemporaryDirectory() as cwd:
    print(f'Working directory for exptools-server: {cwd}')
    proc = subprocess.Popen(['exptools-server'],
        bufsize=0,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=cwd)

    time.sleep(1)

    yield proc, cwd

    proc.kill()
    proc.wait()

def run(server, *args, stdin=None):
  cmd = ['etc', f'--secret={server[1]}/secret.json'] + list(args)
  proc = subprocess.Popen(cmd,
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE)

  stdout, stderr = proc.communicate(input=stdin)
  return proc.returncode, stdout, stderr

def test_s(server):
  returncode, stdout, stderr = run(server, 's')
  # pytest -s
  print(stdout)
