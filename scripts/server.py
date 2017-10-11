#!/usr/bin/env python

import argparse
from exptools.history import History
from exptools.runner import Runner
from exptools.rpc import Server

parser = argparse.ArgumentParser(description='Run an exptools RPC server.')
parser.add_argument('--host', dest='host', type=str, default='localhost', help='hostname')
parser.add_argument('--port', dest='port', type=int, default='31234', help='port')
parser.add_argument('--hist-path', dest='hist_path', type=str, default=None, help='path to hist.dat')

if __name__ == '__main__':
  args = parser.parse_args()
   
  if args.hist_path is not None:
    print(f'Using {args.hist_path}')
  hist = History(args.hist_path)
  runner = Runner(hist)

  server = Server(args.host, args.port, hist, runner)
  server.serve_forever()
