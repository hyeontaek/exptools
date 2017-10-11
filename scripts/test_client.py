#!/usr/bin/env python

import argparse
from exptools.rpc import Client

parser = argparse.ArgumentParser(description='Run an exptools RPC client.')
parser.add_argument('--host', dest='host', type=str, default='localhost', help='hostname')
parser.add_argument('--port', dest='port', type=int, default='31234', help='port')

if __name__ == '__main__':
  args = parser.parse_args()

  client = Client(args.host, args.port)
  for i in range(10):
    print(client.runner.format_remaining_time())
