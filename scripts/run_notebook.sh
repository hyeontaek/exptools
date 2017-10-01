#!/bin/sh

jupyter notebook --log-level=ERROR --ip=0.0.0.0 --port=48888 --no-browser --notebook-dir=$(dirname "$0")/../exptools/sample
