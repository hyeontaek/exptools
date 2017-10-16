#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}/.."

python setup.py bdist_wheel
