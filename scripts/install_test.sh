#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}/.."

pip install -e .[test]
