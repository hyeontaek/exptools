#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}/.."

pytest --cov=exptools --cov-report html:cov_html --color=no
