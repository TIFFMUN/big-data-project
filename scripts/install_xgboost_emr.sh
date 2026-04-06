#!/bin/bash
set -euxo pipefail

PYTHON_BIN="$(command -v python3)"

sudo "$PYTHON_BIN" -m pip install --upgrade "xgboost==1.6.2"
