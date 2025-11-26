#!/usr/bin/env bash
set -euo pipefail

PYBIN="/opt/elasticbeanstalk/containerfiles/eb-python3.13/bin/python"
APP_DIR="${EB_APP_CURRENT_DIR:-/var/app/current}"

cd "$APP_DIR"
"$PYBIN" manage.py migrate --noinput
"$PYBIN" manage.py collectstatic --noinput
