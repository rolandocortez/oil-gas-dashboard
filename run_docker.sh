#!/usr/bin/env bash
set -euo pipefail

IMAGE=oil-gas-dashboard

docker build -t "$IMAGE" .
# Monta el repo y carga variables desde .env si existe
if [ -f ".env" ]; then
  docker run --rm --env-file .env -v "$PWD":/app "$IMAGE"
else
  docker run --rm -v "$PWD":/app "$IMAGE"
fi
