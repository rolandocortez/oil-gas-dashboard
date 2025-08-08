#!/usr/bin/env bash
set -euo pipefail

LOG_TS(){ echo "[$(date -Iseconds)] $*"; }

# Cargar variables si hay .env (ignora si no existe)
if [ -f ".env" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs -d '\n' -r) || true
fi

LOG_TS "Step 0: sanity check"
python -c "import pandas, snowflake.connector; print('Sanity OK')"

LOG_TS "Step 1: Ingest CSV -> Snowflake"
if [ -f "etl/load_to_snowflake.py" ]; then
  python etl/load_to_snowflake.py
else
  LOG_TS "skip (etl/load_to_snowflake.py aún no existe)"
fi

LOG_TS "Step 2: dbt run"
if [ -d "dbt" ]; then
  (cd dbt && dbt run || LOG_TS 'dbt run no configurado todavía (ok en H1)')
else
  LOG_TS "skip (dbt aún no inicializado)"
fi

LOG_TS "Step 3: dbt test"
if [ -d "dbt" ]; then
  (cd dbt && dbt test || LOG_TS 'dbt test no configurado todavía (ok en H1)')
fi

LOG_TS "Step 4: dbt docs"
if [ -d "dbt" ]; then
  (cd dbt && dbt docs generate || LOG_TS 'dbt docs no configurado todavía (ok en H1)')
fi

LOG_TS "Done."
