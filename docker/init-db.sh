#!/bin/bash
set -e

echo "[INIT] Restaurando banco de dados '$POSTGRES_DB' com usuario '$POSTGRES_USER'..."
pg_restore \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --no-owner \
  --role="$POSTGRES_USER" \
  /tmp/bcbdb.dump
echo "[INIT] Banco de dados restaurado com sucesso."