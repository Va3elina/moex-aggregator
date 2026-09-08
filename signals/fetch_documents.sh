#!/bin/bash
# Wrapper: документы компаний с CDN FinanceMarker → /opt/frame/docs + document_versions/pages.
#   30 6 * * * /opt/frame/signals/fetch_documents.sh --since-days 3 >> /opt/frame/logs/fetch_documents.log 2>&1
# Нужны pypdf и pdfplumber в signals/.venv.
set -eu
cd /opt/frame
export DB_URL=$(grep '^DB_URL=' .env | cut -d= -f2- | tr -d '\r' | sed 's/@db:/@127.0.0.1:/')
exec /opt/frame/signals/.venv/bin/python -m signals.fetch_documents "$@"
