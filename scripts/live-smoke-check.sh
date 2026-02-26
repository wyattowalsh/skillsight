#!/usr/bin/env bash
set -euo pipefail

: "${SMOKE_WEB_URL:?Set SMOKE_WEB_URL (base web URL)}"

search_base="${SMOKE_SEARCH_URL:-${SMOKE_WORKER_URL:-}}"
data_base="${SMOKE_DATA_URL:-}"

: "${search_base:?Set SMOKE_SEARCH_URL (or legacy SMOKE_WORKER_URL) for search API checks}"
: "${data_base:?Set SMOKE_DATA_URL (base static data URL, e.g. R2 public domain)}"

search_path="${SMOKE_SEARCH_PATH:-/v1/search?q=skill&page=1&page_size=1}"
data_path="${SMOKE_DATA_PATH:-/data/v1/latest.json}"
web_path="${SMOKE_WEB_PATH:-/}"

if [[ "${search_path}" != /* ]]; then
  search_path="/${search_path}"
fi
if [[ "${web_path}" != /* ]]; then
  web_path="/${web_path}"
fi
if [[ "${data_path}" != /* ]]; then
  data_path="/${data_path}"
fi

search_target="${search_base%/}${search_path}"
data_target="${data_base%/}${data_path}"
web_target="${SMOKE_WEB_URL%/}${web_path}"

echo "Checking static data manifest: ${data_target}"
curl -fsS --max-time 20 "${data_target}" >/dev/null

echo "Checking search API: ${search_target}"
curl -fsS --max-time 20 "${search_target}" >/dev/null

echo "Checking web: ${web_target}"
curl -fsS --max-time 20 "${web_target}" >/dev/null

echo "Smoke checks passed."
