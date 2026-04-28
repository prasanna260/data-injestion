#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
LOG_FILE="${SCRIPT_DIR}/feeder.log"
CRON_EXPR="${1:-*/5 * * * *}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing virtualenv python at ${PYTHON_BIN}"
  echo "Create it first:"
  echo "  cd ${SCRIPT_DIR}"
  echo "  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

JOB="${CRON_EXPR} cd \"${SCRIPT_DIR}\" && \"${PYTHON_BIN}\" \"${SCRIPT_DIR}/fetch_global_markets.py\" >> \"${LOG_FILE}\" 2>&1"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
UPDATED_CRON="$(printf "%s\n" "${CURRENT_CRON}" | grep -v "fetch_global_markets.py" || true)"
UPDATED_CRON="$(printf "%s\n%s\n" "${UPDATED_CRON}" "${JOB}")"

printf "%s\n" "${UPDATED_CRON}" | crontab -

echo "Installed cron job:"
echo "  ${JOB}"
echo "Logs:"
echo "  tail -f ${LOG_FILE}"
