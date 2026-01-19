#!/usr/bin/env bash
set -euo pipefail

IN_DIR="${1:-}"
if [[ -z "${IN_DIR}" ]]; then
  echo "Usage: bash archive_saamm_inputs.sh /workspaces/disc3_ext/data/saamm"
  exit 1
fi

if [[ ! -d "${IN_DIR}" ]]; then
  echo "ERROR: Not a directory: ${IN_DIR}"
  exit 1
fi

STAMP="$(date +%m%d%Y_%H%M)"
ARCHIVE_DIR="${IN_DIR}/arch_${STAMP}"
mkdir -p "${ARCHIVE_DIR}"

shopt -s nullglob

moved=0
for f in "${IN_DIR}"/*.txt "${IN_DIR}"/*.tsv "${IN_DIR}"/*.tab; do
  if [[ -f "$f" ]]; then
    mv "$f" "${ARCHIVE_DIR}/"
    moved=$((moved+1))
  fi
done

echo "Archived ${moved} file(s) to: ${ARCHIVE_DIR}"
