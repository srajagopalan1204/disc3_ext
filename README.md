# Update Description 3 (SAAMM + Pricing)

Builds a searchable `descrip3` with schema lock (only `descrip3` changes).
Inputs: SAAMM (primary), Pricing (secondary). PO removed.
See `docs/` for Spec + Runbook. Use `scripts/run_line.ps1` to run.


python build_desc3_saamm_only.py `
  --line app `
  --saamm "C:\Users\scottuser\Documents\SAAMM_WIP\Sent_SME\next batch\SAAMM_desc3_app_en_20260115_1630.csv" `
  --out  "C:\Users\scottuser\Documents\SAAMM_WIP\Logs\desc3_runs" `
  --mode writefinal
new multi mode rapid fire for basic update - no fancy merge 


01 17 2026 1442
python3 /workspaces/disc3_ext/src/saamm_desc3_update_v2_1.py \
  --saamm-in "/workspaces/disc3_ext/data/saamm" \
  --master-xlsx "/workspaces/disc3_ext/data/pricing/alt/Master_input_alt_price_all_lines_01172026_1219.xlsx" \
  --out-dir "/workspaces/disc3_ext/rep/final/qckMstr" \
  --log-dir "/workspaces/disc3_ext/rep/logs"
  after each batch

  bash /workspaces/disc3_ext/src/archive_saamm_inputs.sh "/workspaces/disc3_ext/data/saamm"
  