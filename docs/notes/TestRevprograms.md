Yes — that’s a much better plan. If we can reproduce one known-good LEV dryrun and get the same QA + (optionally) final output, then we know the scaffold + scripts are solid. After that, we can worry about bucketing/housekeeping.

What I need from you to run one LEV dryrun end-to-end
1) SAAMM input for LEV (required)

One file:

SAAMM_LEV_... .csv (or .txt if that’s what you have)

Must contain these columns (as you showed before):
extractseqno, prod, source-desc-name, attributegrp, descrip1, descrip2, descrip3, lookupnm, user24, rowpointer

2) Pricing file for LEV (required)

One Excel file:

LEV_LEVITON_with_descriptions.xlsx (or whatever your LEV pricing file is called)

It needs at least:

Item# (or whichever join column the probe detects)

plus any description columns you want to feed token enrichment (often MANUFACTURER DESCRIPTION, Description)

3) Config files (required)

These 2 are enough for a dryrun:

configs/global/thresholds.json5

configs/global/schema_lock.json (used for writefinal, but include it anyway)

And for LEV line:

configs/lines/lev.json5

If you already have global synonym/glue in a global json5 (some versions do), include it too (only if it exists):

configs/global/global.json5 (name varies; if you see it, include it)

4) The Python scripts (required)

Minimum set to run the dryrun and generate QA:

src/build_desc3_enh.py (or if you don’t have enh yet, src/build_desc3.py)

src/join_utils.py

src/token_packs.py

src/qa_metrics.py

src/io_schemas.py

Optional but helpful (for the “sanity check” before running):

src/inspect_columns.py

src/probe_pricing_join.py

(Decision kit is NOT required for a dryrun.)

Folder placement (so commands are simple)

Put them here in Codespaces:

SAAMM: data/saamm/SAAMM_LEV_....csv

Pricing: data/pricing/LEV_....xlsx

LEV config: configs/lines/lev.json5

Globals: configs/global/*

Scripts: src/*

The exact dryrun command (LEV)

Once files are in place:

location and source of file 
--saamm "C:\Users\scottuser\Documents\SAAMM_WIP\SAAMM_XL\SAAMM_LEV_leviton_100795_1507.csv"
--pricing "C:\Users\scottuser\Documents\SAAMM_WIP\Pricing\LEV_LEVITON_with_descriptions.xlsx"

python src/build_desc3_enh.py \
  --line lev \
  --saamm  data/saamm/SAAMM_LEV_leviton_100795_1507.csv \
  --pricing data/pricing/LEV_LEVITON_with_descriptions.xlsx \
  --pricing-extra-cols "MANUFACTURER DESCRIPTION,Description" \
  --extra-into both \
  --pricing-join-prefer auto \
  --pline-in-descrip3 no \
  --pline-in-www no \
  --strip-parens yes \
  --squelch-stars yes \
  --out rep \
  --mode dryrun


It should produce something like:

rep/qa/QA_lev_en_<ts>.csv

rep/logs/run_lev_en_<ts>.log

rep/manifest/manifest_lev_en_<ts>.json

Then you compare the new QA/final patterns to your known-good outputs.

What you should upload (short list)

For LEV dryrun, upload these 3 items first:

SAAMM_LEV…csv

LEV pricing xlsx

lev.json5

If those work, we can proceed without pulling in the rest of your archive mess.

If you want, paste the ls -l data/saamm and ls -l data/pricing after you upload in Codespaces, and I’ll tell you the exact command with your exact filenames.

Yes — that’s the best plan.

If we can re-run LEV in a clean Codespaces repo and get the same dryrun QA (and optionally the same final CSV), then we know the program + configs are correct. After that, housekeeping/bucketing is just cleanup work.

Minimum files needed for ONE LEV dryrun
A) Inputs (2 files)

SAAMM LEV extract (CSV or TXT exported to CSV)

Example: SAAMM_LEV_leviton_100795_1507.csv

Must include these columns (exact names, case-insensitive is usually OK):

extractseqno, prod, source-desc-name, attributegrp, descrip1, descrip2, descrip3, lookupnm, user24, rowpointer

LEV Pricing workbook (Excel)

Example: LEV_LEVITON_with_descriptions.xlsx

Must include:

a join key column (often Item#)

at least one description column you want to enrich from (commonly MANUFACTURER DESCRIPTION and/or Description)

B) Configs (2–3 files)

configs/global/thresholds.json5 (defaults, trims, switches)

configs/global/schema_lock.json (output schema/order)

configs/lines/lev.json5 (LEV synonyms/glue/line rules)

If you also have a “global synonyms/glue” file (sometimes called global.json5), include it only if your code expects it. If not sure, skip it for now.

C) Python code (the run set)

src/build_desc3_enh.py (the runner)

src/join_utils.py

src/token_packs.py

src/qa_metrics.py

src/io_schemas.py

D) Optional “preflight” helpers (nice-to-have)

src/inspect_columns.py

src/probe_pricing_join.py

Folder placement in the repo (so commands are easy)

data/saamm/SAAMM_LEV....csv

data/pricing/LEV_....xlsx

configs/global/*

configs/lines/lev.json5

src/*

Dryrun command (template)
python src/build_desc3_enh.py \
  --line lev \
  --saamm  data/saamm/<your_saamm_file.csv> \
  --pricing data/pricing/<your_pricing_file.xlsx> \
  --pricing-extra-cols "MANUFACTURER DESCRIPTION,Description" \
  --extra-into both \
  --pricing-join-prefer auto \
  --pline-in-descrip3 no \
  --pline-in-www no \
  --strip-parens yes \
  --squelch-stars yes \
  --out rep \
  --mode dryrun

What I’d like you to do next (fastest)

Upload just these 3 files first into the repo:

SAAMM_LEV…csv

LEV pricing xlsx

configs/lines/lev.json5

Then run:

ls -l data/saamm
ls -l data/pricing