# Ground Truth App

Human-in-the-loop annotation tool for the **multi-document splitting** pipeline.
A reviewer opens a sampled PDF, marks where each new document starts, flags whether
the package is multi-document, and saves. The app stores the label as JSON in the
`ground_truth/` volume and compares it against the model's prediction
(`split_results.predicted_starts`), writing metrics to `evaluation_results`.

Runs as a **Databricks App** (Flask backend; pages are rendered server-side to
JPEG by PyMuPDF and lazy-loaded in the browser — no PDF.js, no CDN). The UI is a
neutral black/white/gray theme.

**Operator-blind:** the model's predicted boundaries are never sent to the
annotation page, so the human's labelling is unbiased. The comparison happens
server-side at save time, and metrics are shown only *after* the operator commits.

## Where it sits in the pipeline

```
inbox → nb_parse → parsed_documents → nb_split → split_results → nb_pdf_split → output/ → nb_sftp_upload → SFTP
                                            │
                        nb_check_export copies a sample → validation/
                                            │
                                   ┌────────▼────────┐
                                   │  Ground Truth   │  ← this app
                                   │      App        │
                                   └────────┬────────┘
                         ground_truth/*.json + evaluation_results
```

## What the annotator does

1. Picks a PDF from `validation/` (sidebar shows pending vs completed).
2. Clicks each page that is the **first page of a new document**. Page 1 is always
   a start. (No model hints — the operator works blind.)
3. Toggles **Multi-document** (auto-set from boundary count, manually overridable).
4. **Save & Evaluate** → writes the GT JSON and shows precision / recall / F1
   (exact and ±1-tolerant) plus exact-match and multidoc-correct flags.

## Dashboard

`/dashboard` aggregates `evaluation_results` into headline numbers — PDFs annotated,
exact split matches + rate, multidoc-correct rate, off-by-one count, and average
precision / recall / F1 (exact and ±1). Export buttons produce a **PNG** snapshot
(vendored `html2canvas`, no CDN) and a **standalone HTML** file to drop into slides.

## Data produced

**`ground_truth/{filename}.json`**
```json
{
  "filename": "34572_PACKAGE",
  "folder_id": "34572",
  "total_pages": 12,
  "is_multidoc": true,
  "predicted_starts": [1, 5, 9],
  "n_documents": 3,
  "documents": [
    {"start": 1, "end": 4, "type": null},
    {"start": 5, "end": 8, "type": null},
    {"start": 9, "end": 12, "type": null}
  ],
  "annotator": "lorenzo.muscillo@luxottica.com",
  "annotated_at": "2026-06-25T10:00:00+00:00",
  "schema_version": 1
}
```
`documents[].type` is reserved `null` — v2 will let annotators tag document types
(Invoice, AWB, …) without changing the boundary model.

**`evaluation_results`** — one row per save. See [sql/ddl_evaluation_results.sql](sql/ddl_evaluation_results.sql).

## Evaluation metric

A boundary = first page of a document. Page 1 is excluded from precision/recall/F1
(it is always a trivial boundary that both sides agree on); `exact_match` compares
the full sets including page 1.

- **Exact**: a predicted boundary is correct only if its page is in the GT set.
- **Tolerant (±1)**: a predicted boundary within one page of an unused GT boundary
  counts as a match (greedy nearest). `n_offby1` = how many matches were off-by-one.
- **multidoc_correct**: `(gt_n_docs > 1) == (model_n_docs > 1)`.

## Setup

1. **Create the table** (once): run [sql/ddl_evaluation_results.sql](sql/ddl_evaluation_results.sql)
   in a SQL editor or notebook.
2. **Configure** [app.yaml](app.yaml): set `DATABRICKS_WAREHOUSE_ID` and, if different,
   `UC_CATALOG` / `UC_SCHEMA`.
3. **Grant the app's service principal**:
   - `CAN_USE` on the SQL Warehouse,
   - `SELECT` on `split_results`, `INSERT` on `evaluation_results`,
   - `READ VOLUME` on `validation/`, `READ/WRITE VOLUME` on `ground_truth/`.
4. **Deploy**: `databricks apps deploy` (or via the Apps UI pointing at this folder).

## Local development / testing (no Databricks)

`run_local.py` runs the whole app against the local filesystem — no workspace
connection. It reuses only the pure `src/evaluation.py`; production modules are
untouched. It serves the same `templates/` + `static/`, renders pages with the same
server-side PyMuPDF path, and **fakes** `split_results` so evaluation has something
to compare against (`local_data/model_predictions.json`).

```bash
pip install -r requirements.txt
python run_local.py --check-dir "C:/path/to/pdfs" --regen-model
# open http://localhost:8000   (and /dashboard)
```

Local mappings: `validation/` → `--check-dir`; `split_results` → `local_data/model_predictions.json`;
`ground_truth/` → `local_data/ground_truth/*.json`; `evaluation_results` → `local_data/evaluation_results.jsonl`.

To run the **production** app instead (the Databricks SDK auto-authenticates from a
`DATABRICKS` profile / env vars):

```bash
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<pat>
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
python app.py            # http://localhost:8000
```

## Layout

```
app.py                          Flask routes (production)
run_local.py                    self-contained local test harness (filesystem backend)
app.yaml                        Databricks Apps config
requirements.txt
src/config.py                   env-driven config
src/db.py                       SQL Warehouse access (SDK StatementExecution)
src/volumes.py                  UC Volume access (SDK Files API)
src/evaluation.py               boundary metrics + dashboard aggregate (pure, unit-testable)
src/annotation.py               worklist, model lookup, page render, GT persistence, eval, stats
templates/index.html            annotation UI
templates/dashboard.html        evaluation dashboard
static/js/app.js                lazy image viewer + annotation logic
static/js/dashboard.js          stats render + PNG/HTML export
static/vendor/html2canvas.min.js  vendored (PNG export, no CDN)
static/css/style.css            monochrome theme
sql/ddl_evaluation_results.sql  table DDL
```

## Notes / future work

- **Types per document** (Invoice/AWB/…) — schema already carries a `type` slot.
- **Concurrency**: GT files are keyed by filename; last save wins. Fine for a small
  review team; add optimistic locking if multiple reviewers share one file.
- Pages render server-side to JPEG (`PAGE_ZOOM`/`JPEG_QUALITY` in `src/annotation.py`
  and `run_local.py`) and the browser lazy-loads only visible pages. The in-process
  PDF cache is per-worker (bounded); fine for single-reviewer use.
