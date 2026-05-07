# Architecture Documentation

## Overview

The pipeline follows the **medallion architecture** (Bronze → Silver → Gold)
on Databricks Delta Live Tables, with Azure Blob Storage as the document
landing zone.

---

## Layer 1 — Ingestion (Azure Blob → UC Volume)

**Notebook:** `notebooks/01_bronze_ingestion.py`

PDF documents arrive in Azure Blob Storage under three sub-folders:
- `purchase_order/` — PO documents from procurement system
- `good_receipt/`  — GR documents from warehouse system
- `invoice/`       — Tax invoices from vendors

The ingestion script syncs new/updated files from blob into a
Databricks Unity Catalog Volume (`staging.p2p_files`). It uses
size-based deduplication to skip already-synced files.

**Key design decisions:**
- UC Volume path (`/Volumes/...`) used directly — no `/dbfs` prefix
  (required for Databricks Serverless compute)
- Azure connection string stored in Databricks Secrets (never plaintext)
- In production: replace with Auto Loader + Azure Event Grid for
  event-driven ingestion (sub-second latency vs. scheduled polling)

---

## Layer 2 — Bronze DLT

**Notebook:** `notebooks/02_bronze_dlt.py`
**Target schema:** `<catalog>.bronze`

Reads PDF binary files from UC Volume using Spark `binaryFile` format.
Applies three transformations per document:

| Column | Description |
|---|---|
| `filename` | Document ID extracted from file path |
| `doc_type` | `purchase_order`, `good_receipt`, or `invoice` |
| `text` | Plain text extracted from PDF via pdfminer + pypdf fallback |
| `file_hash` | SHA-256 hash for deduplication |
| `file_size_bytes` | File size for quality gate |
| `_text_extracted` | Boolean — was text extraction successful |
| `_load_timestamp` | Ingestion timestamp |
| `_source_path` | Original blob path for lineage |

**Quality gates** (DLT `@dlt.expect_or_drop`):
- `filename_not_null` — document must have a parseable filename
- `file_not_empty` — minimum 500 bytes
- `text_extracted` — pdfminer/pypdf must return > 20 characters

Documents failing any gate go to `bronze.quarantine` table.

**Change Data Feed** enabled on all Bronze tables — required for
downstream Vector Search index incremental sync.

---

## Layer 3 — Silver DLT

**Notebook:** `notebooks/03_silver_dlt.py`
**Target schema:** `<catalog>.silver`

Transforms raw PDF text into structured fields using a two-tier
extraction strategy:

### Tier 1 — Databricks ai_extract()

```python
ai_extract(text, array(
    'po_number', 'vendor_name', 'total_amount_aed', ...
))
```

Uses Databricks Foundation Model APIs under the hood. Returns structured
fields as a MAP type. Fast and handles complex layouts well but
occasionally returns null on longer documents (4+ line items).

### Tier 2 — Regex Fallback

For every critical field, a field-specific regex UDF fires when
`ai_extract` returns null:

| Field | Regex strategy |
|---|---|
| `po_number` | Pattern: `PO-\d{4}-\d{4}` |
| `gr_number` | Pattern: `GR-\d{4}-\d{4}` |
| `invoice_number` | Pattern: `INV-\d{4}-\d{4}(-DUP)?` |
| `vendor_name` (PO) | After `Vendor:` label, excluding buyer name |
| `vendor_name` (Invoice) | Line after `TAX INVOICE` before pipe separator |
| `total_amount_aed` | After `Total:` label, then last AED amount |
| `gr_date` | After `GR Date:` label |
| `invoice_date` | After `Invoice Date:` label |
| `due_date` | After `Due Date:` label |

**Result:** 100% field extraction completeness across 184 test documents.
The `extraction_audit` table tracks exactly which fields used the fallback.

**Important fix applied:** Both `ai_extract` and regex fallbacks can
return buyer name (Aurelius Corporation LLC) as the vendor on some
documents. All vendor UDFs explicitly exclude the buyer name.

**Date casting strategy:** Dates are kept as strings through all fallback
calls, then cast to `date` type after all fallbacks complete. This
ensures regex-returned date strings are properly converted.

---

## Layer 4 — Gold DLT

**Notebook:** `notebooks/04_gold_reconciliation.py`
**Target schema:** `<catalog>.gold`

### Three-Way Join

```
silver.invoice
    JOIN silver.purchase_order ON po_number (left)
    JOIN silver.good_receipt   ON po_number (left)
```

### Column Build Order (dependency-strict)

```
1. po_found                  ← from PO join
2. gr_found                  ← from GR join
3. vendor_similarity_score   ← vendor_similarity() UDF
4. line_qty_check            ← compare_line_item_qty() UDF
5. is_duplicate              ← row_number() window over po_number
6. reason_code               ← determine_reason_code() UDF
7. match_status              ← determine_match_status() UDF
8. amount_deviation_pct      ← arithmetic
9. final .select()
```

Column ordering is strict — Spark resolves column names at plan
compilation time. Any column referenced before it is created causes
`UNRESOLVED_COLUMN` errors.

### Vendor Similarity

```python
SequenceMatcher(normalise(name1), normalise(name2)).ratio()
```

Normalisation removes LLC/Ltd/FZ suffixes before comparison.
Returns `None` (not 0.0) when either name is missing — prevents
false EXC_VENDOR flags when extraction missed the vendor name.

### Duplicate Detection

Uses `row_number()` window partitioned by `po_number` ordered by
`invoice_number`. Only rank > 1 is flagged as duplicate. This preserves
the original invoice as APPROVED — earlier approach partitioned by
`(po_number, amount)` which incorrectly flagged originals when
extracted amounts varied slightly between original and duplicate.

### Output Tables

| Table | Contents |
|---|---|
| `reconciliation_results` | All 64 invoices with reason codes |
| `approved_matches` | APPROVED invoices ready for payment |
| `exception_queue` | EXCEPTION invoices for human review |
| `audit_log` | Immutable decision trail |

---

## Layer 5 — Evaluation Harness

**Notebook:** `notebooks/05_eval_harness.py`

Joins Gold results against `ground_truth.csv` (64 labelled rows) and
computes:

- Overall accuracy, precision, recall, F1 (weighted)
- Per-scenario accuracy across 8 scenarios
- Confusion matrix (APPROVED vs EXCEPTION)
- Wrong prediction details with diagnostic columns

All metrics logged to MLflow experiment `/p2p_procurement/eval_harness`.
Artifacts: confusion matrix CSV, full eval results, scenario summary,
wrong predictions.

**Run this after every pipeline change** to detect regressions.

---

## Data Flow Diagram

```
Azure Blob                UC Volume              Bronze Delta
─────────────             ──────────             ────────────
purchase_order/ ──sync──► /Volumes/              purchase_order
good_receipt/   ──sync──► .../staging/           good_receipt
invoice/        ──sync──► .../p2p_files/         invoice
                                                 quarantine

Bronze Delta              Silver Delta           Gold Delta
────────────              ────────────           ──────────
purchase_order ──extract─► purchase_order ──┐
good_receipt   ──extract─► good_receipt   ──┼──► reconciliation_results
invoice        ──extract─► invoice        ──┘    approved_matches
                           extraction_audit       exception_queue
                                                  audit_log

Gold Delta                MLflow
──────────                ──────
reconciliation  ──eval──► experiments/
results                   p2p_procurement/
ground_truth.csv          eval_harness/
                          → accuracy metrics
                          → confusion matrix
                          → scenario breakdown
```

---

## Known Gaps (Production Roadmap)

1. **Auto Loader + Event Grid** — replace scheduled Volume sync with
   event-driven ingestion for sub-second latency
2. **PII masking** — Presidio layer between Bronze and LLM call before
   sending to external endpoints
3. **MLflow Prompt Registry** — version the ai_extract field lists
   so prompt changes are tracked and reversible
4. **LLMOps observability** — token cost, latency, and accuracy drift
   dashboard in Databricks SQL
5. **Agentic exception resolution** — Exception Agent that investigates
   EXC_* rows and auto-resolves where root cause is deterministic
6. **HITL review app** — Streamlit/Databricks App for AP clerk to
   approve/reject/edit exceptions with corrections feeding back
7. **Arabic/bilingual support** — UAE deployment requires RTL text
   handling and bilingual field extraction validation
