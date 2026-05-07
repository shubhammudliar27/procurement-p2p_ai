# Procurement AI — Automated Three-Way Match

> End-to-end procurement document automation on Azure Databricks.
> PDF ingestion → AI extraction → three-way match reconciliation → audit trail.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-15.4_LTS_ML-red)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue)
![MLflow](https://img.shields.io/badge/MLflow-2.x-orange)
![Accuracy](https://img.shields.io/badge/Eval_Accuracy-95.3%25-brightgreen)

---

## Why This Exists

Manual three-way matching (PO ↔ GR ↔ Invoice) takes ~12 minutes per invoice.
At 10,000 invoices/month that is 2,000 hours of finance team time.

This pipeline targets **sub-30-second automated matching** with a structured
fallback for low-confidence and exception cases — reducing manual review to
only genuine exceptions.

---

## Architecture

```
Azure Blob Storage          Databricks Lakehouse              Serving
──────────────────    ──────────────────────────────    ─────────────────
                      ┌─────────┐                       Power BI Dashboard
 purchase_order/  ──► │ Bronze  │ raw PDF + text
 good_receipt/    ──► │ Delta   │ quality gates         Exception Alerts
 invoice/         ──► └────┬────┘ dedup hash            (Email / Teams)
                           │
                      ┌────▼────┐
                      │ Silver  │ ai_extract()
                      │ Delta   │ + regex fallback       ERP API
                      └────┬────┘ 100% completeness     (SAP/Oracle)
                           │
                      ┌────▼──────────────┐
                      │ Gold              │
                      │ Reconciliation    │
                      │ Engine            │
                      │ 5 paths           │
                      │ reason codes      │
                      │ audit log         │
                      └───────────────────┘
                           │
                      ┌────▼────┐
                      │ MLflow  │ eval harness
                      │ Eval    │ 95.3% accuracy
                      └─────────┘
```

---

## Reconciliation Engine — 5 Paths

Every invoice is evaluated in strict order:

```
Invoice + PO + GR
        │
        ├─► EXC_DUPLICATE      duplicate submission detected
        ├─► EXC_NO_PO          no matching PO found
        ├─► EXC_NO_GR          no matching GR found
        ├─► EXC_DATE           invoice date before GR date (fraud signal)
        ├─► EXC_VENDOR         vendor name mismatch (similarity < 0.6)
        ├─► EXC_QTY_MISMATCH   invoice qty exceeds GR received qty
        ├─► EXC_PRICE          amount deviation > 5% above PO
        ├─► MATCH_FUZZY        vendor fuzzy match (0.6–0.95 similarity)
        ├─► MATCH_TOLERANCE    amount within ±5% of PO
        └─► MATCH_EXACT        all fields align perfectly
```

---

## Evaluation Results

Evaluated against a 64-row golden test set across 8 scenarios.
All results logged to MLflow.

| Scenario | Correct | Total | Accuracy |
|---|---|---|---|
| MATCH_EXACT | 15 | 15 | 100% |
| MATCH_TOLERANCE | 9 | 10 | 90% |
| MATCH_PARTIAL_DELIVERY | 8 | 8 | 100% |
| EXC_QTY_MISMATCH | 8 | 8 | 100% |
| EXC_PRICE_MISMATCH | 4 | 6 | 67% |
| EXC_WRONG_VENDOR | 5 | 5 | 100% |
| EXC_DUPLICATE | 8 | 8 | 100% |
| EXC_GR_DATE_AFTER_INVOICE | 4 | 4 | 100% |
| **Overall** | **61** | **64** | **95.3%** |

The 3 misclassifications are documented boundary cases:
- 2 × EXC_PRICE scenarios with deviation < 5% (within tolerance window by design)
- 1 × MATCH_TOLERANCE with extracted amount variance on a multi-line document

---

## Stack

| Layer | Technology |
|---|---|
| Cloud storage | Azure Blob Storage |
| Lakehouse | Databricks + Delta Lake + Unity Catalog |
| Pipelines | Databricks Delta Live Tables (DLT) |
| AI extraction | Databricks `ai_extract()` + regex fallback |
| Vendor matching | Python `difflib.SequenceMatcher` |
| Experiment tracking | MLflow |
| PII detection | Microsoft Presidio (architecture) |
| Dataset generation | Python + ReportLab + Faker |

---

## Repository Structure

```
p2p-procurement-ai/
├── README.md
├── docs/
│   ├── architecture.md          detailed component documentation
│   └── screenshots/             pipeline run screenshots
├── data/
│   ├── generate_p2p_dataset_v2.py   synthetic dataset generator
│   └── ground_truth.csv             64-row golden eval labels
├── notebooks/
│   ├── 01_bronze_ingestion.py   Azure Blob → UC Volume sync
│   ├── 02_bronze_dlt.py         Bronze DLT pipeline
│   ├── 03_silver_dlt.py         Silver DLT — ai_extract + fallback
│   ├── 04_gold_reconciliation.py Gold — 5-path reconciliation engine
│   └── 05_eval_harness.py       MLflow eval harness
└── ingestion/
    ├── 00_azure_setup.py        one-time Azure resource setup
    └── 02_upload_dataset_to_azure.py  upload utility
```

---

## Dataset

The project uses a **synthetically generated dataset** of 184 documents
(60 POs, 60 GRs, 64 Invoices) across 8 carefully designed scenarios.

All vendor names, company names, addresses, and financial figures are
**fully fictional** — no real company data is used.

To regenerate:
```bash
pip install reportlab faker
python data/generate_p2p_dataset_v2.py
```

This produces linked PO/GR/Invoice triples with a `ground_truth.csv`
containing expected match outcomes for each triple.

---

## Production Considerations

| Concern | How it is handled |
|---|---|
| Extraction accuracy | Two-tier: ai_extract primary, regex fallback. 100% field completeness. |
| LLM failures | Regex fallback on all critical fields. Quarantine table for unrecoverable docs. |
| Deduplication | SHA-256 hash at Bronze ingestion. Window-based duplicate detection in Gold. |
| Vendor name variation | SequenceMatcher similarity with LLC/Ltd normalisation. Threshold configurable. |
| Audit trail | Every reconciliation decision written to immutable Delta audit log. |
| Data quality | DLT `@dlt.expect` gates at Bronze and Silver. Quarantine for failed rows. |
| Cost | ai_extract called once per document at Silver. Gold uses deterministic logic. |
| PII | Presidio masking layer in architecture (between Bronze and LLM call). |
| Eval drift | MLflow experiment tracks accuracy per pipeline run. Per-scenario breakdown. |

---

## Known Limitations

- **Line item qty comparison** uses heuristic parsing of `ai_extract` output.
  True qty comparison requires structured JSON line items from a fine-tuned model.
- **EXC_PRICE threshold** is hardcoded at 5%. In production this would be
  configurable per vendor contract.
- **Currency** assumes AED throughout. Multi-currency requires FX rate lookup.
- **Arabic/bilingual PDFs** not yet tested. Production UAE deployment would
  require bilingual extraction validation.

---

## What I Extended Beyond the Reference Implementation

This project started from a Databricks DLT reference architecture and was
significantly extended with:

1. **Two-tier extraction** — ai_extract primary with field-specific regex
   fallbacks. Rescued 63+ null fields that ai_extract missed.
2. **Buyer-name exclusion** — regex UDFs explicitly exclude the buyer entity
   (Aurelius Corporation) which ai_extract occasionally extracts as the vendor.
3. **Date fallbacks** — invoice_date and gr_date regex fallbacks enabling the
   EXC_DATE fraud signal to fire correctly.
4. **5-path reconciliation** — original codebase had a single LLM judge call.
   Replaced with deterministic 5-path engine. LLM cost reduced to zero.
5. **Window-based duplicate detection** — rank-based approach preserving
   originals as APPROVED. Original approach flagged originals as duplicates.
6. **Golden test set + MLflow eval** — 64-row labelled dataset across 8
   scenarios. Per-scenario accuracy tracked across pipeline runs.
7. **Audit log** — immutable Delta table recording every decision with
   reason code, confidence, and timestamp.
8. **Quarantine table** — documents failing Bronze quality gates routed to
   separate table rather than silently dropped.

---

## Author

Shubham Mudliar
Senior Technical Consultant — Data & AI
[LinkedIn](https://linkedin.com/in/your-profile)
[GitHub](https://github.com/your-username)
