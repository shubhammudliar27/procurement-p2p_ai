# Procurement AI — Automated Three-Way Match

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-15.4_LTS_ML-red)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue)
![MLflow](https://img.shields.io/badge/MLflow-2.x-orange)
![Pipeline Accuracy](https://img.shields.io/badge/Pipeline_Accuracy-95.3%25-brightgreen)
![Agent Accuracy](https://img.shields.io/badge/Agent_Accuracy-96.2%25-brightgreen)
![Extraction](https://img.shields.io/badge/Extraction_Completeness-100%25-blue)

---

## Problem Statement

Every time a company buys something, three documents are created:

- **Purchase Order (PO)** — the company says *"we want to buy X units of Y from vendor Z at price P"*
- **Goods Receipt (GR)** — the warehouse says *"we received X units of Y from vendor Z on date D"*
- **Invoice** — the vendor says *"please pay us currency X for Y units delivered"*

Before paying an invoice, the finance team must verify that all three documents
agree. This is called **three-way matching**:

> Does the invoice match what was ordered (PO)?
> Does it match what was actually received (GR)?

### Why This Is a Massive Problem in Practice

In most organisations today, this process is entirely manual:

1. An AP (Accounts Payable) associate receives a PDF invoice by email
2. They open the ERP system and search for the matching PO number
3. They open the warehouse system and find the corresponding GR
4. They compare quantities, prices, vendor names, and dates across all three
5. If everything matches → approve for payment
6. If anything is off → email the vendor, wait, re-check

**This takes an average of 12 minutes per invoice.**

At a mid-size organisation processing 10,000 invoices/month:
- **2,000 hours of finance team time every month**
- **24,000 hours per year** — equivalent to 12 full-time employees doing nothing but matching documents
- Human errors causing duplicate payments, overpayments, and fraud that goes undetected

Real consequences of manual matching failures:

| Failure type | Example | Financial impact |
|---|---|---|
| Duplicate invoice | Same invoice submitted twice | Double payment |
| Price inflation | Invoice 15% above agreed PO price | Direct overpayment |
| Quantity fraud | Invoiced for 50 units, received 40 | 25% overpayment |
| Wrong vendor | Invoice from subsidiary, PO to parent | Potential fraud |
| Early invoicing | Invoice before goods arrive | Payment for nothing |

### What This System Does

This project automates the entire three-way match process:

- PDFs are processed automatically the moment they land in cloud storage
- AI extracts structured data from unstructured PDF documents — no manual entry
- A 5-path reconciliation engine checks every invoice against its PO and GR
- An AI agent investigates every exception before escalating to humans
- Only genuine exceptions reach the AP team — pre-investigated with context
- Every decision is logged in an immutable audit trail for compliance

**Result: 30-second processing per invoice. Human review only for genuine exceptions.**

---

## How It Works — Step by Step

```
Step 1 — Document arrives
  Vendor uploads or emails an invoice PDF
  System detects it automatically within seconds (Azure Blob Storage)

Step 2 — Reading the document (Bronze layer)
  Raw PDF is stored and text is extracted
  SHA-256 hash prevents duplicate processing
  Quality gates reject corrupted or empty files

Step 3 — Understanding the document (Silver layer)
  AI reads the text and extracts key fields:
    invoice number, vendor name, line items, amounts, dates
  If the AI misses a field, a backup regex system fills the gap
  Result: 100% of fields extracted on all 184 test documents

Step 4 — Three-way check (Gold layer)
  System joins the invoice to its matching PO and GR
  Checks across various paths:
    ✅ Exact match     → approve immediately
    ✅ Tolerance match → approve (within 5% price variation)
    ✅ Fuzzy match     → approve (vendor name variation resolved)
    ⚠️  Price mismatch → raise exception
    ⚠️  Qty mismatch   → raise exception
    ⚠️  Wrong vendor   → raise exception
    🚨 Date fraud      → raise critical exception
    🚨 Duplicate       → raise critical exception

Step 5 — Exception investigation (AI Agent)
  For every exception, an AI agent investigates before alerting humans:
  "Is there a second delivery GR that covers the quantity gap?"
  "Is this vendor name a known alias of the PO vendor?"
  "Has the original duplicate invoice already been paid?"
  Agent makes RESOLVE or ESCALATE decision with reasoning

Step 6 — Human notification (Communication Agent)
  Escalated exceptions are sent to the RIGHT person via Microsoft Teams
  🔴 Finance Controller + Legal  ← date fraud, duplicate fraud
  🟠 Procurement Team + AP       ← vendor mismatches
  🟡 AP Manager                  ← price and quantity issues
  Each alert has the agent's findings — not just an error code
```

---

## Results at a Glance

| Metric | Result |
|---|---|
| Documents processed | 184 (60 POs, 60 GRs, 64 Invoices) |
| Field extraction completeness | **100%** |
| Pipeline accuracy (golden test set) | **95.3%** (61/64) |
| Agent accuracy (exception classification) | **96.2%** (25/26) |
| Average agent confidence | 84.2% |
| Processing time per invoice | < 30 seconds |
| Test scenarios | 8 designed scenarios |

---

## Full System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SOURCE DOCUMENTS                               │
│     Purchase Order PDFs  │  Goods Receipt PDFs  │  Invoice PDFs    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼  Azure Blob Storage
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Delta Lake)                       │
│  PDF binary storage  →  text extraction (pdfminer + pypdf)         │
│  SHA-256 hash dedup  │  quality gates  │  quarantine table         │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Delta Lake)                       │
│  Tier 1: Databricks ai_extract()  →  structured fields             │
│  Tier 2: Regex fallback  →  rescues null fields (63+ rescued)      │
│  Buyer-name exclusion  │  date cast after fallback                  │
│  100% field completeness  │  tracked in extraction_audit           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER  (Delta Lake)                        │
│                                                                     │
│  Three-way join: Invoice ←→ PO ←→ GR  (on po_number)              │
│                                                                     │
│  5-path reconciliation engine:                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  EXC_DUPLICATE      duplicate submission detected           │   │
│  │  EXC_DATE           invoice before GR (fraud signal)       │   │
│  │  EXC_VENDOR         vendor mismatch (similarity < 0.6)     │   │
│  │  EXC_QTY_MISMATCH   invoice qty > GR received qty          │   │
│  │  EXC_PRICE          amount deviation > 5% above PO         │   │
│  │  MATCH_FUZZY        vendor fuzzy match (0.6-0.95)          │   │
│  │  MATCH_TOLERANCE    amount within ±5%                       │   │
│  │  MATCH_EXACT        all fields align                        │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  Output: reconciliation_results │ approved_matches                  │
│          exception_queue        │ audit_log                         │
└────────────┬──────────────────────────────┬────────────────────────┘
             │                              │
             ▼                              ▼
┌────────────────────┐         ┌────────────────────────────────────┐
│   EVALUATION       │         │          AGENTIC LAYER             │
│                    │         │                                    │
│  MLflow harness    │         │  ┌──────────────────────────────┐  │
│  64-row golden set │         │  │     Exception Agent           │  │
│  8 scenarios       │         │  │                              │  │
│  95.3% accuracy    │         │  │  Tools:                      │  │
│                    │         │  │  • get_invoice_context        │  │
│  VS Comparison:    │         │  │  • check_additional_gr        │  │
│  SequenceMatcher   │         │  │  • check_vendor_aliases       │  │
│  95.3% ← winner   │         │  │  • check_duplicate_status     │  │
│  BGE Vector Search │         │  │  • check_price_context        │  │
│  85.9%             │         │  │                              │  │
└────────────────────┘         │  │  LLM: Llama 3.3 70B          │  │
                               │  │  Accuracy: 96.2%             │  │
                               │  └──────────────┬───────────────┘  │
                               │                 │                   │
                               │                 ▼                   │
                               │  ┌──────────────────────────────┐  │
                               │  │   Communication Agent         │  │
                               │  │                              │  │
                               │  │  🔴 EXC_DATE/DUPLICATE        │  │
                               │  │     → Finance + Legal         │  │
                               │  │  🟠 EXC_VENDOR                │  │
                               │  │     → Procurement + AP        │  │
                               │  │  🟡 EXC_PRICE/QTY             │  │
                               │  │     → AP Manager              │  │
                               │  │                              │  │
                               │  │  Microsoft Teams             │  │
                               │  │  Adaptive Cards              │  │
                               │  └──────────────────────────────┘  │
                               └────────────────────────────────────┘
```

---

## Exception Agent — How It Investigates

For every exception the agent follows a structured investigation:

### EXC_QTY_MISMATCH — Invoice quantity exceeds GR received

```
Agent calls: check_additional_gr(po_number, known_gr)

Scenario A — second GR found:
  "A second goods receipt GR-2025-0017B exists showing 5 additional
   units received. Combined GRs cover the invoice quantity."
  → RESOLVED  (partial delivery covered)

Scenario B — no second GR:
  "No additional GR found for PO-2025-0017. Invoice claims 15 units
   but only 10 were received. Genuine overbilling."
  → ESCALATE to AP Manager + Warehouse Team
```

### EXC_WRONG_VENDOR — Vendor on invoice differs from PO

```
Agent calls: check_vendor_aliases(invoice_vendor, po_vendor)

Scenario A — known alias found (similarity > 0.85):
  "Invoice vendor 'Zephyr LLC' is a known alias of PO vendor
   'Zephyr Trading LLC' with 91% similarity."
  → RESOLVED  (same entity, different legal name)

Scenario B — genuinely different vendor:
  "Invoice vendor 'Meridian Commerce FZ LLC' does not match PO vendor
   'Zephyr Trading LLC'. No alias found. Potential fraud."
  → ESCALATE to Procurement Team + AP Manager
```

### EXC_DUPLICATE — Same invoice submitted twice

```
Agent calls: check_duplicate_status(po_number, invoice_number)

Scenario A — original NOT yet paid:
  "Original invoice INV-2025-0053 has not been approved for payment.
   This duplicate is likely a re-submission."
  → RESOLVED  (safe to process original, block duplicate)

Scenario B — original already paid:
  "Original invoice INV-2025-0053 was already approved and paid.
   This is a duplicate payment attempt."
  → ESCALATE to AP Manager + Finance Controller (HIGH RISK)
```

### EXC_DATE — Invoice date before GR date

```
No tool call needed.
Always ESCALATE — this is a fraud signal (confidence: 1.0)
"Vendor invoiced on 2025-10-06 but goods were not received
 until 2025-10-18. Invoice predates delivery by 12 days."
→ ESCALATE to Finance Controller + Legal (CRITICAL)
```

---

## Method Comparison — SequenceMatcher vs Vector Search

| Method | Approach | Accuracy | Cost |
|---|---|---|---|
| **A — chosen** | Exact join + SequenceMatcher | **95.3%** | $0 |
| B | Exact join + BGE-large-en Vector Search | 85.9% | ~$0.001/invoice |

Method A wins. VS performed worse because:
1. False positives on borderline similarity scores (score 0.768 incorrectly rejected)
2. False negatives on wrong-vendor fraud — VS finds semantically similar POs from the same vendor regardless of PO identity, missing 5 fraud cases

VS retained in architecture for: po_number extraction failure fallback and near-duplicate invoice detection.

---

## Evaluation Results

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

---

## Stack

| Component | Technology |
|---|---|
| Cloud storage | Azure Blob Storage |
| Lakehouse | Databricks + Delta Lake + Unity Catalog |
| Pipelines | Databricks Delta Live Tables (DLT) |
| AI extraction | Databricks `ai_extract()` + regex fallback |
| Vendor matching | Python `difflib.SequenceMatcher` |
| Vector Search | Databricks Vector Search + BGE-large-en |
| Exception Agent | Databricks `ai_query()` + Llama 3.3 70B |
| Communication | Microsoft Teams Adaptive Cards |
| Experiment tracking | MLflow |
| Dataset generation | Python + ReportLab + Faker |

---

## Repository Structure

```
p2p-procurement-ai/
├── README.md
├── requirements.txt
├── data/
│   ├── generate_p2p_dataset_v2.py
│   └── ground_truth.csv
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_bronze_dlt.py
│   ├── 03_silver_dlt.py
│   ├── 04_gold_reconciliation.py
│   ├── 05_eval_harness.py
│   ├── 06_vector_search_setup.py
│   ├── 07_vs_comparison.py
│   ├── 08_exception_agent.py
│   └── 09_communication_agent.py
├── ingestion/
│   ├── 00_azure_setup.py
│   └── 02_upload_dataset_to_azure.py
├── docs/
│   ├── architecture.md
│   └── screenshots/
└── assets/
    └── architecture.mermaid
```

---

## Production Considerations

| Concern | How it is handled |
|---|---|
| Extraction accuracy | Two-tier extraction — 100% field completeness |
| LLM failures | Regex fallback on all critical fields |
| Deduplication | SHA-256 hash at Bronze + window-based detection in Gold |
| Vendor name variation | SequenceMatcher with normalisation |
| Audit trail | Immutable Delta audit tables for pipeline and agent |
| Data quality | DLT expect gates + quarantine table |
| Exception investigation | 5-tool agent checks root cause before escalating |
| Human routing | Priority tiers route to right team |
| Eval drift | MLflow tracks accuracy per pipeline run |

---

## Known Limitations

- Line item qty comparison uses heuristic parsing — true comparison requires structured JSON
- EXC_PRICE threshold hardcoded at 5% — production would be per-vendor-contract configurable
- Currency assumes AED — multi-currency requires FX rate lookup
- Arabic/bilingual PDFs not tested — UAE production requires bilingual validation
- Auto-resolve rate 0% on this dataset (all exceptions genuine) — expected 25-35% in production

---

## What Was Extended Beyond the Reference Implementation

1. Two-tier extraction with field-specific regex fallbacks (63+ null fields rescued)
2. Buyer-name exclusion preventing mis-extraction of buyer as vendor
3. Date fallbacks enabling EXC_DATE fraud signal to fire correctly
4. 5-path deterministic reconciliation engine (replaced single LLM judge — Gold LLM cost = $0)
5. Window-based duplicate detection preserving originals as APPROVED
6. 64-row golden test set with 8 scenarios and MLflow eval harness
7. Rigorous SequenceMatcher vs Vector Search method comparison
8. Exception Agent with 5 investigation tools and 96.2% accuracy
9. Communication Agent with priority-based Teams routing
10. Immutable audit log covering every pipeline and agent decision

---

## Author

**Shubham Mudliar**
Senior Technical Consultant — Data & AI | 8+ years enterprise data engineering
Microsoft Fabric Data Engineer Associate (DP-700)

[LinkedIn](https://www.linkedin.com/in/shubham-mudliar) | [GitHub](https://github.com/shubhammudliar27)
