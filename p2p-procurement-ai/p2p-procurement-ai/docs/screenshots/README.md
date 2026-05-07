# Screenshots

Add the following screenshots to this folder before pushing to GitHub.
Each one is referenced in the main README and architecture doc.

---

## Required Screenshots

### 1. `bronze_pipeline_dag.png`
**Where:** Databricks → Jobs & Pipelines → P2P_Bronze_Pipeline → Pipeline graph tab
**What to show:** The DAG with all 4 nodes (purchase_order, good_receipt, invoice, quarantine) showing green ✅ and row counts (60, 60, 64, 0)

### 2. `silver_pipeline_dag.png`
**Where:** Databricks → Jobs & Pipelines → P2P_Silver_Pipeline → Pipeline graph tab
**What to show:** DAG with 4 tables showing green ✅ and row counts. Expectations column showing "X met"

### 3. `gold_pipeline_dag.png`
**Where:** Databricks → Jobs & Pipelines → P2P_Gold_Pipeline → Pipeline graph tab
**What to show:** DAG with 4 Gold tables (reconciliation_results, approved_matches, exception_queue, audit_log)

### 4. `gold_reason_code_breakdown.png`
**Where:** Databricks SQL Editor or notebook output
**What to show:** Table with reason_code, match_status, count columns showing the full breakdown

### 5. `silver_extraction_completeness.png`
**Where:** Notebook output from extraction completeness verify query
**What to show:** 60/60, 60/60, 64/64 complete (100%) for all three tables

### 6. `mlflow_eval_run.png`
**Where:** Databricks → Experiments → p2p_procurement → eval_harness → click latest run
**What to show:** Metrics panel showing overall_accuracy, weighted_f1, per-scenario acc_* metrics

### 7. `mlflow_eval_metrics.png`
**Where:** Same MLflow run → Charts tab
**What to show:** Bar chart of per-scenario accuracy metrics

### 8. `unity_catalog_structure.png`
**Where:** Databricks → Catalog → p2p_databricks
**What to show:** Expanded catalog showing bronze/silver/gold schemas and their tables

### 9. `azure_blob_structure.png`
**Where:** Azure Portal → your storage account → Containers → p2p-landing
**What to show:** Three folders (purchase_order, good_receipt, invoice) with file counts

---

## How to Take Screenshots on Windows
- `Win + Shift + S` → select area → paste into Paint → Save as PNG
- Or use Snipping Tool

## Naming Convention
Use exactly the filenames listed above — they will be referenced
in the README img tags once you add them.
