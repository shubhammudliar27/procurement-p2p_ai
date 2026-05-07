# Databricks notebook source
# =============================================================================
# 05_eval_harness.py
# Evaluates Gold reconciliation results against golden test set
# Logs accuracy metrics, confusion matrix, and per-scenario breakdown to MLflow
# Run after every pipeline change to detect regressions
# =============================================================================

# COMMAND ----------
# %pip install scikit-learn tabulate mlflow --quiet

# COMMAND ----------

import mlflow
import pandas as pd
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    precision_recall_fscore_support,
    accuracy_score
)
from pyspark.sql import functions as F
from tabulate import tabulate
from datetime import datetime

CATALOG_NAME      = "p2p_databricks"
GROUND_TRUTH_PATH = f"/Volumes/{CATALOG_NAME}/staging/p2p_files/ground_truth.csv"
MLFLOW_EXPERIMENT = "/p2p_procurement/eval_harness"

print("✅ Imports ready")

# COMMAND ----------
# ── Load ground truth ──────────────────────────────────────────────────────

gt = spark.read.csv(GROUND_TRUTH_PATH, header=True, inferSchema=True)
print(f"Ground truth rows: {gt.count()}")

display(
    gt.groupBy("scenario")
      .count()
      .orderBy("scenario")
)

# COMMAND ----------
# ── Load Gold and join ─────────────────────────────────────────────────────

gold = spark.table(f"{CATALOG_NAME}.gold.reconciliation_results")
print(f"Gold result rows: {gold.count()}")

eval_df = (
    gt.join(
        gold.select(
            F.col("invoice_number"),
            F.col("po_number").alias("gold_po_number"),
            F.col("reason_code").alias("predicted_reason_code"),
            F.col("match_status").alias("predicted_match_status"),
            F.col("amount_deviation_pct"),
            F.col("vendor_similarity_score"),
            F.col("is_duplicate"),
            F.col("line_qty_check"),
        ),
        on="invoice_number",
        how="left"
    )
    .withColumn("expected_match_status",
        F.when(F.col("expected_overall_match") == True,
               F.lit("APPROVED"))
         .otherwise(F.lit("EXCEPTION"))
    )
    .withColumn("prediction_correct",
        F.col("predicted_match_status") == F.col("expected_match_status")
    )
)

print(f"Joined rows       : {eval_df.count()}")
print(f"Unmatched by Gold : "
      f"{eval_df.filter(F.col('predicted_reason_code').isNull()).count()}")

# COMMAND ----------
# ── Compute accuracy ───────────────────────────────────────────────────────

pdf         = eval_df.toPandas()
missed      = pdf[pdf["predicted_reason_code"].isna()]
pdf_matched = pdf[pdf["predicted_reason_code"].notna()].copy()

print(f"Total ground truth : {len(pdf)}")
print(f"Matched by pipeline: {len(pdf_matched)}")
print(f"Missed             : {len(missed)}")

y_true           = pdf_matched["expected_match_status"].tolist()
y_pred           = pdf_matched["predicted_match_status"].tolist()
overall_accuracy = accuracy_score(y_true, y_pred)

print(f"\nOverall accuracy: {overall_accuracy*100:.1f}%")

# COMMAND ----------
# ── Classification report and confusion matrix ─────────────────────────────

print("Classification Report:")
print("─" * 60)
print(classification_report(y_true, y_pred,
                             target_names=["APPROVED", "EXCEPTION"]))

cm = confusion_matrix(y_true, y_pred, labels=["APPROVED", "EXCEPTION"])
print("Confusion Matrix:")
print(tabulate(
    pd.DataFrame(cm,
                 index=["Actual APPROVED",  "Actual EXCEPTION"],
                 columns=["Pred APPROVED",  "Pred EXCEPTION"]),
    headers="keys", tablefmt="rounded_outline"
))

# COMMAND ----------
# ── Per-scenario breakdown ─────────────────────────────────────────────────

print("\nAccuracy per scenario:")
print("─" * 70)

scenario_results = []
for scenario in sorted(pdf_matched["scenario"].unique()):
    subset  = pdf_matched[pdf_matched["scenario"] == scenario]
    correct = subset["prediction_correct"].sum()
    total   = len(subset)
    pct     = round(correct / total * 100, 1) if total > 0 else 0
    status  = "✅" if pct == 100 else ("⚠️ " if pct >= 50 else "❌")
    scenario_results.append({
        "scenario": scenario, "correct": correct,
        "total": total, "accuracy": pct, "status": status
    })
    print(f"  {status} {scenario:45s}: {correct}/{total} ({pct}%)")

wrong = pdf_matched[~pdf_matched["prediction_correct"]]
if len(wrong) > 0:
    print(f"\n⚠️  {len(wrong)} incorrect predictions:")
    print("─" * 70)
    for _, row in wrong.iterrows():
        print(f"  {row['invoice_number']:25s} | {row['scenario']:40s}")
        print(f"    expected: {row['expected_match_status']:12s} | "
              f"got: {row['predicted_match_status']:12s} "
              f"({row['predicted_reason_code']}) | "
              f"deviation: {row['amount_deviation_pct']}%")
else:
    print("\n✅ Perfect accuracy!")

# COMMAND ----------
# ── Log to MLflow ──────────────────────────────────────────────────────────

mlflow.set_experiment(MLFLOW_EXPERIMENT)

with mlflow.start_run(
    run_name=f"eval_{datetime.now().strftime('%Y%m%d_%H%M')}"
) as run:

    mlflow.log_param("catalog",             CATALOG_NAME)
    mlflow.log_param("gold_table",          "reconciliation_results")
    mlflow.log_param("price_tolerance_pct", 5)
    mlflow.log_param("extraction_method",   "ai_extract + regex_fallback")
    mlflow.log_param("eval_timestamp",      datetime.now().isoformat())

    prec, rec, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="weighted", zero_division=0
    )
    mlflow.log_metric("overall_accuracy",   round(overall_accuracy, 4))
    mlflow.log_metric("weighted_precision", round(float(prec), 4))
    mlflow.log_metric("weighted_recall",    round(float(rec),  4))
    mlflow.log_metric("weighted_f1",        round(float(f1),   4))
    mlflow.log_metric("total_invoices",     len(pdf))
    mlflow.log_metric("pipeline_coverage",  round(len(pdf_matched)/len(pdf), 4))
    mlflow.log_metric("wrong_predictions",  len(wrong))

    for row in scenario_results:
        safe = row["scenario"].lower().replace(" ", "_")
        mlflow.log_metric(f"acc_{safe}", row["accuracy"] / 100)

    cm_df = pd.DataFrame(cm,
                         index=["Actual_APPROVED",  "Actual_EXCEPTION"],
                         columns=["Pred_APPROVED",  "Pred_EXCEPTION"])
    cm_df.to_csv("/tmp/confusion_matrix.csv")
    mlflow.log_artifact("/tmp/confusion_matrix.csv")

    pdf_matched.to_csv("/tmp/eval_results.csv", index=False)
    mlflow.log_artifact("/tmp/eval_results.csv")

    pd.DataFrame(scenario_results).to_csv("/tmp/scenario_summary.csv", index=False)
    mlflow.log_artifact("/tmp/scenario_summary.csv")

    if len(wrong) > 0:
        wrong.to_csv("/tmp/wrong_predictions.csv", index=False)
        mlflow.log_artifact("/tmp/wrong_predictions.csv")

    run_id = run.info.run_id

print(f"""
✅ Logged to MLflow
{'─'*45}
  Run ID     : {run_id}
  Experiment : {MLFLOW_EXPERIMENT}
  Accuracy   : {overall_accuracy*100:.1f}%
  F1 Score   : {f1*100:.1f}%
  Coverage   : {len(pdf_matched)/len(pdf)*100:.1f}%
  Wrong      : {len(wrong)}/64
{'─'*45}
""")
