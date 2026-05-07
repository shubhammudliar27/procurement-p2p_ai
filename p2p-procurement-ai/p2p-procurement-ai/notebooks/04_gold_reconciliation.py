import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ── Config ──────────────────────────────────────────────────────────────────
CATALOG_NAME = spark.conf.get("catalog_name", "p2p_databricks")

SILVER_PO  = f"{CATALOG_NAME}.silver.purchase_order"
SILVER_GR  = f"{CATALOG_NAME}.silver.good_receipt"
SILVER_INV = f"{CATALOG_NAME}.silver.invoice"


# ══════════════════════════════════════════════════════════════════════════════
# UDFs
# ══════════════════════════════════════════════════════════════════════════════

@F.udf(returnType=T.DoubleType())
def vendor_similarity(name1: str, name2: str) -> float:
    """
    Vendor name similarity via SequenceMatcher.
    Returns None (not 0.0) when either name missing —
    None = unverified, not a mismatch.
    """
    if not name1 or not name2:
        return None
    from difflib import SequenceMatcher
    import re
    def normalise(s):
        s = s.lower().strip()
        s = re.sub(r'\b(llc|fz llc|ltd|limited|fz|co\.?)\b', '', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s
    return round(
        SequenceMatcher(None, normalise(name1), normalise(name2)).ratio(), 4
    )


@F.udf(returnType=T.StringType())
def compare_line_item_qty(inv_line_items: str, gr_line_items: str) -> str:
    """
    Compare quantities between invoice and GR line items.
    QTY_MISMATCH / QTY_MATCH / UNKNOWN
    """
    if not inv_line_items or not gr_line_items:
        return "UNKNOWN"
    import re
    try:
        inv_qtys = [int(x) for x in re.findall(r'\b(\d+)\b', str(inv_line_items))
                    if 1 <= int(x) <= 1000]
        gr_qtys  = [int(x) for x in re.findall(r'\b(\d+)\b', str(gr_line_items))
                    if 1 <= int(x) <= 1000]
        if not inv_qtys or not gr_qtys:
            return "UNKNOWN"
        if max(inv_qtys) > max(gr_qtys):
            return "QTY_MISMATCH"
        return "QTY_MATCH"
    except Exception:
        return "UNKNOWN"


@F.udf(returnType=T.StringType())
def determine_reason_code(
    po_found:       bool,
    gr_found:       bool,
    inv_amount:     float,
    po_amount:      float,
    line_qty_check: str,
    inv_qty:        str,
    vendor_sim:     float,
    inv_date:       str,
    gr_date:        str,
    is_duplicate:   bool,
    gr_status:      str
) -> str:
    """
    5-path reconciliation engine:
    1. Structural  (duplicate, missing PO/GR)
    2. Fraud date  (invoice before GR)
    3. Vendor      (only when both names present)
    4. Qty vs Price discrimination
    5. Match quality tier
    """
    # Path 1: Structural
    if is_duplicate:
        return "EXC_DUPLICATE"
    if not po_found:
        return "EXC_NO_PO"
    if not gr_found:
        return "EXC_NO_GR"

    # Path 2: Date fraud signal
    try:
        if inv_date and gr_date:
            from datetime import date
            inv_d = date.fromisoformat(str(inv_date)[:10])
            gr_d  = date.fromisoformat(str(gr_date)[:10])
            if inv_d < gr_d:
                return "EXC_DATE"
    except Exception:
        pass

    # Path 3: Vendor identity
    if vendor_sim is not None and vendor_sim < 0.6:
        return "EXC_VENDOR"

    # Path 4: Qty vs Price discrimination
    if inv_amount and po_amount and po_amount > 0:
        deviation = (inv_amount - po_amount) / po_amount
        if deviation > 0.05:
            if line_qty_check == "QTY_MISMATCH":
                return "EXC_QTY_MISMATCH"
            return "EXC_PRICE"

    # Path 5: Match quality tier
    if vendor_sim is not None and vendor_sim < 0.95:
        return "MATCH_FUZZY"

    if inv_amount and po_amount and po_amount > 0:
        deviation = abs(inv_amount - po_amount) / po_amount
        if deviation > 0.001:
            return "MATCH_TOLERANCE"

    return "MATCH_EXACT"


@F.udf(returnType=T.StringType())
def determine_match_status(reason_code: str) -> str:
    if reason_code is None:
        return "UNKNOWN"
    if reason_code.startswith("MATCH_"):
        return "APPROVED"
    if reason_code.startswith("EXC_"):
        return "EXCEPTION"
    return "UNKNOWN"


# ══════════════════════════════════════════════════════════════════════════════
# GOLD VIEW — THREE-WAY MATCH
# ══════════════════════════════════════════════════════════════════════════════

@dlt.view(name="raw_gold_reconciliation")
def raw_gold_reconciliation():
    inv = spark.table(SILVER_INV)
    po  = spark.table(SILVER_PO)
    gr  = spark.table(SILVER_GR)

    # Step 1: Invoice → PO join
    inv_po = (
        inv.join(
            po.select(
                F.col("po_number"),
                F.col("vendor_name").alias("po_vendor_name"),
                F.col("total_amount_aed").alias("po_total_amount"),
                F.col("po_date"),
                F.col("delivery_date"),
                F.col("payment_terms"),
            ),
            on="po_number",
            how="left"
        )
        .withColumn("po_found", F.col("po_vendor_name").isNotNull())
    )

    # Step 2: → GR join
    inv_po_gr = (
        inv_po.join(
            gr.select(
                F.col("po_number"),
                F.col("gr_number"),
                F.col("gr_date"),
                F.col("gr_status"),
                F.col("vendor_name").alias("gr_vendor_name"),
                F.col("line_items").alias("gr_line_items"),
            ),
            on="po_number",
            how="left"
        )
        .withColumn("gr_found", F.col("gr_number").isNotNull())
    )

    # Step 3: Vendor similarity — must exist before determine_reason_code
    inv_po_gr = inv_po_gr.withColumn(
        "vendor_similarity_score",
        vendor_similarity(F.col("vendor_name"), F.col("po_vendor_name"))
    )

    # Step 4: Line qty check — must exist before determine_reason_code
    inv_po_gr = inv_po_gr.withColumn(
        "line_qty_check",
        compare_line_item_qty(F.col("line_items"), F.col("gr_line_items"))
    )

    # Step 5: Duplicate detection
    # Partition by po_number ONLY (not amount) so DUP invoices are always
    # caught regardless of extracted amount differences.
    # Rank 1 = original (APPROVED), rank > 1 = duplicate (EXCEPTION).
    dup_window   = Window.partitionBy("po_number").orderBy("invoice_number")
    count_window = Window.partitionBy("po_number")

    inv_po_gr = (
        inv_po_gr
        .withColumn("_inv_rank",  F.row_number().over(dup_window))
        .withColumn("_inv_count", F.count("invoice_number").over(count_window))
        .withColumn("is_duplicate",
                    (F.col("_inv_count") > 1) & (F.col("_inv_rank") > 1))
        .drop("_inv_rank", "_inv_count")
    )

    # Step 6: Reconciliation logic
    # All dependent columns exist before this call
    inv_po_gr = inv_po_gr.withColumn(
        "reason_code",
        determine_reason_code(
            F.col("po_found"),
            F.col("gr_found"),
            F.col("total_amount_aed"),
            F.col("po_total_amount"),
            F.col("line_qty_check"),
            F.lit(None).cast("string"),
            F.col("vendor_similarity_score"),
            F.col("invoice_date").cast("string"),
            F.col("gr_date").cast("string"),
            F.col("is_duplicate"),
            F.col("gr_status")
        )
    )

    # Step 7: Derived columns — depend on reason_code
    inv_po_gr = (
        inv_po_gr
        .withColumn("match_status",
            determine_match_status(F.col("reason_code"))
        )
        .withColumn("amount_deviation_pct",
            F.when(
                F.col("po_total_amount").isNotNull() &
                (F.col("po_total_amount") > 0),
                F.round(
                    (F.col("total_amount_aed") - F.col("po_total_amount")) /
                    F.col("po_total_amount") * 100, 2
                )
            ).otherwise(F.lit(None))
        )
        .withColumn("_reconciliation_timestamp", F.current_timestamp())
    )

    # Step 8: Final column selection
    return inv_po_gr.select(
        F.col("invoice_number"),
        F.col("invoice_date"),
        F.col("due_date"),
        F.col("po_number"),
        F.col("gr_number"),
        F.col("total_amount_aed").alias("invoice_amount"),
        F.col("po_total_amount"),
        F.col("amount_deviation_pct"),
        F.col("vendor_name").alias("invoice_vendor"),
        F.col("po_vendor_name"),
        F.col("vendor_similarity_score"),
        F.col("gr_date"),
        F.col("gr_status"),
        F.col("line_qty_check"),
        F.col("reason_code"),
        F.col("match_status"),
        F.col("po_found"),
        F.col("gr_found"),
        F.col("is_duplicate"),
        F.col("_reconciliation_timestamp"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLES
# ══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="reconciliation_results",
    comment="Gold: all invoices with reconciliation outcome and reason codes",
    table_properties={"quality": "gold"}
)
def gold_reconciliation_results():
    return dlt.read("raw_gold_reconciliation")


@dlt.table(
    name="approved_matches",
    comment="Gold: invoices approved for payment",
    table_properties={"quality": "gold"}
)
def gold_approved():
    return (
        dlt.read("raw_gold_reconciliation")
           .filter(F.col("match_status") == "APPROVED")
    )


@dlt.table(
    name="exception_queue",
    comment="Gold: invoices requiring human review",
    table_properties={"quality": "gold"}
)
def gold_exceptions():
    return (
        dlt.read("raw_gold_reconciliation")
           .filter(F.col("match_status") == "EXCEPTION")
    )


@dlt.table(
    name="audit_log",
    comment="Gold: immutable audit trail — every reconciliation decision",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_audit_log():
    return (
        dlt.read("raw_gold_reconciliation")
           .select(
               F.col("invoice_number"),
               F.col("po_number"),
               F.col("gr_number"),
               F.col("invoice_amount"),
               F.col("po_total_amount"),
               F.col("amount_deviation_pct"),
               F.col("invoice_vendor"),
               F.col("po_vendor_name"),
               F.col("vendor_similarity_score"),
               F.col("reason_code"),
               F.col("match_status"),
               F.col("is_duplicate"),
               F.col("_reconciliation_timestamp").alias("decided_at"),
               F.lit("system").alias("decided_by"),
               F.lit("auto").alias("decision_method")
           )
    )
