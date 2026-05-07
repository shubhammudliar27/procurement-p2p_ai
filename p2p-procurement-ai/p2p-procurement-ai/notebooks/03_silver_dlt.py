import dlt
import re
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── Config ─────────────────────────────────────────────────────────────────
CATALOG_NAME = spark.conf.get("catalog_name", "p2p_databricks")

BRONZE_PO  = f"{CATALOG_NAME}.bronze.purchase_order"
BRONZE_GR  = f"{CATALOG_NAME}.bronze.good_receipt"
BRONZE_INV = f"{CATALOG_NAME}.bronze.invoice"


# ══════════════════════════════════════════════════════════════════════════════
# REGEX FALLBACK UDFs
# ══════════════════════════════════════════════════════════════════════════════

@F.udf(returnType=T.StringType())
def regex_po_number(text: str) -> str:
    if not text:
        return None
    m = re.search(r'PO[-\s]?\d{4}[-\s]\d{4}', text)
    return m.group(0).replace(" ", "-") if m else None


@F.udf(returnType=T.StringType())
def regex_gr_number(text: str) -> str:
    if not text:
        return None
    m = re.search(r'GR[-\s]?\d{4}[-\s]\d{4}', text)
    return m.group(0).replace(" ", "-") if m else None


@F.udf(returnType=T.StringType())
def regex_invoice_number(text: str) -> str:
    if not text:
        return None
    m = re.search(r'INV[-\s]?\d{4}[-\s]\d{4}(?:-DUP)?', text)
    return m.group(0).replace(" ", "-") if m else None


@F.udf(returnType=T.StringType())
def regex_vendor_name(text: str) -> str:
    """
    Extract vendor name from PO — appears after 'Vendor:' label.
    FIX: explicitly excludes buyer name (Aurelius Corporation LLC)
    which ai_extract sometimes picks up instead of the real vendor.
    """
    if not text:
        return None

    # Strategy 1: explicit Vendor: label — most reliable for POs
    m = re.search(
        r'Vendor:\s*([\w][\w\s,\.]+?(?:LLC|FZ LLC|Ltd|Limited))',
        text, re.IGNORECASE
    )
    if m:
        candidate = m.group(1).strip()
        if 'Aurelius' not in candidate:
            return candidate

    # Strategy 2: any company name that isn't the buyer
    for line in text.split('\n')[:20]:
        m = re.search(
            r'([\w][\w\s,\.]+?(?:LLC|FZ LLC|Ltd|Limited))',
            line, re.IGNORECASE
        )
        if m:
            candidate = m.group(1).strip()
            if 'Aurelius' not in candidate and len(candidate) > 5:
                return candidate
    return None


@F.udf(returnType=T.StringType())
def regex_invoice_vendor(text: str) -> str:
    """
    Extract vendor name from Invoice header.
    Invoice layout:
        TAX INVOICE
        <Vendor Name>  |  <Address>  |  TRN: ...
    FIX: excludes buyer name (Aurelius) which appears later in Bill To section.
    """
    if not text:
        return None

    lines = [l.strip() for l in text.replace('\r', '').split('\n') if l.strip()]

    # Strategy 1: line after TAX INVOICE header with pipe separator
    for i, line in enumerate(lines):
        if 'TAX INVOICE' in line.upper():
            for candidate_line in lines[i + 1: i + 4]:
                if '|' in candidate_line:
                    vendor = candidate_line.split('|')[0].strip()
                    if len(vendor) > 4 and 'Aurelius' not in vendor:
                        return vendor
            break

    # Strategy 2: any pipe-separated line with company suffix
    for line in lines[:15]:
        if '|' in line:
            before_pipe = line.split('|')[0].strip()
            if (re.search(r'\b(LLC|FZ LLC|Ltd|Limited)\b',
                          before_pipe, re.IGNORECASE)
                    and 'Aurelius' not in before_pipe):
                return before_pipe

    # Strategy 3: any line with company suffix, excluding buyer
    for line in lines[:15]:
        m = re.search(
            r'([\w][\w\s,\.]+?(?:LLC|FZ LLC|Ltd|Limited))',
            line, re.IGNORECASE
        )
        if m:
            candidate = m.group(1).strip()
            if 'Aurelius' not in candidate:
                return candidate
    return None


@F.udf(returnType=T.DoubleType())
def regex_total_amount(text: str) -> float:
    """Extract total AED amount — last AED amount in the document."""
    if not text:
        return None
    matches = re.findall(r'AED\s*([\d,]+\.?\d*)', text)
    if matches:
        try:
            return float(matches[-1].replace(",", ""))
        except Exception:
            return None
    return None


@F.udf(returnType=T.StringType())
def regex_gr_status(text: str) -> str:
    if not text:
        return None
    m = re.search(r'GR Status:\s*(RECEIVED|PARTIAL|PENDING)',
                  text, re.IGNORECASE)
    return m.group(1).upper() if m else None


@F.udf(returnType=T.StringType())
def regex_gr_date(text: str) -> str:
    """Extract GR date — appears after 'GR Date:' label."""
    if not text:
        return None
    m = re.search(r'GR\s*Date[:\s]+(\d{4}-\d{2}-\d{2})',
                  text, re.IGNORECASE)
    return m.group(1) if m else None


@F.udf(returnType=T.StringType())
def regex_invoice_date(text: str) -> str:
    """
    Extract invoice date — appears after 'Invoice Date:' label.
    FIX: added because ai_extract misses invoice_date on some rows,
    causing EXC_DATE fraud signal check to silently skip.
    """
    if not text:
        return None
    m = re.search(
        r'Invoice\s*Date[:\s]+(\d{4}-\d{2}-\d{2})',
        text, re.IGNORECASE
    )
    return m.group(1) if m else None


@F.udf(returnType=T.StringType())
def regex_due_date(text: str) -> str:
    """Extract due date — appears after 'Due Date:' label."""
    if not text:
        return None
    m = re.search(
        r'Due\s*Date[:\s]+(\d{4}-\d{2}-\d{2})',
        text, re.IGNORECASE
    )
    return m.group(1) if m else None


# ══════════════════════════════════════════════════════════════════════════════
# COALESCE HELPER
# ══════════════════════════════════════════════════════════════════════════════

def with_fallback(df, col_name, regex_udf, text_col="text", *udf_args):
    """
    If ai_extract returned null for col_name, apply regex fallback.
    Tracks when fallback fired via _fallback_<col> boolean column.
    """
    fallback_col = f"_fallback_{col_name}"
    fallback_val = (
        regex_udf(F.col(text_col), *udf_args)
        if udf_args
        else regex_udf(F.col(text_col))
    )
    return (
        df
        .withColumn(
            fallback_col,
            F.when(
                F.col(col_name).isNull() & fallback_val.isNotNull(),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            col_name,
            F.coalesce(F.col(col_name), fallback_val)
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# SILVER: PURCHASE ORDER
# ══════════════════════════════════════════════════════════════════════════════

@dlt.view(name="raw_silver_purchase_order")
def raw_silver_purchase_order():
    df = (
        spark.table(BRONZE_PO)
             .withColumn("extracted", F.expr("""
                 ai_extract(text, array(
                     'po_number',
                     'po_date',
                     'delivery_date',
                     'payment_terms',
                     'vendor_id',
                     'vendor_name',
                     'vendor_address',
                     'vendor_trn',
                     'buyer_name',
                     'buyer_trn',
                     'currency',
                     'subtotal_aed',
                     'vat_amount_aed',
                     'total_amount_aed',
                     'line_items'
                 ))
             """))
             # Keep dates as string first — cast AFTER fallback
             .withColumn("po_number",        F.col("extracted.po_number"))
             .withColumn("po_date",          F.col("extracted.po_date"))
             .withColumn("delivery_date",    F.col("extracted.delivery_date"))
             .withColumn("payment_terms",    F.col("extracted.payment_terms"))
             .withColumn("vendor_id",        F.col("extracted.vendor_id"))
             .withColumn("vendor_name",      F.col("extracted.vendor_name"))
             .withColumn("vendor_address",   F.col("extracted.vendor_address"))
             .withColumn("vendor_trn",       F.col("extracted.vendor_trn"))
             .withColumn("buyer_name",       F.col("extracted.buyer_name"))
             .withColumn("buyer_trn",        F.col("extracted.buyer_trn"))
             .withColumn("currency",         F.col("extracted.currency"))
             .withColumn("subtotal_aed",     F.col("extracted.subtotal_aed").cast("double"))
             .withColumn("vat_amount_aed",   F.col("extracted.vat_amount_aed").cast("double"))
             .withColumn("total_amount_aed", F.col("extracted.total_amount_aed").cast("double"))
             .withColumn("line_items",       F.col("extracted.line_items"))
             .withColumn("_silver_timestamp", F.current_timestamp())
             .drop("extracted")
    )

    # ── Regex fallbacks for critical fields ───────────────────────────────
    df = with_fallback(df, "po_number",        regex_po_number)
    # FIX: use regex_vendor_name (Vendor: label strategy) not invoice vendor
    df = with_fallback(df, "vendor_name",      regex_vendor_name)
    df = with_fallback(df, "total_amount_aed", regex_total_amount)

    # ── Cast dates AFTER fallback so string fallback values convert too ───
    df = df.withColumn("po_date",       F.col("po_date").cast("date"))
    df = df.withColumn("delivery_date", F.col("delivery_date").cast("date"))

    # ── Extraction completeness ───────────────────────────────────────────
    df = df.withColumn("_extraction_complete",
             F.when(
                 F.col("po_number").isNotNull() &
                 F.col("vendor_name").isNotNull() &
                 F.col("total_amount_aed").isNotNull(),
                 F.lit(True)
             ).otherwise(F.lit(False)))

    return df


@dlt.expect("po_number_present", "po_number IS NOT NULL")
@dlt.expect("vendor_present",    "vendor_name IS NOT NULL")
@dlt.expect("amount_present",    "total_amount_aed IS NOT NULL")
@dlt.expect("amount_positive",   "total_amount_aed > 0")
@dlt.table(
    name="purchase_order",
    comment="Silver: PO fields via ai_extract + regex fallback (Aurelius exclusion fix)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)
def silver_purchase_order():
    return dlt.read("raw_silver_purchase_order")


# ══════════════════════════════════════════════════════════════════════════════
# SILVER: GOODS RECEIPT
# ══════════════════════════════════════════════════════════════════════════════

@dlt.view(name="raw_silver_good_receipt")
def raw_silver_good_receipt():
    df = (
        spark.table(BRONZE_GR)
             .withColumn("extracted", F.expr("""
                 ai_extract(text, array(
                     'gr_number',
                     'gr_date',
                     'gr_status',
                     'po_number',
                     'vendor_id',
                     'vendor_name',
                     'received_by',
                     'line_items'
                 ))
             """))
             # Keep gr_date as string first — cast AFTER fallback
             .withColumn("gr_number",   F.col("extracted.gr_number"))
             .withColumn("gr_date",     F.col("extracted.gr_date"))
             .withColumn("gr_status",   F.col("extracted.gr_status"))
             .withColumn("po_number",   F.col("extracted.po_number"))
             .withColumn("vendor_id",   F.col("extracted.vendor_id"))
             .withColumn("vendor_name", F.col("extracted.vendor_name"))
             .withColumn("received_by", F.col("extracted.received_by"))
             .withColumn("line_items",  F.col("extracted.line_items"))
             .withColumn("_silver_timestamp", F.current_timestamp())
             .drop("extracted")
    )

    # ── Regex fallbacks ───────────────────────────────────────────────────
    df = with_fallback(df, "gr_number",  regex_gr_number)
    df = with_fallback(df, "po_number",  regex_po_number)
    df = with_fallback(df, "gr_status",  regex_gr_status)
    # FIX: gr_date fallback added — prevents EXC_DATE check silently skipping
    df = with_fallback(df, "gr_date",    regex_gr_date)

    # ── Cast date AFTER fallback ──────────────────────────────────────────
    df = df.withColumn("gr_date", F.col("gr_date").cast("date"))

    df = df.withColumn("_extraction_complete",
             F.when(
                 F.col("gr_number").isNotNull() &
                 F.col("po_number").isNotNull() &
                 F.col("gr_status").isNotNull(),
                 F.lit(True)
             ).otherwise(F.lit(False)))

    return df


@dlt.expect("gr_number_present", "gr_number IS NOT NULL")
@dlt.expect("po_ref_present",    "po_number IS NOT NULL")
@dlt.expect("gr_status_present", "gr_status IS NOT NULL")
@dlt.table(
    name="good_receipt",
    comment="Silver: GR fields via ai_extract + regex fallback (gr_date fallback added)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)
def silver_good_receipt():
    return dlt.read("raw_silver_good_receipt")


# ══════════════════════════════════════════════════════════════════════════════
# SILVER: INVOICE
# ══════════════════════════════════════════════════════════════════════════════

@dlt.view(name="raw_silver_invoice")
def raw_silver_invoice():
    df = (
        spark.table(BRONZE_INV)
             .withColumn("extracted", F.expr("""
                 ai_extract(text, array(
                     'invoice_number',
                     'invoice_date',
                     'due_date',
                     'po_number',
                     'payment_status',
                     'vendor_name',
                     'vendor_trn',
                     'buyer_name',
                     'buyer_trn',
                     'subtotal_aed',
                     'vat_amount_aed',
                     'total_amount_aed',
                     'bank_name',
                     'iban',
                     'line_items'
                 ))
             """))
             # Keep dates as string first — cast AFTER fallback
             .withColumn("invoice_number",   F.col("extracted.invoice_number"))
             .withColumn("invoice_date",     F.col("extracted.invoice_date"))
             .withColumn("due_date",         F.col("extracted.due_date"))
             .withColumn("po_number",        F.col("extracted.po_number"))
             .withColumn("payment_status",   F.col("extracted.payment_status"))
             .withColumn("vendor_name",      F.col("extracted.vendor_name"))
             .withColumn("vendor_trn",       F.col("extracted.vendor_trn"))
             .withColumn("buyer_name",       F.col("extracted.buyer_name"))
             .withColumn("buyer_trn",        F.col("extracted.buyer_trn"))
             .withColumn("subtotal_aed",     F.col("extracted.subtotal_aed").cast("double"))
             .withColumn("vat_amount_aed",   F.col("extracted.vat_amount_aed").cast("double"))
             .withColumn("total_amount_aed", F.col("extracted.total_amount_aed").cast("double"))
             .withColumn("bank_name",        F.col("extracted.bank_name"))
             .withColumn("iban",             F.col("extracted.iban"))
             .withColumn("line_items",       F.col("extracted.line_items"))
             .withColumn("_silver_timestamp", F.current_timestamp())
             .drop("extracted")
    )

    # ── Regex fallbacks ───────────────────────────────────────────────────
    df = with_fallback(df, "invoice_number",   regex_invoice_number)
    df = with_fallback(df, "po_number",        regex_po_number)
    df = with_fallback(df, "total_amount_aed", regex_total_amount)
    # FIX: use regex_invoice_vendor (TAX INVOICE header strategy)
    #      not regex_vendor_name (Vendor: label — not present on invoices)
    df = with_fallback(df, "vendor_name",      regex_invoice_vendor)
    # FIX: invoice_date + due_date fallbacks added
    #      prevents EXC_DATE fraud check silently skipping on null dates
    df = with_fallback(df, "invoice_date",     regex_invoice_date)
    df = with_fallback(df, "due_date",         regex_due_date)

    # ── Cast dates AFTER all fallbacks ───────────────────────────────────
    df = df.withColumn("invoice_date", F.col("invoice_date").cast("date"))
    df = df.withColumn("due_date",     F.col("due_date").cast("date"))

    # ── Extraction completeness — includes vendor_name now ────────────────
    df = df.withColumn("_extraction_complete",
             F.when(
                 F.col("invoice_number").isNotNull() &
                 F.col("po_number").isNotNull() &
                 F.col("total_amount_aed").isNotNull() &
                 F.col("vendor_name").isNotNull(),
                 F.lit(True)
             ).otherwise(F.lit(False)))

    return df


@dlt.expect("invoice_number_present", "invoice_number IS NOT NULL")
@dlt.expect("po_ref_present",         "po_number IS NOT NULL")
@dlt.expect("amount_present",         "total_amount_aed IS NOT NULL")
@dlt.expect("amount_positive",        "total_amount_aed > 0")
@dlt.expect("due_date_present",       "due_date IS NOT NULL")
@dlt.table(
    name="invoice",
    comment="Silver: Invoice fields via ai_extract + regex fallback (vendor + date fixes)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)
def silver_invoice():
    return dlt.read("raw_silver_invoice")


# ══════════════════════════════════════════════════════════════════════════════
# SILVER: EXTRACTION AUDIT
# ══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="extraction_audit",
    comment="Extraction completeness + fallback tracking across all doc types",
    table_properties={"quality": "silver"}
)
def silver_extraction_audit():
    po = (
        dlt.read("raw_silver_purchase_order")
           .select(
               F.col("filename"),
               F.lit("purchase_order").alias("doc_type"),
               F.col("po_number").alias("doc_id"),
               F.col("_extraction_complete"),
               F.col("_fallback_po_number"),
               F.col("_fallback_vendor_name"),
               F.col("_fallback_total_amount_aed"),
               F.lit(False).alias("_fallback_gr_number"),
               F.lit(False).alias("_fallback_invoice_number"),
               F.lit(False).alias("_fallback_invoice_date"),
               F.col("_silver_timestamp")
           )
    )

    gr = (
        dlt.read("raw_silver_good_receipt")
           .select(
               F.col("filename"),
               F.lit("good_receipt").alias("doc_type"),
               F.col("gr_number").alias("doc_id"),
               F.col("_extraction_complete"),
               F.col("_fallback_po_number"),
               F.lit(False).alias("_fallback_vendor_name"),
               F.lit(False).alias("_fallback_total_amount_aed"),
               F.col("_fallback_gr_number"),
               F.lit(False).alias("_fallback_invoice_number"),
               F.lit(False).alias("_fallback_invoice_date"),
               F.col("_silver_timestamp")
           )
    )

    inv = (
        dlt.read("raw_silver_invoice")
           .select(
               F.col("filename"),
               F.lit("invoice").alias("doc_type"),
               F.col("invoice_number").alias("doc_id"),
               F.col("_extraction_complete"),
               F.col("_fallback_po_number"),
               F.lit(False).alias("_fallback_vendor_name"),
               F.col("_fallback_total_amount_aed"),
               F.lit(False).alias("_fallback_gr_number"),
               F.col("_fallback_invoice_number"),
               F.col("_fallback_invoice_date"),
               F.col("_silver_timestamp")
           )
    )

    return (
        po.unionByName(gr,  allowMissingColumns=True)
          .unionByName(inv, allowMissingColumns=True)
    )
