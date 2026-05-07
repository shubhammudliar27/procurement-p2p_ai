import dlt
import io
import hashlib
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── Config ─────────────────────────────────────────────────────────────────
CATALOG_NAME = spark.conf.get("catalog_name", "p2p_databricks")
VOLUME_BASE  = f"/Volumes/{CATALOG_NAME}/staging/p2p_files"


# ── UDFs ───────────────────────────────────────────────────────────────────

@F.udf(returnType=T.StringType())
def extract_pdf_text(content: bytes) -> str:
    """Extract plain text from PDF binary. pdfminer primary, pypdf fallback."""
    if content is None:
        return None
    try:
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        import io as _io
        out = _io.StringIO()
        extract_text_to_fp(
            _io.BytesIO(content), out,
            laparams=LAParams(), output_type="text", codec="utf-8"
        )
        text = out.getvalue().strip()
        if text:
            return text
    except Exception:
        pass
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(content))
        return "\n".join(p.extract_text() or "" for p in reader.pages).strip()
    except Exception:
        return None


@F.udf(returnType=T.StringType())
def compute_hash(content: bytes) -> str:
    if content is None:
        return None
    return hashlib.sha256(content).hexdigest()


@F.udf(returnType=T.StringType())
def get_filename(path: str) -> str:
    if path is None:
        return None
    return path.split("/")[-1].replace(".pdf", "").replace(".PDF", "")


def read_pdfs(folder_name: str, doc_type: str):
    return (
        spark.read
             .format("binaryFile")
             .option("pathGlobFilter", "*.pdf")
             .option("recursiveFileLookup", "false")
             .load(f"{VOLUME_BASE}/{folder_name}/")
             .withColumn("filename",        get_filename(F.col("path")))
             .withColumn("doc_type",        F.lit(doc_type))
             .withColumn("text",            extract_pdf_text(F.col("content")))
             .withColumn("file_hash",       compute_hash(F.col("content")))
             .withColumn("file_size_bytes", F.col("length"))
             .withColumn("_load_timestamp", F.current_timestamp())
             .withColumn("_source_path",    F.col("path"))
             .withColumn("_text_extracted",
                         F.when(
                             F.col("text").isNotNull() & (F.length("text") > 20),
                             F.lit(True)
                         ).otherwise(F.lit(False)))
             .drop("content", "path", "length", "modificationTime")
    )


# ── Bronze: Purchase Order ──────────────────────────────────────────────────

@dlt.view(name="raw_purchase_order")
def raw_purchase_order():
    return read_pdfs("purchase_order", "purchase_order")

@dlt.expect_or_drop("filename_not_null", "filename IS NOT NULL")
@dlt.expect_or_drop("file_not_empty",    "file_size_bytes > 500")
@dlt.expect_or_drop("text_extracted",    "_text_extracted = true")
@dlt.table(
    name="purchase_order",
    comment="Bronze: PO PDFs with extracted text and quality gates",
    table_properties={"delta.enableChangeDataFeed": "true", "quality": "bronze"}
)
def bronze_purchase_order():
    return dlt.read("raw_purchase_order")


# ── Bronze: Goods Receipt ───────────────────────────────────────────────────

@dlt.view(name="raw_good_receipt")
def raw_good_receipt():
    return read_pdfs("good_receipt", "good_receipt")

@dlt.expect_or_drop("filename_not_null", "filename IS NOT NULL")
@dlt.expect_or_drop("file_not_empty",    "file_size_bytes > 500")
@dlt.expect_or_drop("text_extracted",    "_text_extracted = true")
@dlt.table(
    name="good_receipt",
    comment="Bronze: GR PDFs with extracted text and quality gates",
    table_properties={"delta.enableChangeDataFeed": "true", "quality": "bronze"}
)
def bronze_good_receipt():
    return dlt.read("raw_good_receipt")


# ── Bronze: Invoice ─────────────────────────────────────────────────────────

@dlt.view(name="raw_invoice")
def raw_invoice():
    return read_pdfs("invoice", "invoice")

@dlt.expect_or_drop("filename_not_null", "filename IS NOT NULL")
@dlt.expect_or_drop("file_not_empty",    "file_size_bytes > 500")
@dlt.expect_or_drop("text_extracted",    "_text_extracted = true")
@dlt.table(
    name="invoice",
    comment="Bronze: Invoice PDFs with extracted text and quality gates",
    table_properties={"delta.enableChangeDataFeed": "true", "quality": "bronze"}
)
def bronze_invoice():
    return dlt.read("raw_invoice")


# ── Bronze: Quarantine ──────────────────────────────────────────────────────

@dlt.table(
    name="quarantine",
    comment="PDFs that failed quality checks — need manual review",
    table_properties={"quality": "quarantine"}
)
def bronze_quarantine():
    po  = read_pdfs("purchase_order", "purchase_order")
    gr  = read_pdfs("good_receipt",   "good_receipt")
    inv = read_pdfs("invoice",        "invoice")
    all_docs = (po.unionByName(gr,  allowMissingColumns=True)
                  .unionByName(inv, allowMissingColumns=True))
    return all_docs.filter(
        F.col("text").isNull() | (F.length("text") <= 20)
    )
