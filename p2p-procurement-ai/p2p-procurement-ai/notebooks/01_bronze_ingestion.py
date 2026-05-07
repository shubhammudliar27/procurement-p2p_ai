# Databricks notebook source
# =============================================================================
# 01_bronze_ingestion.py
# Syncs PDFs from Azure Blob Storage into Databricks UC Volume
# Run manually or schedule as a Databricks Job
# =============================================================================

# COMMAND ----------
# %pip install azure-storage-blob pdfminer.six pypdf --quiet

# COMMAND ----------

# ── Config ─────────────────────────────────────────────────────────────────
# TODO: move credentials to Databricks Secrets for production
CONN_STR       = "YOUR_AZURE_STORAGE_CONNECTION_STRING"
ACCOUNT_NAME   = "YOUR_STORAGE_ACCOUNT_NAME"
CONTAINER_NAME = "p2p-landing"
CATALOG_NAME   = "p2p_databricks"   # change to your catalog name
VOLUME_BASE    = f"/Volumes/{CATALOG_NAME}/staging/p2p_files"

print(f"Account  : {ACCOUNT_NAME}")
print(f"Container: {CONTAINER_NAME}")
print(f"Volume   : {VOLUME_BASE}")
print("✅ Config ready")

# COMMAND ----------
# ── Create Unity Catalog structure (run once) ──────────────────────────────

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.staging "
          f"COMMENT 'Landing zone for raw procurement PDFs'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.bronze "
          f"COMMENT 'Bronze layer'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.silver "
          f"COMMENT 'Silver layer'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold "
          f"COMMENT 'Gold layer'")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.staging.p2p_files
    COMMENT 'Raw PDF files synced from Azure Blob Storage'
""")

for doc_type in ["purchase_order", "good_receipt", "invoice"]:
    dbutils.fs.mkdirs(f"{VOLUME_BASE}/{doc_type}")
    print(f"✅ Folder ready: {VOLUME_BASE}/{doc_type}")

# COMMAND ----------
# ── Sync Azure Blob → UC Volume ────────────────────────────────────────────

from azure.storage.blob import BlobServiceClient

def sync_blob_to_volume(conn_str, container_name, volume_base):
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container    = blob_service.get_container_client(container_name)
    doc_types    = ["purchase_order", "good_receipt", "invoice"]
    stats        = {"downloaded": 0, "skipped": 0, "failed": 0}

    for doc_type in doc_types:
        print(f"\n📁 {doc_type}/")
        blobs = [b for b in container.list_blobs(
                     name_starts_with=f"{doc_type}/")
                 if b.name.endswith(".pdf")]
        print(f"   Found {len(blobs)} PDFs in Azure Blob")

        try:
            existing = {f.name: f.size
                        for f in dbutils.fs.ls(f"{volume_base}/{doc_type}/")}
        except Exception:
            existing = {}

        for blob in blobs:
            filename  = blob.name.split("/")[-1]
            blob_size = blob.size

            if filename in existing and existing[filename] == blob_size:
                stats["skipped"] += 1
                continue

            try:
                pdf_bytes = (container
                             .get_blob_client(blob.name)
                             .download_blob()
                             .readall())
                write_path = f"{volume_base}/{doc_type}/{filename}"
                with open(write_path, "wb") as f:
                    f.write(pdf_bytes)
                stats["downloaded"] += 1
                print(f"   ✅ {filename}  ({blob_size:,} bytes)")
            except Exception as e:
                stats["failed"] += 1
                print(f"   ❌ {filename} — {e}")

    return stats

stats = sync_blob_to_volume(CONN_STR, CONTAINER_NAME, VOLUME_BASE)
print(f"""
Sync complete
  ✅ Downloaded : {stats['downloaded']}
  ⏭️  Skipped   : {stats['skipped']}
  ❌ Failed     : {stats['failed']}
""")

# COMMAND ----------
# ── Verify counts ──────────────────────────────────────────────────────────

expected = {"purchase_order": 60, "good_receipt": 60, "invoice": 64}
for doc_type, exp_count in expected.items():
    try:
        files  = dbutils.fs.ls(f"{VOLUME_BASE}/{doc_type}/")
        pdfs   = [f for f in files if f.name.endswith(".pdf")]
        actual = len(pdfs)
        status = "✅" if actual == exp_count else "⚠️ "
        print(f"  {status} {doc_type:22s}: {actual:3d} PDFs  (expected {exp_count})")
    except Exception as e:
        print(f"  ❌ {doc_type}: {e}")

# COMMAND ----------
# ── Sanity check: extract text from one PDF ────────────────────────────────

import io
from pdfminer.high_level import extract_text

test_files = {
    "purchase_order": "PO-2025-0001.pdf",
    "good_receipt":   "GR-2025-0001.pdf",
    "invoice":        "INV-2025-0001.pdf",
}

for doc_type, filename in test_files.items():
    path = f"{VOLUME_BASE}/{doc_type}/{filename}"
    try:
        with open(path, "rb") as f:
            text = extract_text(io.BytesIO(f.read()))
        print(f"✅ {doc_type}/{filename}: {len(text)} chars")
        print(f"   Preview: {' '.join(text.split())[:150]}...")
    except Exception as e:
        print(f"❌ {doc_type}/{filename}: {e}")
