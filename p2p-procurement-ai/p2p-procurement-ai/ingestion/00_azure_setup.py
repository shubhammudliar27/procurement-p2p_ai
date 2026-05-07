# Databricks notebook source
# =============================================================================
# 00_azure_setup.py  —  Run ONCE to provision Azure Blob Storage structure
# =============================================================================
# What this does:
#   1. Creates the Azure Blob container with three sub-folders
#      (purchase_order / good_receipt / invoice)
#   2. Stores the Azure connection string in Databricks Secrets (not plain text)
#   3. Verifies the connection is working
#
# Pre-requisites (do these manually before running):
#   a. Create an Azure Storage Account in portal.azure.com
#      (General Purpose v2, LRS redundancy is fine for portfolio)
#   b. Copy the Connection String from:
#      Storage Account → Security + Networking → Access keys → Connection string
#   c. Create a Databricks Secret Scope:
#      databricks secrets create-scope --scope p2p-scope
#   d. Store the connection string:
#      databricks secrets put --scope p2p-scope --key azure-storage-conn-str
#   e. Store the storage account name:
#      databricks secrets put --scope p2p-scope --key azure-storage-account
#   f. Store the container name:
#      databricks secrets put --scope p2p-scope --key azure-blob-container
# =============================================================================

# COMMAND ----------
# %pip install azure-storage-blob --quiet

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------
# ── Pull credentials from Databricks Secrets (never hardcode) ─────────────────
CONN_STR       = dbutils.secrets.get(scope="p2p-scope", key="azure-storage-conn-str")
ACCOUNT_NAME   = dbutils.secrets.get(scope="p2p-scope", key="azure-storage-account")
CONTAINER_NAME = dbutils.secrets.get(scope="p2p-scope", key="azure-blob-container")

# Sub-folder structure mirrors your document types
DOC_FOLDERS = ["purchase_order", "good_receipt", "invoice"]

print(f"Storage account : {ACCOUNT_NAME}")
print(f"Container       : {CONTAINER_NAME}")

# COMMAND ----------
# ── Create container and folder placeholders ───────────────────────────────────
from azure.storage.blob import BlobServiceClient

blob_service = BlobServiceClient.from_connection_string(CONN_STR)

# Create container if it doesn't exist
try:
    blob_service.create_container(CONTAINER_NAME)
    print(f"✅ Container '{CONTAINER_NAME}' created")
except Exception as e:
    if "ContainerAlreadyExists" in str(e):
        print(f"ℹ️  Container '{CONTAINER_NAME}' already exists — skipping")
    else:
        raise

# Create a .keep placeholder in each folder so the folder is visible
container_client = blob_service.get_container_client(CONTAINER_NAME)
for folder in DOC_FOLDERS:
    blob_name = f"{folder}/.keep"
    try:
        container_client.upload_blob(name=blob_name, data=b"", overwrite=True)
        print(f"✅ Folder created: {folder}/")
    except Exception as e:
        print(f"⚠️  {folder}: {e}")

# COMMAND ----------
# ── Mount Azure Blob into DBFS (for Auto Loader and UC Volume reference) ───────
# NOTE: For Databricks Unity Catalog workspaces, prefer External Locations
#       over DBFS mounts. This mount is for compatibility with the existing
#       Bronze DLT pipeline that reads from a volume path.

MOUNT_POINT = f"/mnt/p2p-landing"

try:
    dbutils.fs.mount(
        source=f"wasbs://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/",
        mount_point=MOUNT_POINT,
        extra_configs={
            f"fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net":
                dbutils.secrets.get(scope="p2p-scope", key="azure-storage-conn-str")
                .split("AccountKey=")[1].split(";")[0]  # extract key from conn string
        }
    )
    print(f"✅ Mounted at {MOUNT_POINT}")
except Exception as e:
    if "already mounted" in str(e).lower():
        print(f"ℹ️  Already mounted at {MOUNT_POINT}")
    else:
        raise

# COMMAND ----------
# ── Register as Databricks External Location (Unity Catalog preferred) ─────────
# Run this in SQL if your workspace uses Unity Catalog:
# CREATE EXTERNAL LOCATION p2p_landing
#   URL 'abfss://<container>@<account>.dfs.core.windows.net/'
#   WITH (STORAGE CREDENTIAL <your_storage_credential>);
#
# Then reference it in Auto Loader as:
#   abfss://{CONTAINER_NAME}@{ACCOUNT_NAME}.dfs.core.windows.net/purchase_order/
#
# For simplicity in this portfolio setup, we use the wasbs:// mount above.

# COMMAND ----------
# ── Verify: list the folder structure ─────────────────────────────────────────
print("\nVerifying folder structure:")
for folder in DOC_FOLDERS:
    files = dbutils.fs.ls(f"{MOUNT_POINT}/{folder}/")
    print(f"  {folder}/  → {len(files)} file(s)")

print("\n✅ Azure Blob Storage setup complete.")
print(f"   Drop PDFs into: wasbs://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/<doc_type>/")
print(f"   Auto Loader will detect new files automatically via Event Grid.")
print(f"\n   Next step: Run 01_autoloader_bronze_ingestion.py as a DLT pipeline")
