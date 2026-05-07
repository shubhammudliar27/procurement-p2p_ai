"""
02_upload_dataset_to_azure.py
==============================
Local utility (runs on your laptop, not Databricks) to upload
the generated P2P dataset PDFs to Azure Blob Storage.

This replaces the manual "copy to Google Drive" step from the tutorial.
In a real system, vendors upload directly via an API or portal.
For your portfolio, this script simulates that upload.

Usage:
    pip install azure-storage-blob tqdm
    python 02_upload_dataset_to_azure.py

Set environment variables before running (never hardcode credentials):
    export AZURE_STORAGE_CONN_STR="DefaultEndpointsProtocol=https;AccountName=..."
    export AZURE_CONTAINER_NAME="p2p-landing"
    export DATASET_PATH="./dataset_v2"       # path to your generated PDFs

Features:
    - Uploads POs, GRs, Invoices into correct blob sub-folders
    - Skips files already uploaded (checks blob existence + size)
    - Shows upload progress with tqdm
    - Can upload a subset by scenario using --scenario flag
    - Dry-run mode with --dry-run to preview what would be uploaded
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
    from tqdm import tqdm
except ImportError:
    print("Missing dependencies. Run: pip install azure-storage-blob tqdm")
    sys.exit(1)


# ── Config from environment variables ─────────────────────────────────────────
CONN_STR        = os.environ.get("AZURE_STORAGE_CONN_STR", "")
CONTAINER_NAME  = os.environ.get("AZURE_CONTAINER_NAME", "p2p-landing")
DATASET_PATH    = os.environ.get("DATASET_PATH", "./dataset_v2")

# Maps local folder name → Azure blob sub-folder name
FOLDER_MAP = {
    "purchase_order": "purchase_order",
    "good_receipt":   "good_receipt",
    "invoice":        "invoice",
}


def get_blob_client(conn_str: str, container: str):
    """Create Azure Blob container client."""
    if not conn_str:
        raise ValueError(
            "AZURE_STORAGE_CONN_STR environment variable is not set.\n"
            "Get it from: Azure Portal → Storage Account → Access keys → Connection string"
        )
    service = BlobServiceClient.from_connection_string(conn_str)
    return service.get_container_client(container)


def blob_exists_and_matches(container_client, blob_name: str, local_size: int) -> bool:
    """Return True if blob exists with same file size — skip re-upload."""
    try:
        props = container_client.get_blob_client(blob_name).get_blob_properties()
        return props.size == local_size
    except Exception:
        return False


def upload_folder(container_client, local_folder: Path, blob_folder: str,
                  dry_run: bool = False, scenario_filter: str = None) -> dict:
    """
    Upload all PDFs in local_folder to blob_folder.
    Returns stats dict: {uploaded, skipped, failed}.
    """
    pdf_files = sorted(local_folder.glob("*.pdf"))
    if not pdf_files:
        print(f"  ⚠️  No PDF files found in {local_folder}")
        return {"uploaded": 0, "skipped": 0, "failed": 0}

    stats = {"uploaded": 0, "skipped": 0, "failed": 0}

    for pdf_path in tqdm(pdf_files, desc=f"  {blob_folder}", unit="file"):
        blob_name  = f"{blob_folder}/{pdf_path.name}"
        local_size = pdf_path.stat().st_size

        # Skip if scenario filter set and file doesn't match
        if scenario_filter and scenario_filter.lower() not in pdf_path.name.lower():
            stats["skipped"] += 1
            continue

        # Skip if already uploaded with same size
        if blob_exists_and_matches(container_client, blob_name, local_size):
            stats["skipped"] += 1
            continue

        if dry_run:
            print(f"    [DRY RUN] Would upload: {blob_name} ({local_size:,} bytes)")
            stats["uploaded"] += 1
            continue

        try:
            with open(pdf_path, "rb") as f:
                container_client.upload_blob(
                    name=blob_name,
                    data=f,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/pdf"),
                    metadata={
                        "source":     "p2p_dataset_generator",
                        "uploaded_at": datetime.utcnow().isoformat(),
                        "doc_type":   blob_folder,
                    }
                )
            stats["uploaded"] += 1
        except Exception as e:
            print(f"\n    ❌ Failed: {pdf_path.name} — {e}")
            stats["failed"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Upload P2P dataset PDFs to Azure Blob Storage"
    )
    parser.add_argument(
        "--dataset-path", default=DATASET_PATH,
        help="Path to dataset_v2 folder (default: ./dataset_v2)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--scenario", default=None,
        help="Only upload files matching this scenario name (e.g. MATCH_EXACT)"
    )
    parser.add_argument(
        "--doc-type", default=None, choices=list(FOLDER_MAP.keys()),
        help="Only upload one document type (purchase_order / good_receipt / invoice)"
    )
    args = parser.parse_args()

    dataset_path = Path(args.dataset_path)
    if not dataset_path.exists():
        print(f"❌ Dataset path not found: {dataset_path}")
        print("   Run generate_p2p_dataset_v2.py first to create the dataset.")
        sys.exit(1)

    print(f"\nP2P Dataset → Azure Blob Uploader")
    print(f"{'─'*45}")
    print(f"  Dataset path  : {dataset_path.resolve()}")
    print(f"  Container     : {CONTAINER_NAME}")
    print(f"  Dry run       : {args.dry_run}")
    if args.scenario:
        print(f"  Scenario filter: {args.scenario}")
    if args.doc_type:
        print(f"  Doc type filter: {args.doc_type}")
    print(f"{'─'*45}\n")

    # Connect to Azure
    try:
        container_client = get_blob_client(CONN_STR, CONTAINER_NAME)
        # Verify connection
        container_client.get_container_properties()
        print(f"✅ Connected to Azure Blob container: {CONTAINER_NAME}\n")
    except Exception as e:
        print(f"❌ Cannot connect to Azure: {e}")
        print("\nTroubleshooting:")
        print("  1. Check AZURE_STORAGE_CONN_STR is set correctly")
        print("  2. Verify the container exists (run 00_azure_setup.py in Databricks first)")
        print("  3. Check your network/firewall allows outbound HTTPS to Azure")
        sys.exit(1)

    # Upload each document type
    total_stats = {"uploaded": 0, "skipped": 0, "failed": 0}
    folders_to_process = (
        {args.doc_type: FOLDER_MAP[args.doc_type]}
        if args.doc_type else FOLDER_MAP
    )

    for local_folder_name, blob_folder in folders_to_process.items():
        local_path = dataset_path / local_folder_name
        if not local_path.exists():
            print(f"  ⚠️  Skipping {local_folder_name}/ — folder not found")
            continue

        print(f"📁 Uploading {local_folder_name}/")
        stats = upload_folder(
            container_client, local_path, blob_folder,
            dry_run=args.dry_run, scenario_filter=args.scenario
        )
        for k in total_stats:
            total_stats[k] += stats[k]
        print(f"   ↳ uploaded={stats['uploaded']}, "
              f"skipped={stats['skipped']}, failed={stats['failed']}\n")

    # Summary
    print(f"{'─'*45}")
    print(f"  Total uploaded : {total_stats['uploaded']}")
    print(f"  Total skipped  : {total_stats['skipped']}  (already in blob)")
    print(f"  Total failed   : {total_stats['failed']}")
    print(f"{'─'*45}")

    if total_stats["uploaded"] > 0 and not args.dry_run:
        print(f"\n✅ Upload complete.")
        print(f"   Auto Loader in Databricks will pick up new files automatically")
        print(f"   via Azure Event Grid — no manual trigger needed.")
        print(f"\n   Monitor ingestion in Databricks:")
        print(f"   Workflows → DLT Pipelines → P2P Bronze Ingestion → View")
    elif args.dry_run:
        print(f"\n✅ Dry run complete — no files were actually uploaded.")
        print(f"   Remove --dry-run to perform the real upload.")


if __name__ == "__main__":
    main()
