#!/usr/bin/env python3
"""
Event Simulator — Push sample data to Azure Event Hubs and Blob Storage.

Sends the same sample files used in local testing through the cloud
ingestion pipeline to verify end-to-end functionality.

Usage:
    pip install azure-eventhub azure-storage-blob python-dotenv
    python simulate.py

    # Or test individual pipelines:
    python simulate.py --claims
    python simulate.py --remittances
    python simulate.py --patients
    python simulate.py --documents
    python simulate.py --fee-schedules
"""

import argparse
import json
import os
import sys
import time

from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient

# Sample data directory (from local ingestion tests)
SAMPLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "ingestion", "sample_data"
)


def load_env():
    """Load connection strings from .env file."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        print(f"Warning: {env_path} not found. Set environment variables manually.")


def read_sample(filename: str) -> str:
    """Read a sample data file."""
    path = os.path.join(SAMPLE_DIR, filename)
    if not os.path.exists(path):
        print(f"  Skipping {filename} — file not found at {path}")
        return None
    with open(path, "r") as f:
        return f.read()


def send_to_eventhub(hub_name: str, data: str, label: str):
    """Send data to an Event Hub."""
    conn_str = os.environ.get("EVENTHUB_CONNECTION_STRING")
    if not conn_str:
        print(f"  EVENTHUB_CONNECTION_STRING not set — skipping {label}")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str, eventhub_name=hub_name
    )
    with producer:
        batch = producer.create_batch()
        batch.add(EventData(data))
        producer.send_batch(batch)
    print(f"  Sent {label} to Event Hub '{hub_name}' ({len(data)} bytes)")


def upload_to_blob(container: str, blob_name: str, data, label: str):
    """Upload data to a blob container."""
    conn_str = os.environ.get("STORAGE_CONNECTION_STRING")
    if not conn_str:
        print(f"  STORAGE_CONNECTION_STRING not set — skipping {label}")
        return

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container, blob_name)

    if isinstance(data, str):
        data = data.encode("utf-8")

    blob_client.upload_blob(data, overwrite=True)
    print(f"  Uploaded {label} to {container}/{blob_name} ({len(data)} bytes)")


def simulate_claims():
    """Send sample EDI 837 claims to Event Hub."""
    print("\n--- Claims (EDI 837) ---")
    for filename in ["claims_837.edi", "claims_837_replacement.edi",
                     "claims_837_batch2.edi", "claims_837_batch3.edi"]:
        raw = read_sample(filename)
        if raw:
            send_to_eventhub("claims", raw, filename)
            time.sleep(1)


def simulate_remittances():
    """Send sample EDI 835 remittances to Event Hub."""
    print("\n--- Remittances (EDI 835) ---")
    for filename in ["era_835.edi", "era_835_second.edi",
                     "era_835_batch2.edi", "era_835_batch3.edi"]:
        raw = read_sample(filename)
        if raw:
            send_to_eventhub("remittances", raw, filename)
            time.sleep(1)

    # Also send EOB extractions
    print("\n--- EOB Extractions ---")
    raw = read_sample("eob_extracted.json")
    if raw:
        send_to_eventhub("documents", raw, "eob_extracted.json")


def simulate_patients():
    """POST FHIR patients to the HTTP function."""
    print("\n--- Patients (FHIR) ---")
    raw = read_sample("fhir_patients.json")
    if raw:
        app_url = os.environ.get("APP_SERVICE_URL", "http://localhost:7071")
        func_url = f"{app_url}/api/ingest/patients"
        print(f"  POST to {func_url}")

        try:
            import urllib.request
            req = urllib.request.Request(
                func_url,
                data=raw.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())
                print(f"  Result: {json.dumps(result)}")
        except Exception as e:
            print(f"  Failed: {e}")
            print("  (If running locally, start the function: func start)")


def simulate_documents():
    """Upload sample documents to Blob Storage."""
    print("\n--- Documents (Blob Upload) ---")
    # Create some sample document content
    sample_docs = {
        "eob_anthem_sample.pdf": b"Sample EOB document content for Anthem",
        "clinical_record_patient1.pdf": b"Sample clinical record content",
        "contract_aetna_2025.pdf": b"Sample contract document content",
        "denial_letter_uhc.pdf": b"Sample denial correspondence",
    }
    for filename, content in sample_docs.items():
        upload_to_blob("documents", filename, content, filename)
        time.sleep(0.5)


def simulate_fee_schedules():
    """Upload fee schedule CSVs to Bronze container."""
    print("\n--- Fee Schedules (Batch/Blob) ---")
    for filename in ["medicare_fee_schedule_2025.csv",
                     "medicare_fee_schedule_2026.csv",
                     "payer_fee_schedule_aetna.csv",
                     "fair_health_rates.csv"]:
        raw = read_sample(filename)
        if raw:
            # Convention: fee-schedules/<payer_id>_<name>.csv
            blob_name = f"fee-schedules/{filename}"
            upload_to_blob("bronze", blob_name, raw, filename)
            time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="Simulate ingestion events")
    parser.add_argument("--claims", action="store_true", help="Send EDI 837 claims")
    parser.add_argument("--remittances", action="store_true", help="Send EDI 835 remittances")
    parser.add_argument("--patients", action="store_true", help="POST FHIR patients")
    parser.add_argument("--documents", action="store_true", help="Upload documents to Blob")
    parser.add_argument("--fee-schedules", action="store_true", help="Upload fee schedule CSVs")
    parser.add_argument("--all", action="store_true", help="Run all simulations")
    args = parser.parse_args()

    load_env()

    run_all = args.all or not any([args.claims, args.remittances, args.patients,
                                    args.documents, args.fee_schedules])

    print("=" * 60)
    print("  Medical Billing — Ingestion Simulator")
    print("=" * 60)
    print(f"  Sample data: {SAMPLE_DIR}")
    print(f"  Event Hub:   {os.environ.get('EVENTHUB_NAMESPACE', 'not set')}")

    if run_all or args.claims:
        simulate_claims()
    if run_all or args.remittances:
        simulate_remittances()
    if run_all or args.patients:
        simulate_patients()
    if run_all or args.documents:
        simulate_documents()
    if run_all or args.fee_schedules:
        simulate_fee_schedules()

    print("\n" + "=" * 60)
    print("  Simulation complete!")
    print("  Check Azure Function logs: func azure functionapp logstream <app-name>")
    print("=" * 60)


if __name__ == "__main__":
    main()
