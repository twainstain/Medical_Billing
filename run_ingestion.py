#!/usr/bin/env python3
"""
Run the complete ingestion pipeline with sample data.
Produces a populated OLTP database (oltp.db) for downstream OLAP/analytics layers.

Usage:
    python run_ingestion.py
"""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.db import reset_db
from ingestion.ingest.patients import ingest_patients, ingest_hl7v2_patients
from ingestion.ingest.providers import ingest_providers
from ingestion.ingest.fee_schedules import ingest_fee_schedule
from ingestion.ingest.claims import ingest_claims
from ingestion.ingest.remittances import ingest_era, ingest_eob
from ingestion.ingest.contracts import ingest_contract
from ingestion.ingest.documents import ingest_document
from ingestion.ingest.backfill import ingest_backfill
from ingestion.ingest.regulations import ingest_regulation
from ingestion.crosswalk import load_crosswalk

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "ingestion", "sample_data")
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oltp.db")


def read(filename: str) -> str:
    with open(os.path.join(SAMPLE_DIR, filename), "r") as f:
        return f.read()


def main():
    logger.info("=" * 70)
    logger.info("MEDICAL BILLING ARBITRATION — INGESTION PIPELINE")
    logger.info("=" * 70)

    # Initialize fresh OLTP database
    logger.info("\n--- Initializing OLTP database: %s", DB_PATH)
    conn = reset_db(DB_PATH)

    # ── Stage 1: Reference Data ──────────────────────────────────────
    logger.info("\n--- Stage 1: Reference Data (Patients, Providers, Fee Schedules)")

    # Patients (FHIR)
    result = ingest_patients(conn, read("fhir_patients.json"))
    logger.info("  Patients (FHIR): %s", result)

    # Patients (HL7 v2)
    result = ingest_hl7v2_patients(conn, read("adt_patients.hl7"))
    logger.info("  Patients (HL7 v2): %s", result)

    # Providers
    result = ingest_providers(conn, read("nppes_providers.csv"))
    logger.info("  Providers (NPPES): %s", result)

    # Fee Schedules — Medicare 2025
    result = ingest_fee_schedule(conn, read("medicare_fee_schedule_2025.csv"),
                                 "MEDICARE", "CMS_PFS_2025")
    logger.info("  Medicare 2025: %s", result)

    # Fee Schedules — Medicare 2026 (SCD Type 2 rate changes)
    result = ingest_fee_schedule(conn, read("medicare_fee_schedule_2026.csv"),
                                 "MEDICARE", "CMS_PFS_2026")
    logger.info("  Medicare 2026 (SCD2 update): %s", result)

    # Fee Schedules — FAIR Health
    result = ingest_fee_schedule(conn, read("fair_health_rates.csv"),
                                 "FAIR_HEALTH", "FAIR_HEALTH_2025")
    logger.info("  FAIR Health: %s", result)

    # Fee Schedules — Aetna contracted rates
    result = ingest_fee_schedule(conn, read("payer_fee_schedule_aetna.csv"),
                                 "AETNA", "AETNA_CONTRACTED_2025")
    logger.info("  Aetna contracted: %s", result)

    # ── Stage 2: Claims ──────────────────────────────────────────────
    logger.info("\n--- Stage 2: Claims (EDI 837)")

    result = ingest_claims(conn, read("claims_837.edi"))
    logger.info("  Claims batch 1: %s", result)

    # Replacement claim
    result = ingest_claims(conn, read("claims_837_replacement.edi"))
    logger.info("  Claims replacement: %s", result)

    # Additional claim batches
    result = ingest_claims(conn, read("claims_837_batch2.edi"))
    logger.info("  Claims batch 2: %s", result)

    result = ingest_claims(conn, read("claims_837_batch3.edi"))
    logger.info("  Claims batch 3: %s", result)

    # ── Stage 3: Remittances ─────────────────────────────────────────
    logger.info("\n--- Stage 3: Remittances (ERA 835 + EOB PDF)")

    result = ingest_era(conn, read("era_835.edi"))
    logger.info("  ERA (Aetna): %s", result)

    result = ingest_era(conn, read("era_835_second.edi"))
    logger.info("  ERA (UHC): %s", result)

    # Additional ERA batches matching new claims
    result = ingest_era(conn, read("era_835_batch2.edi"))
    logger.info("  ERA batch 2 (BCBS/UHC/CIGNA/Medicaid/Aetna): %s", result)

    result = ingest_era(conn, read("era_835_batch3.edi"))
    logger.info("  ERA batch 3 (Aetna/BCBS/UHC/CIGNA/Medicaid): %s", result)

    result = ingest_eob(conn, read("eob_extracted.json"))
    logger.info("  EOB PDFs: %s", result)

    # ── Stage 4: Contracts & Documents ───────────────────────────────
    logger.info("\n--- Stage 4: Contracts & Documents")

    result = ingest_contract(conn, read("contract_extracted.json"))
    logger.info("  Contract (Aetna): %s", result)

    result = ingest_document(conn, b"Clinical record: patient INS100001, encounter 2025-06-15",
                             "clinical_record_clm1001.pdf", uploaded_by="analyst_jsmith")
    logger.info("  Document upload (clinical): %s", result)

    result = ingest_document(conn, b"Appeal letter for CLM-1003 denial",
                             "appeal_clm1003.pdf", uploaded_by="analyst_jsmith")
    logger.info("  Document upload (appeal): %s", result)

    # ── Stage 5: Regulations ──────────────────────────────────────────
    logger.info("\n--- Stage 5: Regulations & Rules")

    result = ingest_regulation(conn, read("nsa_regulation.txt"), {
        "document_title": "No Surprises Act — IDR Process",
        "jurisdiction": "federal",
        "effective_date": "2022-01-01",
        "cfr_citation": "45 CFR 149.510-530",
        "source_file": "nsa_regulation.txt",
    })
    logger.info("  NSA Federal: %s", result)

    result = ingest_regulation(conn, read("ca_surprise_billing.txt"), {
        "document_title": "California AB 72 — Surprise Billing",
        "jurisdiction": "CA",
        "effective_date": "2017-07-01",
        "cfr_citation": "HSC 1371.30",
        "source_file": "ca_surprise_billing.txt",
    })
    logger.info("  CA AB 72: %s", result)

    # ── Stage 6: Code Crosswalk + Historical Backfill ──────────────
    logger.info("\n--- Stage 6: Code Crosswalk + Historical Claims Backfill")

    cw = load_crosswalk(conn, read("code_crosswalk.csv"))
    logger.info("  Crosswalk loaded: %s", cw)

    result = ingest_backfill(conn, read("backfill_claims.csv"),
                             source_system="legacy_system_a")
    logger.info("  Backfill batch 1: %s", result)

    result = ingest_backfill(conn, read("backfill_claims_batch2.csv"),
                             source_system="legacy_system_b")
    logger.info("  Backfill batch 2: %s", result)

    # ── Stage 7: Idempotency Verification ────────────────────────────
    logger.info("\n--- Stage 7: Idempotency Verification (re-run all)")

    r = ingest_claims(conn, read("claims_837.edi"))
    assert r["inserted"] == 0, f"Claims idempotency failed: {r}"

    r = ingest_era(conn, read("era_835.edi"))
    assert r["inserted"] == 0, f"ERA idempotency failed: {r}"

    r = ingest_fee_schedule(conn, read("medicare_fee_schedule_2026.csv"), "MEDICARE")
    assert r["inserted"] == 0, f"Fee schedule idempotency failed: {r}"

    r = ingest_providers(conn, read("nppes_providers.csv"))
    assert r["inserted"] == 0, f"Provider idempotency failed: {r}"

    r = ingest_backfill(conn, read("backfill_claims.csv"))
    assert r["inserted"] == 0, f"Backfill idempotency failed: {r}"

    logger.info("  All idempotency checks passed!")

    # ── Final Summary ────────────────────────────────────────────────
    logger.info("\n" + "=" * 70)
    logger.info("OLTP DATABASE SUMMARY")
    logger.info("=" * 70)

    tables = [
        "patients", "providers", "payers", "claims", "claim_lines",
        "remittances", "fee_schedule", "evidence_artifacts", "audit_log",
        "event_log", "dead_letter_queue", "regulation_chunks",
    ]
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]
        logger.info("  %-25s %d rows", table, count)

    # SCD Type 2 stats
    current_fs = conn.execute("SELECT COUNT(*) as c FROM fee_schedule WHERE is_current = 1").fetchone()["c"]
    historical_fs = conn.execute("SELECT COUNT(*) as c FROM fee_schedule WHERE is_current = 0").fetchone()["c"]
    logger.info("\n  Fee Schedule: %d current + %d historical = %d total",
                current_fs, historical_fs, current_fs + historical_fs)

    current_prov = conn.execute("SELECT COUNT(*) as c FROM providers WHERE is_current = 1").fetchone()["c"]
    logger.info("  Providers: %d current", current_prov)

    active_claims = conn.execute("SELECT COUNT(*) as c FROM claims WHERE source != 'backfill'").fetchone()["c"]
    backfill_claims = conn.execute("SELECT COUNT(*) as c FROM claims WHERE source = 'backfill'").fetchone()["c"]
    logger.info("  Claims: %d active + %d historical backfill", active_claims, backfill_claims)

    # Sample point-in-time query
    logger.info("\n--- Sample Query: Medicare rate for 99213/CA-01 on 2025-06-15")
    row = conn.execute("""
        SELECT rate, valid_from, valid_to FROM fee_schedule
        WHERE payer_id='MEDICARE' AND cpt_code='99213' AND modifier=''
          AND geo_region='CA-01' AND rate_type='medicare_pfs'
          AND '2025-06-15' BETWEEN valid_from AND valid_to
    """).fetchone()
    if row:
        logger.info("  Rate: $%.2f (valid %s to %s)", row["rate"], row["valid_from"], row["valid_to"])

    logger.info("\n--- Sample Query: Medicare rate for 99213/CA-01 on 2026-03-01")
    row = conn.execute("""
        SELECT rate, valid_from, valid_to FROM fee_schedule
        WHERE payer_id='MEDICARE' AND cpt_code='99213' AND modifier=''
          AND geo_region='CA-01' AND rate_type='medicare_pfs'
          AND '2026-03-01' BETWEEN valid_from AND valid_to
    """).fetchone()
    if row:
        logger.info("  Rate: $%.2f (valid %s to %s)", row["rate"], row["valid_from"], row["valid_to"])

    logger.info("\n" + "=" * 70)
    logger.info("OLTP database ready at: %s", os.path.abspath(DB_PATH))
    logger.info("This database can now be used for CDC → OLAP Lakehouse pipeline.")
    logger.info("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
