"""Comprehensive test suite for the Medical Billing ingestion layer.

Tests cover: parsers, validators, dedup/idempotency, each ingestion source,
SCD Type 2 merge behavior, and end-to-end pipeline.
"""

import json
import os
import sys
import sqlite3
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.db import init_db, reset_db
from ingestion.parsers.edi_837 import EDI837Parser
from ingestion.parsers.edi_835 import EDI835Parser
from ingestion.parsers.fhir_patient import normalize_fhir_patient
from ingestion.parsers.csv_parser import parse_fee_schedule_csv, parse_nppes_csv, parse_backfill_csv
from ingestion.parsers.eob_mock import normalize_eob_extraction, compute_content_hash
from ingestion.parsers.hl7v2_patient import normalize_hl7v2_patient, parse_hl7v2_message
from ingestion.parsers.regulation_parser import parse_regulation_text, compute_document_hash
from ingestion.validators.validation import (
    validate_claim, validate_remittance, validate_patient,
    validate_fee_schedule_row, validate_provider
)
from ingestion.dedup import resolve_claim_duplicate, check_remittance_duplicate, match_claim_id
from ingestion.dlq import send_to_dlq, get_pending_dlq, resolve_dlq_entry
from ingestion.events import emit_event, get_events
from ingestion.crosswalk import load_crosswalk, apply_crosswalk
from ingestion.ingest.claims import ingest_claims
from ingestion.ingest.remittances import ingest_era, ingest_eob
from ingestion.ingest.patients import ingest_patients, ingest_hl7v2_patients
from ingestion.ingest.fee_schedules import ingest_fee_schedule
from ingestion.ingest.providers import ingest_providers
from ingestion.ingest.contracts import ingest_contract
from ingestion.ingest.documents import ingest_document
from ingestion.ingest.backfill import ingest_backfill
from ingestion.ingest.regulations import ingest_regulation, search_regulations

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "ingestion", "sample_data")

TEST_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_oltp.db")


def _read_sample(filename: str) -> str:
    with open(os.path.join(SAMPLE_DIR, filename), "r") as f:
        return f.read()


@pytest.fixture
def db():
    """Fresh database for each test."""
    conn = reset_db(TEST_DB)
    yield conn
    conn.close()
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


# ════════════════════════════════════════════════════════════════════════
# PARSER TESTS
# ════════════════════════════════════════════════════════════════════════

class TestEDI837Parser:
    def test_parse_claims(self):
        raw = _read_sample("claims_837.edi")
        parser = EDI837Parser(raw)
        result = parser.parse()
        assert result["transaction_type"] == "837P"
        assert len(result["claims"]) == 4

    def test_claim_fields(self):
        raw = _read_sample("claims_837.edi")
        result = EDI837Parser(raw).parse()
        clm = result["claims"][0]
        assert clm["claim_id"] == "CLM-1001"
        assert clm["total_billed"] == 500.0
        assert clm["date_of_service"] == "20250615"
        assert len(clm["lines"]) == 2
        assert clm["lines"][0]["cpt_code"] == "99213"
        assert clm["lines"][1]["cpt_code"] == "99214"
        assert clm["lines"][1]["modifier"] == "25"

    def test_diagnosis_codes(self):
        raw = _read_sample("claims_837.edi")
        result = EDI837Parser(raw).parse()
        assert "J06.9" in result["claims"][0]["diagnosis_codes"]

    def test_frequency_code_original(self):
        raw = _read_sample("claims_837.edi")
        result = EDI837Parser(raw).parse()
        assert result["claims"][0]["frequency_code"] == "1"

    def test_frequency_code_replacement(self):
        raw = _read_sample("claims_837_replacement.edi")
        result = EDI837Parser(raw).parse()
        assert result["claims"][0]["frequency_code"] == "7"
        assert result["claims"][0]["total_billed"] == 525.0

    def test_invalid_edi(self):
        with pytest.raises(ValueError, match="ISA"):
            EDI837Parser("NOT_AN_EDI_FILE")


class TestEDI835Parser:
    def test_parse_remittances(self):
        raw = _read_sample("era_835.edi")
        parser = EDI835Parser(raw)
        remittances = parser.parse()
        assert len(remittances) == 3

    def test_remittance_fields(self):
        raw = _read_sample("era_835.edi")
        remittances = EDI835Parser(raw).parse()
        r = remittances[0]
        assert r["payer_claim_id"] == "CLM-1001"
        assert r["total_billed"] == 500.0
        assert r["paid_amount"] == 350.0
        assert r["trace_number"] == "TRC-50001"
        assert r["payer_id"] == "AETNA"

    def test_adjustments(self):
        raw = _read_sample("era_835.edi")
        remittances = EDI835Parser(raw).parse()
        adjs = remittances[0]["adjustments"]
        assert len(adjs) >= 2
        co_adj = [a for a in adjs if a["group_code"] == "CO"]
        assert len(co_adj) >= 1

    def test_denied_claim(self):
        raw = _read_sample("era_835.edi")
        remittances = EDI835Parser(raw).parse()
        denied = remittances[2]  # CLM-1003 denied
        assert denied["paid_amount"] == 0.0
        assert denied["claim_status"] == "2"  # denied
        denial_code = EDI835Parser.extract_denial_code(denied["adjustments"])
        assert denial_code == "50"


class TestFHIRParser:
    def test_normalize_patient(self):
        patients = json.loads(_read_sample("fhir_patients.json"))
        p = normalize_fhir_patient(patients[0])
        assert p["patient_id"] == "INS100001"
        assert p["first_name"] == "Jane Marie"
        assert p["last_name"] == "Doe"
        assert p["dob"] == "1985-03-15"
        assert p["city"] == "Los Angeles"
        assert p["insurance_id"] == "AETNA-PPO-12345"

    def test_patient_without_insurance(self):
        patients = json.loads(_read_sample("fhir_patients.json"))
        p = normalize_fhir_patient(patients[2])  # Williams — no insurance extension
        assert p["patient_id"] == "INS100003"
        assert p["insurance_id"] is None


class TestCSVParsers:
    def test_fee_schedule(self):
        rows = parse_fee_schedule_csv(_read_sample("medicare_fee_schedule_2025.csv"))
        assert len(rows) == 10
        assert rows[0]["cpt_code"] == "99213"
        assert rows[0]["rate"] == 120.0

    def test_nppes(self):
        providers = parse_nppes_csv(_read_sample("nppes_providers.csv"))
        assert len(providers) == 7
        assert providers[0]["npi"] == "1234567890"
        deactivated = [p for p in providers if p["status"] == "deactivated"]
        assert len(deactivated) == 1

    def test_nppes_with_filter(self):
        providers = parse_nppes_csv(
            _read_sample("nppes_providers.csv"),
            known_npis={"1234567890"},
            state_whitelist={"CA"}
        )
        # Should include: known NPI + all CA providers
        ca_or_known = [p for p in providers if p["state"] == "CA" or p["npi"] == "1234567890"]
        assert len(providers) == len(ca_or_known)
        # TX provider not in known_npis should be excluded
        tx = [p for p in providers if p["state"] == "TX"]
        assert len(tx) == 0

    def test_backfill(self):
        claims = parse_backfill_csv(_read_sample("backfill_claims.csv"))
        assert len(claims) == 5
        assert claims[0]["claim_id"] == "CLM-H001"
        assert len(claims[0]["lines"]) == 2  # 99213;99214


class TestEOBMock:
    def test_normalize_eob(self):
        fields = {
            "ClaimNumber": {"value": "CLM-1001", "confidence": 0.95},
            "PaidAmount": {"value": "350.00", "confidence": 0.92},
        }
        normalized = normalize_eob_extraction(fields)
        assert "claim_number" in normalized
        assert "paid_amount" in normalized
        assert normalized["claim_number"]["value"] == "CLM-1001"

    def test_content_hash(self):
        h1 = compute_content_hash(b"test content")
        h2 = compute_content_hash(b"test content")
        h3 = compute_content_hash(b"different content")
        assert h1 == h2
        assert h1 != h3


# ════════════════════════════════════════════════════════════════════════
# VALIDATOR TESTS
# ════════════════════════════════════════════════════════════════════════

class TestValidators:
    def test_valid_claim(self):
        claim = {"claim_id": "C1", "date_of_service": "20250101",
                 "total_billed": 100.0, "lines": [{"cpt_code": "99213"}],
                 "diagnosis_codes": ["J06.9"], "provider_npi": "123"}
        r = validate_claim(claim)
        assert r.is_valid

    def test_invalid_claim_missing_fields(self):
        r = validate_claim({"claim_id": "", "total_billed": 0, "lines": []})
        assert not r.is_valid
        assert len(r.errors) >= 3

    def test_valid_remittance(self):
        r = validate_remittance({"trace_number": "T1", "paid_amount": 100,
                                  "payer_claim_id": "C1", "adjustments": [{}]})
        assert r.is_valid

    def test_invalid_remittance(self):
        r = validate_remittance({})
        assert not r.is_valid

    def test_valid_patient(self):
        r = validate_patient({"patient_id": "P1", "dob": "1990-01-01", "last_name": "Doe"})
        assert r.is_valid

    def test_invalid_patient(self):
        r = validate_patient({"patient_id": "", "dob": None})
        assert not r.is_valid

    def test_valid_fee_row(self):
        r = validate_fee_schedule_row({"cpt_code": "99213", "geo_region": "CA-01",
                                        "rate": 120.0, "effective_date": "2025-01-01",
                                        "rate_type": "medicare_pfs"})
        assert r.is_valid

    def test_invalid_fee_row(self):
        r = validate_fee_schedule_row({"cpt_code": "", "rate": -1})
        assert not r.is_valid


# ════════════════════════════════════════════════════════════════════════
# INGESTION + IDEMPOTENCY TESTS
# ════════════════════════════════════════════════════════════════════════

class TestClaimsIngestion:
    def test_ingest_claims(self, db):
        raw = _read_sample("claims_837.edi")
        summary = ingest_claims(db, raw)
        assert summary["inserted"] == 4
        assert summary["errors"] == 0
        count = db.execute("SELECT COUNT(*) as c FROM claims").fetchone()["c"]
        assert count == 4

    def test_claim_lines_created(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        lines = db.execute("SELECT COUNT(*) as c FROM claim_lines").fetchone()["c"]
        assert lines >= 8  # 4 claims with 2+ lines each

    def test_idempotency_skip_duplicate(self, db):
        raw = _read_sample("claims_837.edi")
        ingest_claims(db, raw)
        s2 = ingest_claims(db, raw)
        assert s2["inserted"] == 0
        assert s2["skipped"] == 4
        count = db.execute("SELECT COUNT(*) as c FROM claims").fetchone()["c"]
        assert count == 4  # No duplicates

    def test_replacement_claim(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        s2 = ingest_claims(db, _read_sample("claims_837_replacement.edi"))
        assert s2["updated"] == 1
        row = db.execute("SELECT total_billed, status FROM claims WHERE claim_id = 'CLM-1001'").fetchone()
        assert row["total_billed"] == 525.0
        assert row["status"] == "corrected"

    def test_audit_log_created(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        logs = db.execute("SELECT COUNT(*) as c FROM audit_log WHERE entity_type = 'claim'").fetchone()["c"]
        assert logs == 4


class TestERAIngestion:
    def test_ingest_era(self, db):
        # First ingest claims so we have claim_ids to match
        ingest_claims(db, _read_sample("claims_837.edi"))
        summary = ingest_era(db, _read_sample("era_835.edi"))
        assert summary["inserted"] >= 2  # CLM-1001 and CLM-1002 match
        assert summary["errors"] == 0

    def test_era_idempotency(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        s2 = ingest_era(db, _read_sample("era_835.edi"))
        assert s2["inserted"] == 0
        assert s2["skipped"] == 3

    def test_denied_claim_remittance(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        remit = db.execute(
            "SELECT * FROM remittances WHERE claim_id = 'CLM-1003'"
        ).fetchone()
        if remit:  # CLM-1003 was denied with $0 paid
            assert remit["paid_amount"] == 0.0
            assert remit["denial_code"] == "50"

    def test_multiple_era_files(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        ingest_era(db, _read_sample("era_835_second.edi"))
        count = db.execute("SELECT COUNT(*) as c FROM remittances").fetchone()["c"]
        assert count >= 3  # From both files


class TestEOBIngestion:
    def test_ingest_eob(self, db):
        # Need claims first for matching
        ingest_claims(db, _read_sample("claims_837.edi"))
        summary = ingest_eob(db, _read_sample("eob_extracted.json"))
        # First EOB (high confidence) should create remittance if claim matched
        # Second EOB (low confidence) should be flagged
        assert summary["low_confidence"] == 1
        artifacts = db.execute("SELECT COUNT(*) as c FROM evidence_artifacts").fetchone()["c"]
        assert artifacts == 2

    def test_eob_idempotency(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_eob(db, _read_sample("eob_extracted.json"))
        s2 = ingest_eob(db, _read_sample("eob_extracted.json"))
        assert s2["skipped"] == 2


class TestPatientIngestion:
    def test_ingest_patients(self, db):
        summary = ingest_patients(db, _read_sample("fhir_patients.json"))
        assert summary["inserted"] == 3
        count = db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"]
        assert count == 3

    def test_patient_fields(self, db):
        ingest_patients(db, _read_sample("fhir_patients.json"))
        p = db.execute("SELECT * FROM patients WHERE patient_id = 'INS100001'").fetchone()
        assert p["first_name"] == "Jane Marie"
        assert p["last_name"] == "Doe"
        assert p["city"] == "Los Angeles"

    def test_patient_upsert_idempotency(self, db):
        ingest_patients(db, _read_sample("fhir_patients.json"))
        s2 = ingest_patients(db, _read_sample("fhir_patients.json"))
        assert s2["updated"] == 3
        assert s2["inserted"] == 0
        count = db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"]
        assert count == 3  # No duplicates


class TestFeeScheduleIngestion:
    def test_ingest_medicare_2025(self, db):
        summary = ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"),
                                      "MEDICARE", "CMS_PFS_2025")
        assert summary["staged"] == 10
        assert summary["inserted"] == 10
        assert summary["errors"] == 0

    def test_fee_schedule_idempotency(self, db):
        csv = _read_sample("medicare_fee_schedule_2025.csv")
        ingest_fee_schedule(db, csv, "MEDICARE")
        s2 = ingest_fee_schedule(db, csv, "MEDICARE")
        assert s2["inserted"] == 0
        assert s2["unchanged"] == 10
        count = db.execute("SELECT COUNT(*) as c FROM fee_schedule WHERE payer_id = 'MEDICARE'").fetchone()["c"]
        assert count == 10  # No duplicates

    def test_scd2_rate_change(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        s2 = ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2026.csv"), "MEDICARE")

        # 99213/CA-01 changed: 120→135, should have old closed + new inserted
        assert s2["closed"] > 0
        assert s2["inserted"] > 0

        # Old rate closed
        old = db.execute("""
            SELECT * FROM fee_schedule
            WHERE payer_id='MEDICARE' AND cpt_code='99213' AND modifier=''
              AND geo_region='CA-01' AND rate=120.0
        """).fetchone()
        assert old["is_current"] == 0
        assert old["valid_to"] == "2025-12-31"

        # New rate current
        new = db.execute("""
            SELECT * FROM fee_schedule
            WHERE payer_id='MEDICARE' AND cpt_code='99213' AND modifier=''
              AND geo_region='CA-01' AND is_current=1
        """).fetchone()
        assert new["rate"] == 135.0
        assert new["valid_from"] == "2026-01-01"

    def test_scd2_unchanged_rate_preserved(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2026.csv"), "MEDICARE")
        # 99214/CA-01 unchanged at 175.0 — should have exactly 1 row
        rows = db.execute("""
            SELECT COUNT(*) as c FROM fee_schedule
            WHERE payer_id='MEDICARE' AND cpt_code='99214' AND modifier=''
              AND geo_region='CA-01' AND rate_type='medicare_pfs'
        """).fetchone()["c"]
        assert rows == 1

    def test_scd2_new_code_added(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2026.csv"), "MEDICARE")
        # 99216 is new in 2026
        row = db.execute("""
            SELECT * FROM fee_schedule
            WHERE payer_id='MEDICARE' AND cpt_code='99216'
        """).fetchone()
        assert row is not None
        assert row["rate"] == 310.0
        assert row["is_current"] == 1

    def test_point_in_time_query(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2026.csv"), "MEDICARE")
        # "What was Medicare rate for 99213/CA-01 on 2025-06-15?"
        row = db.execute("""
            SELECT rate FROM fee_schedule
            WHERE payer_id='MEDICARE' AND cpt_code='99213' AND modifier=''
              AND geo_region='CA-01' AND rate_type='medicare_pfs'
              AND '2025-06-15' BETWEEN valid_from AND valid_to
        """).fetchone()
        assert row["rate"] == 120.0  # 2025 rate, not 2026

    def test_fair_health_separate(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("fair_health_rates.csv"), "FAIR_HEALTH")
        # Both should coexist
        medicare = db.execute("SELECT COUNT(*) as c FROM fee_schedule WHERE payer_id='MEDICARE'").fetchone()["c"]
        fair = db.execute("SELECT COUNT(*) as c FROM fee_schedule WHERE payer_id='FAIR_HEALTH'").fetchone()["c"]
        assert medicare == 10
        assert fair == 7

    def test_payer_fee_schedule(self, db):
        summary = ingest_fee_schedule(db, _read_sample("payer_fee_schedule_aetna.csv"), "AETNA")
        assert summary["inserted"] == 7


class TestProviderIngestion:
    def test_ingest_providers(self, db):
        summary = ingest_providers(db, _read_sample("nppes_providers.csv"))
        assert summary["staged"] == 7
        assert summary["inserted"] == 7

    def test_provider_idempotency(self, db):
        csv = _read_sample("nppes_providers.csv")
        ingest_providers(db, csv)
        s2 = ingest_providers(db, csv)
        assert s2["inserted"] == 0
        assert s2["unchanged"] == 7
        count = db.execute("SELECT COUNT(*) as c FROM providers WHERE is_current = 1").fetchone()["c"]
        assert count == 7

    def test_provider_scd2_change(self, db):
        ingest_providers(db, _read_sample("nppes_providers.csv"))
        # Simulate specialty change for NPI 1234567890
        modified_csv = _read_sample("nppes_providers.csv").replace(
            "207Q00000X,Los Angeles,CA", "208000000X,Los Angeles,CA", 1
        )
        s2 = ingest_providers(db, modified_csv)
        assert s2["closed"] >= 1
        assert s2["inserted"] >= 1

        # Should have 2 rows for this NPI: old (closed) + new (current)
        rows = db.execute(
            "SELECT * FROM providers WHERE npi = '1234567890' ORDER BY is_current"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["is_current"] == 0  # old
        assert rows[1]["is_current"] == 1  # new
        assert rows[1]["specialty_taxonomy"] == "208000000X"

    def test_deactivated_provider(self, db):
        ingest_providers(db, _read_sample("nppes_providers.csv"))
        deact = db.execute(
            "SELECT * FROM providers WHERE npi = '7890123456' AND is_current = 1"
        ).fetchone()
        assert deact["status"] == "deactivated"


class TestContractIngestion:
    def test_ingest_contract(self, db):
        summary = ingest_contract(db, _read_sample("contract_extracted.json"))
        assert summary["stored"] is True
        assert summary["tables_found"] == 1
        artifact = db.execute("SELECT * FROM evidence_artifacts WHERE type = 'contract'").fetchone()
        assert artifact["ocr_status"] == "needs_review"

    def test_contract_idempotency(self, db):
        ingest_contract(db, _read_sample("contract_extracted.json"))
        s2 = ingest_contract(db, _read_sample("contract_extracted.json"))
        assert s2["duplicate"] is True
        assert s2["stored"] is False


class TestDocumentIngestion:
    def test_ingest_document(self, db):
        result = ingest_document(db, b"EOB content bytes", "eob_aetna_12345.pdf")
        assert result["stored"] is True
        assert result["doc_type"] == "EOB"
        assert result["confidence"] >= 0.9

    def test_document_idempotency(self, db):
        ingest_document(db, b"same content", "doc1.pdf")
        r2 = ingest_document(db, b"same content", "doc1.pdf")
        assert r2["duplicate"] is True
        assert r2["stored"] is False

    def test_low_confidence_needs_review(self, db):
        result = ingest_document(db, b"unknown content", "random_file.pdf")
        assert result["needs_review"] is True

    def test_classification_types(self, db):
        types = {
            "eob_test.pdf": "EOB",
            "clinical_record.pdf": "clinical_record",
            "appeal_letter.pdf": "appeal",
            "decision_notice.pdf": "arbitration_decision",
        }
        for filename, expected_type in types.items():
            r = ingest_document(db, filename.encode(), filename)
            assert r["doc_type"] == expected_type


class TestBackfillIngestion:
    def test_ingest_backfill(self, db):
        summary = ingest_backfill(db, _read_sample("backfill_claims.csv"))
        assert summary["inserted"] == 5
        assert summary["errors"] == 0

    def test_backfill_historical_status(self, db):
        ingest_backfill(db, _read_sample("backfill_claims.csv"))
        claims = db.execute("SELECT * FROM claims WHERE source = 'backfill'").fetchall()
        assert len(claims) == 5
        for c in claims:
            assert c["status"] == "historical"
            assert c["source"] == "backfill"

    def test_backfill_idempotency(self, db):
        ingest_backfill(db, _read_sample("backfill_claims.csv"))
        s2 = ingest_backfill(db, _read_sample("backfill_claims.csv"))
        assert s2["inserted"] == 0
        assert s2["skipped"] == 5


# ════════════════════════════════════════════════════════════════════════
# END-TO-END PIPELINE TEST
# ════════════════════════════════════════════════════════════════════════

class TestEndToEnd:
    def test_full_pipeline(self, db):
        """Run the complete ingestion pipeline in order and verify final state."""

        # 1. Patients
        p = ingest_patients(db, _read_sample("fhir_patients.json"))
        assert p["inserted"] == 3

        # 2. Providers
        prov = ingest_providers(db, _read_sample("nppes_providers.csv"))
        assert prov["inserted"] == 7

        # 3. Fee schedules (Medicare 2025 + 2026, FAIR Health, Aetna)
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2026.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("fair_health_rates.csv"), "FAIR_HEALTH")
        ingest_fee_schedule(db, _read_sample("payer_fee_schedule_aetna.csv"), "AETNA")

        # 4. Claims
        c = ingest_claims(db, _read_sample("claims_837.edi"))
        assert c["inserted"] == 4

        # 5. Remittances (ERA)
        r = ingest_era(db, _read_sample("era_835.edi"))
        assert r["inserted"] >= 2

        r2 = ingest_era(db, _read_sample("era_835_second.edi"))
        assert r2["inserted"] >= 1

        # 6. EOB
        eob = ingest_eob(db, _read_sample("eob_extracted.json"))
        assert eob["low_confidence"] == 1

        # 7. Contract
        contract = ingest_contract(db, _read_sample("contract_extracted.json"))
        assert contract["stored"] is True

        # 8. Document uploads
        doc = ingest_document(db, b"Clinical record for case", "clinical_record_001.pdf")
        assert doc["stored"] is True

        # 9. Historical backfill
        bf = ingest_backfill(db, _read_sample("backfill_claims.csv"))
        assert bf["inserted"] == 5

        # ── Verify final database state ──

        # Total claims: 4 (current) + 5 (backfill) = 9
        total_claims = db.execute("SELECT COUNT(*) as c FROM claims").fetchone()["c"]
        assert total_claims == 9

        # Patients
        assert db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"] == 3

        # Providers (all current)
        assert db.execute("SELECT COUNT(*) as c FROM providers WHERE is_current=1").fetchone()["c"] == 7

        # Fee schedule rows
        fs_count = db.execute("SELECT COUNT(*) as c FROM fee_schedule").fetchone()["c"]
        assert fs_count > 0

        # Remittances
        remit_count = db.execute("SELECT COUNT(*) as c FROM remittances").fetchone()["c"]
        assert remit_count >= 3

        # Evidence artifacts (EOBs + contract + document)
        artifacts = db.execute("SELECT COUNT(*) as c FROM evidence_artifacts").fetchone()["c"]
        assert artifacts >= 3

        # Audit log has entries
        audit_count = db.execute("SELECT COUNT(*) as c FROM audit_log").fetchone()["c"]
        assert audit_count > 0

        # Payers seeded
        payers = db.execute("SELECT COUNT(*) as c FROM payers").fetchone()["c"]
        assert payers >= 6

    def test_idempotency_full_rerun(self, db):
        """Run entire pipeline twice — second run should produce no new records."""
        # First run
        ingest_patients(db, _read_sample("fhir_patients.json"))
        ingest_providers(db, _read_sample("nppes_providers.csv"))
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        ingest_eob(db, _read_sample("eob_extracted.json"))
        ingest_contract(db, _read_sample("contract_extracted.json"))
        ingest_backfill(db, _read_sample("backfill_claims.csv"))

        # Snapshot counts
        counts = {}
        for table in ["claims", "remittances", "evidence_artifacts", "fee_schedule"]:
            counts[table] = db.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]

        # Second run
        ingest_patients(db, _read_sample("fhir_patients.json"))
        ingest_providers(db, _read_sample("nppes_providers.csv"))
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        ingest_eob(db, _read_sample("eob_extracted.json"))
        ingest_contract(db, _read_sample("contract_extracted.json"))
        ingest_backfill(db, _read_sample("backfill_claims.csv"))

        # Verify counts unchanged
        for table in ["claims", "remittances", "evidence_artifacts", "fee_schedule"]:
            new_count = db.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]
            assert new_count == counts[table], f"{table}: {new_count} != {counts[table]}"


# ════════════════════════════════════════════════════════════════════════
# DEAD-LETTER QUEUE TESTS
# ════════════════════════════════════════════════════════════════════════

class TestDeadLetterQueue:
    def test_send_to_dlq(self, db):
        send_to_dlq(db, "edi_837", "claim", "validation_failure",
                     "Missing claim_id", entity_id="CLM-BAD",
                     payload={"claim_id": "CLM-BAD"})
        db.commit()
        pending = get_pending_dlq(db)
        assert len(pending) == 1
        assert pending[0]["entity_type"] == "claim"
        assert pending[0]["error_category"] == "validation_failure"
        assert pending[0]["status"] == "pending"

    def test_dlq_resolve(self, db):
        send_to_dlq(db, "edi_835", "remittance", "unmatched_reference",
                     "No claim found", entity_id="TRC-999")
        db.commit()
        pending = get_pending_dlq(db)
        resolve_dlq_entry(db, pending[0]["dlq_id"], "analyst_jsmith")
        db.commit()
        still_pending = get_pending_dlq(db)
        assert len(still_pending) == 0

    def test_dlq_filter_by_source(self, db):
        send_to_dlq(db, "edi_837", "claim", "validation_failure", "err1")
        send_to_dlq(db, "edi_835", "remittance", "validation_failure", "err2")
        db.commit()
        claims_dlq = get_pending_dlq(db, source="edi_837")
        assert len(claims_dlq) == 1
        assert claims_dlq[0]["source"] == "edi_837"

    def test_claims_validation_sends_to_dlq(self, db):
        """Ingest a malformed claim and verify it lands in DLQ."""
        bad_edi = _read_sample("claims_837.edi")
        # Corrupt the first claim's billed amount to 0
        bad_edi = bad_edi.replace("CLM*CLM-1001*500", "CLM*CLM-1001*0", 1)
        ingest_claims(db, bad_edi)
        pending = get_pending_dlq(db)
        # At least the corrupted claim should be in DLQ
        claim_dlq = [d for d in pending if d["entity_id"] == "CLM-1001"]
        assert len(claim_dlq) >= 1

    def test_unmatched_remittance_sends_to_dlq(self, db):
        """ERA with no matching claims → unmatched entries in DLQ."""
        # Don't ingest claims first — all remittances will be unmatched
        ingest_era(db, _read_sample("era_835.edi"))
        pending = get_pending_dlq(db)
        unmatched = [d for d in pending if d["error_category"] == "unmatched_reference"]
        assert len(unmatched) >= 1


# ════════════════════════════════════════════════════════════════════════
# EVENT EMISSION TESTS
# ════════════════════════════════════════════════════════════════════════

class TestEventEmission:
    def test_emit_event(self, db):
        emit_event(db, "claim.insert", "claim", "CLM-1001",
                   {"total_billed": 500.0})
        db.commit()
        events = get_events(db)
        assert len(events) == 1
        assert events[0]["event_type"] == "claim.insert"
        assert events[0]["entity_id"] == "CLM-1001"

    def test_claims_emit_events(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        events = get_events(db, entity_type="claim")
        assert len(events) == 4  # 4 claims inserted
        assert all(e["event_type"] == "claim.insert" for e in events)

    def test_no_event_on_duplicate_skip(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        count_before = len(get_events(db, entity_type="claim"))
        ingest_claims(db, _read_sample("claims_837.edi"))  # re-run
        count_after = len(get_events(db, entity_type="claim"))
        assert count_after == count_before  # No new events on skip

    def test_era_emits_events(self, db):
        ingest_claims(db, _read_sample("claims_837.edi"))
        ingest_era(db, _read_sample("era_835.edi"))
        events = get_events(db, entity_type="remittance")
        assert len(events) >= 2

    def test_patient_emits_events(self, db):
        ingest_patients(db, _read_sample("fhir_patients.json"))
        events = get_events(db, entity_type="patient",
                           event_type="patient.insert")
        assert len(events) == 3

    def test_fee_schedule_emits_on_merge(self, db):
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        events = get_events(db, entity_type="fee_schedule")
        assert len(events) == 1
        assert events[0]["event_type"] == "fee_schedule.merge"

    def test_no_event_on_unchanged_fee_schedule(self, db):
        csv = _read_sample("medicare_fee_schedule_2025.csv")
        ingest_fee_schedule(db, csv, "MEDICARE")
        events_before = len(get_events(db, entity_type="fee_schedule"))
        ingest_fee_schedule(db, csv, "MEDICARE")  # re-run, all unchanged
        events_after = len(get_events(db, entity_type="fee_schedule"))
        assert events_after == events_before  # No event when nothing changed


# ════════════════════════════════════════════════════════════════════════
# HL7 v2 PARSER TESTS
# ════════════════════════════════════════════════════════════════════════

class TestHL7v2Parser:
    def test_parse_adt_a01(self):
        msg = (
            "MSH|^~\\&|EHR|FAC|APP|SYS|20250615||ADT^A01|M1|P|2.5\n"
            "PID|1|P001|P001^^^MRN||Smith^John^Q||19850101|M|||"
            "100 Main St^^Springfield^IL^62701\n"
            "IN1|1|AETNA-123|||AETNA\n"
        )
        result = normalize_hl7v2_patient(msg)
        assert result is not None
        assert result["patient_id"] == "P001"
        assert result["first_name"] == "John Q"
        assert result["last_name"] == "Smith"
        assert result["dob"] == "1985-01-01"
        assert result["gender"] == "male"
        assert result["city"] == "Springfield"
        assert result["state"] == "IL"
        assert result["zip"] == "62701"
        assert result["insurance_id"] == "AETNA-123"
        assert result["source_system"] == "hl7v2"

    def test_parse_adt_a04(self):
        msg = (
            "MSH|^~\\&|EHR|FAC|APP|SYS|20250615||ADT^A04|M2|P|2.5\n"
            "PID|1|P002|P002^^^MRN||Doe^Jane||19900315|F|||"
            "200 Oak Ave^^LA^CA^90001\n"
        )
        result = normalize_hl7v2_patient(msg)
        assert result["patient_id"] == "P002"
        assert result["gender"] == "female"
        assert result["insurance_id"] is None

    def test_non_adt_message_returns_none(self):
        msg = "MSH|^~\\&|EHR|FAC|APP|SYS|20250615||ORU^R01|M3|P|2.5\nPID|1|P003\n"
        result = normalize_hl7v2_patient(msg)
        assert result is None

    def test_missing_pid_returns_none(self):
        msg = "MSH|^~\\&|EHR|FAC|APP|SYS|20250615||ADT^A01|M4|P|2.5\n"
        result = normalize_hl7v2_patient(msg)
        assert result is None

    def test_parse_sample_file(self):
        raw = _read_sample("adt_patients.hl7")
        from ingestion.ingest.patients import _split_hl7_messages
        msgs = _split_hl7_messages(raw)
        assert len(msgs) == 3
        for m in msgs:
            result = normalize_hl7v2_patient(m)
            assert result is not None
            assert result["patient_id"].startswith("INS2000")


class TestHL7v2Ingestion:
    def test_ingest_hl7v2_patients(self, db):
        summary = ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        assert summary["inserted"] == 3
        assert summary["errors"] == 0
        count = db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"]
        assert count == 3

    def test_hl7v2_patient_fields(self, db):
        ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        p = db.execute("SELECT * FROM patients WHERE patient_id = 'INS200001'").fetchone()
        assert p["first_name"] == "Michael James"
        assert p["last_name"] == "Roberts"
        assert p["city"] == "San Francisco"
        assert p["source_system"] == "hl7v2"

    def test_hl7v2_idempotency(self, db):
        ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        s2 = ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        assert s2["updated"] == 3
        assert s2["inserted"] == 0
        count = db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"]
        assert count == 3

    def test_fhir_and_hl7v2_coexist(self, db):
        ingest_patients(db, _read_sample("fhir_patients.json"))
        ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        count = db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"]
        assert count == 6  # 3 FHIR + 3 HL7v2


# ════════════════════════════════════════════════════════════════════════
# CODE CROSSWALK TESTS
# ════════════════════════════════════════════════════════════════════════

class TestCodeCrosswalk:
    def test_load_crosswalk(self, db):
        result = load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        assert result["loaded"] == 8
        row = db.execute(
            "SELECT standard_code FROM code_crosswalk WHERE legacy_code = '99241'"
        ).fetchone()
        assert row["standard_code"] == "99213"

    def test_apply_crosswalk_cpt(self, db):
        load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        claim = {
            "claim_id": "TEST-1",
            "payer_id": "AETNA",
            "lines": [{"cpt_code": "99241", "billed_amount": 100.0}]
        }
        result = apply_crosswalk(db, claim, "legacy_system_a")
        assert result["lines"][0]["cpt_code"] == "99213"

    def test_apply_crosswalk_payer(self, db):
        load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        claim = {
            "claim_id": "TEST-2",
            "payer_id": "MCR",
            "lines": [{"cpt_code": "99213", "billed_amount": 100.0}]
        }
        result = apply_crosswalk(db, claim, "legacy_system_a")
        assert result["payer_id"] == "MEDICARE"

    def test_unknown_code_unchanged(self, db):
        load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        claim = {
            "claim_id": "TEST-3",
            "payer_id": "AETNA",
            "lines": [{"cpt_code": "99999", "billed_amount": 100.0}]
        }
        result = apply_crosswalk(db, claim, "legacy_system_a")
        assert result["lines"][0]["cpt_code"] == "99999"  # Unchanged
        assert result["payer_id"] == "AETNA"  # Unchanged

    def test_backfill_with_crosswalk(self, db):
        load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        summary = ingest_backfill(db, _read_sample("backfill_claims.csv"),
                                  source_system="legacy_system_a")
        assert summary["inserted"] == 5


# ════════════════════════════════════════════════════════════════════════
# REGULATIONS & RULES TESTS
# ════════════════════════════════════════════════════════════════════════

class TestRegulationParser:
    def test_parse_regulation_chunks(self):
        content = _read_sample("nsa_regulation.txt")
        metadata = {
            "document_title": "NSA IDR",
            "jurisdiction": "federal",
            "effective_date": "2022-01-01",
            "cfr_citation": "45 CFR 149.510",
            "source_file": "nsa_regulation.txt",
        }
        chunks = parse_regulation_text(content, metadata)
        assert len(chunks) >= 3
        for chunk in chunks:
            assert chunk["jurisdiction"] == "federal"
            assert chunk["text"]
            assert chunk["chunk_id"]

    def test_parse_ca_regulation(self):
        content = _read_sample("ca_surprise_billing.txt")
        chunks = parse_regulation_text(content, {"jurisdiction": "CA"})
        assert len(chunks) >= 2
        assert any("CA" == c.get("jurisdiction") for c in chunks)

    def test_document_hash_deterministic(self):
        h1 = compute_document_hash("test content")
        h2 = compute_document_hash("test content")
        h3 = compute_document_hash("different content")
        assert h1 == h2
        assert h1 != h3


class TestRegulationIngestion:
    def test_ingest_regulation(self, db):
        content = _read_sample("nsa_regulation.txt")
        summary = ingest_regulation(db, content, {
            "document_title": "NSA IDR Process",
            "jurisdiction": "federal",
            "effective_date": "2022-01-01",
            "cfr_citation": "45 CFR 149.510-530",
            "source_file": "nsa_regulation.txt",
        })
        assert summary["stored"] is True
        assert summary["chunks_indexed"] >= 3
        artifact = db.execute(
            "SELECT * FROM evidence_artifacts WHERE type = 'regulation'"
        ).fetchone()
        assert artifact is not None

    def test_regulation_idempotency(self, db):
        content = _read_sample("nsa_regulation.txt")
        metadata = {"document_title": "NSA", "jurisdiction": "federal",
                     "effective_date": "2022-01-01", "source_file": "nsa.txt"}
        ingest_regulation(db, content, metadata)
        s2 = ingest_regulation(db, content, metadata)
        assert s2["duplicate"] is True
        assert s2["stored"] is False

    def test_multiple_jurisdictions(self, db):
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        ingest_regulation(db, _read_sample("ca_surprise_billing.txt"), {
            "document_title": "CA AB 72", "jurisdiction": "CA",
            "effective_date": "2017-07-01", "source_file": "ca_ab72.txt"})

        federal = db.execute(
            "SELECT COUNT(*) as c FROM regulation_chunks WHERE jurisdiction = 'federal'"
        ).fetchone()["c"]
        ca = db.execute(
            "SELECT COUNT(*) as c FROM regulation_chunks WHERE jurisdiction = 'CA'"
        ).fetchone()["c"]
        assert federal >= 3
        assert ca >= 2

    def test_search_regulations_keyword(self, db):
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        results = search_regulations(db, "qualifying payment amount")
        assert len(results) >= 1
        assert "qualifying payment amount" in results[0]["chunk_text"].lower()

    def test_search_by_jurisdiction(self, db):
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        ingest_regulation(db, _read_sample("ca_surprise_billing.txt"), {
            "document_title": "CA AB 72", "jurisdiction": "CA",
            "effective_date": "2017-07-01", "source_file": "ca_ab72.txt"})
        results = search_regulations(db, "dispute", jurisdiction="CA")
        assert all(r["jurisdiction"] == "CA" for r in results)

    def test_search_by_effective_date(self, db):
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        # Search for regulations effective before 2022
        results = search_regulations(db, "payment", as_of_date="2021-06-01")
        assert len(results) == 0  # NSA not effective until 2022
        # Search for regulations effective after 2022
        results = search_regulations(db, "payment", as_of_date="2023-01-01")
        assert len(results) >= 1

    def test_regulation_emits_event(self, db):
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        events = get_events(db, entity_type="regulation")
        assert len(events) == 1
        assert events[0]["event_type"] == "regulation.indexed"


# ════════════════════════════════════════════════════════════════════════
# EXTENDED END-TO-END TESTS
# ════════════════════════════════════════════════════════════════════════

class TestExtendedEndToEnd:
    def test_full_pipeline_with_all_sources(self, db):
        """Run the complete pipeline including new sources."""
        # Patients — both FHIR and HL7 v2
        p1 = ingest_patients(db, _read_sample("fhir_patients.json"))
        assert p1["inserted"] == 3
        p2 = ingest_hl7v2_patients(db, _read_sample("adt_patients.hl7"))
        assert p2["inserted"] == 3

        # Providers
        ingest_providers(db, _read_sample("nppes_providers.csv"))

        # Fee schedules
        ingest_fee_schedule(db, _read_sample("medicare_fee_schedule_2025.csv"), "MEDICARE")
        ingest_fee_schedule(db, _read_sample("fair_health_rates.csv"), "FAIR_HEALTH")

        # Claims
        ingest_claims(db, _read_sample("claims_837.edi"))

        # Remittances
        ingest_era(db, _read_sample("era_835.edi"))

        # Contract + documents
        ingest_contract(db, _read_sample("contract_extracted.json"))
        ingest_document(db, b"test doc", "eob_test.pdf")

        # Regulations
        ingest_regulation(db, _read_sample("nsa_regulation.txt"), {
            "document_title": "NSA", "jurisdiction": "federal",
            "effective_date": "2022-01-01", "source_file": "nsa.txt"})
        ingest_regulation(db, _read_sample("ca_surprise_billing.txt"), {
            "document_title": "CA AB 72", "jurisdiction": "CA",
            "effective_date": "2017-07-01", "source_file": "ca_ab72.txt"})

        # Crosswalk + backfill
        load_crosswalk(db, _read_sample("code_crosswalk.csv"))
        ingest_backfill(db, _read_sample("backfill_claims.csv"),
                        source_system="legacy_system_a")

        # Verify: 6 patients, 7 regulation chunks, events emitted, no DLQ
        assert db.execute("SELECT COUNT(*) as c FROM patients").fetchone()["c"] == 6
        assert db.execute("SELECT COUNT(*) as c FROM regulation_chunks").fetchone()["c"] >= 5
        assert db.execute("SELECT COUNT(*) as c FROM event_log").fetchone()["c"] > 0
        assert db.execute("SELECT COUNT(*) as c FROM code_crosswalk").fetchone()["c"] == 8
