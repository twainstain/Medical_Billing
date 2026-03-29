# Medical Billing Arbitration — Ingestion Layer

Data ingestion pipeline for healthcare billing arbitration. Handles 10 source types across 7 formats, with validation, deduplication, SCD Type 2 history, audit logging, and dead-letter queue for failed records.

## Architecture

```
Source Systems                    Parsers              Validators         OLTP Database
─────────────────               ──────────           ────────────       ──────────────
EDI 837 (claims)     ──┐
EDI 835 (remittances)──┤        ┌─────────────┐     ┌────────────┐    ┌──────────────┐
FHIR R4 (patients)   ──┤   ──►  │ Format       │ ──► │ Validate   │ ─► │ Dedup +      │
HL7 v2 ADT (patients)──┤        │ Parsers (7)  │     │ Rules (5)  │    │ Insert/Update│
CSV (fee schedules)  ──┤        └─────────────┘     └────────────┘    └──────┬───────┘
CSV (providers/NPPES)──┤                                  │                   │
CSV (backfill claims)──┤                                  │ fail              │ success
EOB PDF (Doc Intel)  ──┤                                  ▼                   ▼
Contract PDF         ──┤                           ┌────────────┐     ┌──────────────┐
Regulation text      ──┘                           │ Dead-Letter │     │ Audit Log +  │
                                                   │ Queue (DLQ) │     │ Event Emit   │
                                                   └────────────┘     └──────────────┘
```

## Ingestion Sources (10)

| # | Source | Format | Parser | Trigger (Cloud) | Target Table | SCD |
|---|--------|--------|--------|-----------------|-------------|-----|
| 1 | Claims | EDI 837 | `edi_837.py` | Event Hub | `claims` + `claim_lines` | Type 1 |
| 2 | Remittances | EDI 835 | `edi_835.py` | Event Hub | `remittances` | Append |
| 3 | Patients (FHIR) | FHIR R4 JSON | `fhir_patient.py` | HTTP POST | `patients` | Type 1 |
| 4 | Patients (HL7) | HL7 v2 ADT | `hl7v2_patient.py` | HTTP POST | `patients` | Type 1 |
| 5 | Fee Schedules | CSV | `csv_parser.py` | Timer (daily) | `fee_schedule` | Type 2 |
| 6 | Providers | NPPES CSV | `csv_parser.py` | Timer (batch) | `providers` | Type 2 |
| 7 | EOB PDFs | Doc Intelligence JSON | `eob_mock.py` | Event Hub | `remittances` + `evidence_artifacts` | Append |
| 8 | Contracts | Doc Intelligence JSON | `eob_mock.py` | Blob trigger | `evidence_artifacts` | Append |
| 9 | Backfill Claims | CSV | `csv_parser.py` | Manual | `claims` + `claim_lines` | Type 1 |
| 10 | Regulations | Plain text | `regulation_parser.py` | Manual | `evidence_artifacts` + `regulation_chunks` | Append |

## Data Flow Per Source

### 1. Claims (EDI 837)

```
EDI 837 file → EDI837Parser.parse()
  → Extracts: claim_id, provider_npi, patient_id, payer_id, date_of_service,
    total_billed, diagnosis_codes, service lines (CPT, modifier, units, amount)
  → validate_claim(): claim_id, DOS, total_billed > 0, lines with CPT codes
  → resolve_claim_duplicate(): frequency_code 1=insert, 7=update, 8=void
  → INSERT/UPDATE claims + claim_lines
  → emit_event("claim.insert" | "claim.update" | "claim.void")
```

**EDI 837 Segments Parsed:**
| Segment | Field | Description |
|---------|-------|-------------|
| NM1*85 | Provider NPI | Billing provider |
| NM1*IL | Patient ID | Subscriber/patient |
| NM1*PR | Payer ID | Insurance company |
| CLM | claim_id, total_billed | Claim header |
| CLM-5 (component 3) | frequency_code | 1=original, 7=replacement, 8=void |
| DTP*472 | date_of_service | Service date |
| HI | diagnosis_codes | ICD-10 codes (colon-separated) |
| SV1 (837P) / SV2 (837I) | cpt_code, modifier, units, amount | Service lines |

### 2. Remittances (EDI 835)

```
EDI 835 file → EDI835Parser.parse()
  → Extracts: payer_claim_id, trace_number, paid_amount, allowed_amount,
    adjustments (CARC/RARC), denial_code, service lines
  → validate_remittance(): trace_number, paid_amount, payer_claim_id
  → match_claim_id(): payer_claim_id → canonical claim_id
    (direct match → normalized → alias table)
  → check_remittance_duplicate(): (claim_id, trace_number, payer_id)
  → INSERT remittances
  → emit_event("remittance.received")
```

**EDI 835 Segments Parsed:**
| Segment | Field | Description |
|---------|-------|-------------|
| TRN | trace_number | Payment trace |
| N1*PR | payer_id | Payer identifier |
| CLP | payer_claim_id, paid_amount, patient_responsibility | Claim payment |
| CAS | group_code, reason_code, amount | Adjustment reasons (CO/PR/OA) |
| SVC | cpt_code, billed, paid | Service line detail |

### 3. Patients (FHIR R4)

```
FHIR JSON → normalize_fhir_patient()
  → Extracts: MRN (identifier where type="MR"), name (use="official"),
    DOB, gender, address (use="home"), insurance_id (extension)
  → validate_patient(): patient_id, DOB required
  → SCD Type 1: INSERT or UPDATE on patient_id
  → emit_event("patient.insert" | "patient.update")
```

### 4. Patients (HL7 v2)

```
HL7 v2 ADT message → parse_hl7v2_message() → normalize_hl7v2_patient()
  → Validates: MSH + PID segments present, MSH-9 contains "ADT"
  → Extracts from PID: patient_id (PID-3), name (PID-5), DOB (PID-7),
    gender (PID-8), address (PID-11), insurance_id (IN1-2)
  → Same validation + insert flow as FHIR patients
```

### 5. Fee Schedules (CSV — SCD Type 2)

```
CSV file → parse_fee_schedule_csv()
  → Extracts: cpt_code, modifier, geo_region, rate, rate_type, effective_date
  → validate_fee_schedule_row(): cpt_code, geo_region, rate >= 0, effective_date, rate_type
  → Stage to stg_fee_schedule
  → SCD Type 2 MERGE: close current rows where rate changed, insert new versions
  → emit_event("fee_schedule.merge")
```

**SCD Type 2 Logic:**
1. Match on (payer_id, cpt_code, modifier, geo_region, rate_type)
2. If rate changed: close existing row (valid_to = effective_date - 1 day, is_current = 0)
3. Insert new row (valid_from = effective_date, valid_to = '9999-12-31', is_current = 1)
4. If unchanged: skip

### 6. Providers (NPPES CSV — SCD Type 2)

```
NPPES CSV → parse_nppes_csv()
  → Filters: Entity Type = 1 (individuals only)
  → Optional filters: known_npis set, state_whitelist
  → validate_provider(): NPI required
  → Stage to stg_providers
  → SCD Type 2 MERGE: track changes to specialty, status, state
  → emit_event("provider.merge")
```

### 7. EOB PDFs (Document Intelligence)

```
Doc Intelligence JSON → parse_eob_extractions()
  → Normalizes payer-specific field names via FIELD_MAPPINGS
  → check_evidence_duplicate(): SHA-256 hash
  → Store as evidence_artifact
  → If avg_confidence >= 0.9: match claim_id, create remittance
  → If avg_confidence < 0.9: flag for manual review
```

### 8. Contracts (Document Intelligence)

```
Doc Intelligence JSON → parse_contract_extraction()
  → Extracts: payer_id, provider_npi, effective_date, rate tables
  → check_evidence_duplicate(): SHA-256 hash
  → Store as evidence_artifact (always ocr_status='needs_review')
```

### 9. Backfill Claims (CSV)

```
CSV → parse_backfill_csv()
  → Splits semicolon-separated CPT codes into individual lines
  → Distributes total_billed evenly across lines
  → apply_crosswalk(): maps legacy codes if source_system provided
  → validate_claim() → check_backfill_duplicate()
  → INSERT with status='historical', source='backfill'
```

### 10. Regulations (Plain Text)

```
Regulation text → parse_regulation_text()
  → Chunks by section headers (§, PART, SUBPART, Section, Article)
  → 200-token overlap between adjacent chunks
  → check_evidence_duplicate(): SHA-256 hash
  → Store as evidence_artifact + index chunks in regulation_chunks
```

## Parsers (7)

| Parser | File | Input Format | Key Output Fields |
|--------|------|-------------|-------------------|
| EDI 837 | `parsers/edi_837.py` | X12 837P/837I | claim_id, lines[], diagnosis_codes, frequency_code |
| EDI 835 | `parsers/edi_835.py` | X12 835 ERA | payer_claim_id, paid_amount, adjustments[], denial_code |
| FHIR R4 | `parsers/fhir_patient.py` | JSON | patient_id (MRN), name, DOB, address, insurance_id |
| HL7 v2 | `parsers/hl7v2_patient.py` | HL7 v2 ADT | patient_id (PID-3), name (PID-5), DOB (PID-7) |
| CSV | `parsers/csv_parser.py` | CSV (3 variants) | fee_schedule rows, provider rows, backfill claims |
| EOB/Contract | `parsers/eob_mock.py` | Doc Intelligence JSON | normalized fields, confidence scores, content_hash |
| Regulation | `parsers/regulation_parser.py` | Plain text | section chunks with metadata + overlap |

## Validators (5)

| Validator | Blocking Errors | Warnings Only |
|-----------|----------------|---------------|
| `validate_claim` | claim_id, date_of_service, total_billed > 0, lines with CPT | diagnosis_codes, provider_npi, payer_id |
| `validate_remittance` | trace_number, paid_amount, payer_claim_id | adjustment reasons |
| `validate_patient` | patient_id, DOB | last_name, insurance_id |
| `validate_fee_schedule_row` | cpt_code, geo_region, rate >= 0, effective_date, rate_type | — |
| `validate_provider` | NPI | specialty_taxonomy, state |

## Deduplication Rules

| Entity | Key | Action on Duplicate |
|--------|-----|-------------------|
| Claims | claim_id + frequency_code | 1=skip, 7=update, 8=void |
| Remittances | (claim_id, trace_number, payer_id) | Skip |
| Documents | SHA-256 content_hash | Skip |
| Backfill | (claim_id, DOS, provider_npi, total_billed) | Skip |
| Fee Schedules | (payer_id, cpt_code, modifier, geo_region, rate_type) | SCD2 merge |
| Providers | NPI | SCD2 merge |

## Dead-Letter Queue (DLQ)

Failed records are sent to the DLQ for analyst triage:

| Error Category | Description | Auto-Retry |
|---------------|-------------|------------|
| `parse_failure` | Malformed source data (EDI/HL7 syntax) | No |
| `validation_failure` | Missing required field or invalid value | No |
| `transient_failure` | Temporary infrastructure error | Yes |
| `unmatched_reference` | References unknown entity (e.g., unknown claim_id in remittance) | No |

## Event Types

| Event | Emitted When |
|-------|-------------|
| `claim.insert` | New claim ingested |
| `claim.update` | Claim corrected (frequency_code=7) |
| `claim.void` | Claim voided (frequency_code=8) |
| `patient.insert` | New patient |
| `patient.update` | Patient demographics updated |
| `remittance.received` | ERA or EOB processed |
| `provider.merge` | Provider SCD2 merge completed |
| `fee_schedule.merge` | Fee schedule SCD2 merge completed |
| `document.upload` | Document classified and stored |
| `contract.upload` | Contract extraction stored |
| `regulation.indexed` | Regulation document chunked |

Events are never emitted on duplicate skips — only on successful insert/update.

## Sample Data Files (15)

| File | Format | Contents |
|------|--------|----------|
| `claims_837.edi` | EDI X12 | 5 claims (frequency_code=1, original) |
| `claims_837_replacement.edi` | EDI X12 | Replacement claim (frequency_code=7) |
| `claims_837_batch2.edi` | EDI X12 | Additional claim batch |
| `claims_837_batch3.edi` | EDI X12 | Additional claim batch |
| `era_835.edi` | EDI X12 | 3 remittances with adjustments |
| `era_835_second.edi` | EDI X12 | Additional ERA batch |
| `era_835_batch2.edi` | EDI X12 | Additional ERA batch |
| `era_835_batch3.edi` | EDI X12 | Additional ERA batch |
| `fhir_patients.json` | FHIR R4 JSON | 3 patients with MRN, name, address, insurance |
| `adt_patients.hl7` | HL7 v2 | ADT messages (A01 admit, A04 register, A08 update) |
| `medicare_fee_schedule_2025.csv` | CSV | Medicare PFS rates by CPT + geo_region |
| `medicare_fee_schedule_2026.csv` | CSV | Updated Medicare rates (SCD2 version test) |
| `fair_health_rates.csv` | CSV | FAIR Health P80 benchmark rates |
| `payer_fee_schedule_aetna.csv` | CSV | Aetna contracted rates |
| `nppes_providers.csv` | CSV | NPPES provider registry extract |
| `eob_extracted.json` | JSON | Mock Doc Intelligence EOB extraction |
| `contract_extracted.json` | JSON | Mock Doc Intelligence contract extraction |
| `backfill_claims.csv` | CSV | Historical claims for backfill |
| `backfill_claims_batch2.csv` | CSV | Additional backfill batch |
| `code_crosswalk.csv` | CSV | Legacy → standard code mappings |
| `nsa_regulation.txt` | Text | No Surprises Act regulation text |
| `ca_surprise_billing.txt` | Text | California surprise billing law |

## Local → Cloud Mapping

```
LOCAL (ingestion/)                    CLOUD (azure-test-env/functions/)
──────────────────────               ────────────────────────────────────
sqlite3 (db.py)               →     pyodbc → Azure SQL (shared/db.py)
event_log table (events.py)   →     Azure Event Hubs (shared/events.py)
DLQ table (dlq.py)            →     Azure SQL DLQ (shared/dlq.py)
dedup.py                      →     Same logic, pyodbc (shared/dedup.py)
audit.py                      →     Same logic, pyodbc (shared/audit.py)
parsers/*                     →     Copied unchanged (pure logic)
validators/*                  →     Copied unchanged (pure logic)
ingest/claims.py              →     Event Hub trigger → Azure SQL
ingest/remittances.py         →     Event Hub trigger → Azure SQL
ingest/documents.py           →     Blob trigger + Doc Intelligence
ingest/patients.py            →     HTTP trigger (FHIR webhook)
ingest/fee_schedules.py       →     Timer trigger (daily batch)
Manual CLI execution          →     Event-driven Azure Functions
```

## Running Locally

```bash
cd ingestion
python -c "
from db import init_db
conn = init_db()
from ingest.claims import ingest_claims
result = ingest_claims(conn, open('sample_data/claims_837.edi').read())
print(result)
"
```

## Tests

172 tests in `tests/test_ingestion.py` covering all parsers, validators, dedup, and ingestion sources.

```bash
cd /path/to/Medical_Billing
python3 -m pytest tests/test_ingestion.py -v
```
