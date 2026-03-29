# Medical Billing Arbitration — Ingestion Layer

Data ingestion pipeline for healthcare billing arbitration. Handles 10 source types across 7 formats, with validation, deduplication, SCD Type 2 history, audit logging, and dead-letter queue for failed records.

## Entity Definitions

Core entities in the medical billing arbitration domain, from claim submission through arbitration resolution.

### Billing & Payment Entities

| Entity | Table | What It Is |
|--------|-------|------------|
| **Claim** | `claims` | A request for payment submitted by a healthcare provider to an insurance payer after a patient receives medical services. Contains the date of service, provider, patient, payer, and total billed amount. Each claim has one or more service lines. Status tracks its lifecycle: `filed` → `acknowledged` → `paid`/`denied`/`underpaid` → `in_dispute` → `resolved`. |
| **Claim Line** | `claim_lines` | A single service within a claim. Each line has a CPT code (procedure), modifier, units, billed amount, and diagnosis codes (ICD-10). For example, an ER visit claim might have lines for the physician evaluation (99285) and lab work (80053). |
| **Remittance** | `remittances` | The payer's response to a claim — how much they paid, allowed, and why. Arrives as an EDI 835 (ERA) or Explanation of Benefits (EOB) PDF. Contains paid_amount, allowed_amount, adjustment reasons (CARC/RARC codes), and denial codes. A claim can have multiple remittances (e.g., initial payment + adjustment). |
| **Fee Schedule** | `fee_schedule` | Contracted rates between a payer and provider for specific CPT codes. Used to determine if a payment is correct. Tracks historical rates via SCD Type 2 (valid_from/valid_to). Includes Medicare and FAIR Health benchmark rates used as QPA (Qualifying Payment Amount) references in arbitration. |

### People & Organizations

| Entity | Table | What It Is |
|--------|-------|------------|
| **Patient** | `patients` | The individual who received medical services. Identified by MRN (Medical Record Number). Contains demographics (name, DOB, gender, address) and insurance information. Ingested from EHR systems via FHIR R4 or HL7 v2 ADT messages. |
| **Provider** | `providers` | The healthcare professional or facility that delivered services and submitted the claim. Identified by NPI (National Provider Identifier) and TIN (Tax ID). Includes specialty (e.g., Emergency Medicine, Orthopedic Surgery) and facility affiliation. Tracked via SCD Type 2 for specialty/status changes. |
| **Payer** | `payers` | The insurance company responsible for paying claims. Types: `commercial` (Anthem, Aetna, UHC, Cigna, Humana), `medicare`, `medicaid`, `tricare`. Each payer has different contracted rates, denial patterns, and arbitration behavior. |

### Arbitration & Dispute Entities

| Entity | Table | What It Is |
|--------|-------|------------|
| **Case** | `cases` | An arbitration case opened when underpayment is detected. Assigned to an analyst with a priority level (`low`/`medium`/`high`/`critical`). Follows the NSA (No Surprises Act) lifecycle: `open` → `in_review` → `negotiation` → `idr_initiated` → `idr_submitted` → `decided` → `closed`. May result in an outcome (`won`/`lost`/`settled`/`withdrawn`) and an award_amount. |
| **Dispute** | `disputes` | A specific underpayment challenge linked to a claim and case. Contains the billed_amount, paid_amount, requested_amount, and QPA (Qualifying Payment Amount). The underpayment_amount (billed - paid) is the amount in dispute. Types: `appeal` (internal payer appeal), `idr` (Independent Dispute Resolution under NSA), `state_arb` (state arbitration), `external_review`. |
| **Deadline** | `deadlines` | An NSA regulatory deadline attached to a case. Missing a deadline can forfeit arbitration rights. Types and days from filed_date: `open_negotiation` (30 days), `idr_initiation` (34 days), `entity_selection` (37 days), `evidence_submission` (44 days), `idr_decision` (64 days). Status: `pending` → `alerted` → `met`/`missed`. |

### Evidence & Documents

| Entity | Table | What It Is |
|--------|-------|------------|
| **Evidence Artifact** | `evidence_artifacts` | Any document supporting an arbitration case — EOBs, clinical records, contracts, payer correspondence, IDR decisions, prior authorizations. Stored with classification type, confidence score (from Document Intelligence), OCR status, and content hash for deduplication. Linked to a case_id. |
| **Regulation Chunk** | `regulation_chunks` | A section of a regulatory document (NSA, state surprise billing laws, CMS guidance) chunked for RAG (retrieval-augmented generation). Contains the text, section title, jurisdiction, effective date, and CFR citation. Used by the AI agent to ground arbitration narratives in legal references. |

### Operational Entities

| Entity | Table | What It Is |
|--------|-------|------------|
| **Audit Log** | `audit_log` | Every action in the system — inserts, updates, deletes, AI-generated classifications, human reviews. Records entity_type, entity_id, action, user_id, old/new values, and AI metadata (agent name, confidence, model version). Used for HIPAA compliance and operational auditing. |
| **Dead-Letter Queue** | `dead_letter_queue` | Failed ingestion records that couldn't be processed — parse failures, validation errors, unmatched references. Each entry has an error category, detail, and the original payload. Analysts triage and resolve pending DLQ entries. |
| **Claim ID Alias** | `claim_id_alias` | Mapping table for payer-specific claim identifiers to canonical claim IDs. Payers often use their own claim numbers; this table enables matching remittances to claims when IDs don't match directly. |
| **Code Crosswalk** | `code_crosswalk` | Legacy-to-standard code mappings for historical data migration. Maps old CPT/ICD codes to current versions, per source system. Used during backfill ingestion to normalize historical claims. |

### Key Domain Concepts

| Concept | Definition |
|---------|------------|
| **Underpayment** | When a payer pays less than the billed amount. Calculated as `billed_amount - paid_amount`. The core trigger for arbitration disputes. |
| **QPA (Qualifying Payment Amount)** | The median contracted rate for a service in the same geographic area. Under the NSA, the QPA is the starting point for determining fair payment in IDR. If billed > QPA and underpayment > $25, the claim is arbitration-eligible. |
| **IDR (Independent Dispute Resolution)** | The federal arbitration process under the No Surprises Act. After a 30-day open negotiation period, either party can initiate IDR. An independent arbiter reviews evidence and selects one side's payment offer. |
| **NSA (No Surprises Act)** | Federal law (effective Jan 2022) protecting patients from surprise medical bills for out-of-network emergency services. Establishes the IDR process and strict deadlines for resolving payment disputes. |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — a data warehousing pattern that preserves history by closing old rows (valid_to = change date) and inserting new rows (valid_from = change date). Used for fee schedules and providers where historical rates matter for arbitration. |
| **CARC/RARC** | Claim Adjustment Reason Codes / Remittance Advice Remark Codes — standardized codes explaining why a payer adjusted or denied payment. Examples: CO-45 (charges exceed contracted rate), CO-197 (precertification absent). |
| **EDI 837/835** | Electronic Data Interchange standards. 837 = healthcare claim submission (provider → payer). 835 = electronic remittance advice (payer → provider). Both use X12 segment format. |
| **ERA / EOB** | ERA = Electronic Remittance Advice (EDI 835, machine-readable). EOB = Explanation of Benefits (PDF, requires OCR/Document Intelligence to extract data). Both convey the same information: how a payer responded to a claim. |

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

**Sample input** (`sample_data/claims_837.edi`):
```
ISA*00*          *00*          *ZZ*CLEARINGHOUSE  *ZZ*MEDBILL        *250101*1200*^*00501*...~
GS*HC*CLEARINGHOUSE*MEDBILL*20250101*1200*1*X*005010X222A1~
ST*837*0001*005010X222A1~
NM1*85*1*SMITH*JOHN*A***XX*1234567890~      ← Provider NPI
NM1*IL*1*DOE*JANE****MI*INS100001~          ← Patient ID (MRN)
NM1*PR*2*AETNA*****PI*AETNA~               ← Payer
CLM*CLM-1001*500***11:B:1*Y*A*Y*Y~         ← Claim ID, $500 billed, frequency=1 (original)
DTP*472*D8*20250615~                        ← Date of service
HI*ABK:J06.9*ABF:R05.9~                    ← Diagnosis codes (ICD-10)
SV1*HC:99213*150*UN*1***1~                  ← Service line: CPT 99213, $150, 1 unit
SV1*HC:99214:25*350*UN*1***1~              ← Service line: CPT 99214 modifier 25, $350
```

**Data flow:**
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

**Sample input** (`sample_data/era_835.edi`):
```
ISA*00*          *00*          *ZZ*AETNA          *ZZ*MEDBILL        *250801*1000*...~
TRN*1*TRC-50001*1999999999~                     ← Trace number
N1*PR*AETNA*XV*AETNA~                           ← Payer
CLP*CLM-1001*1*500.00*350.00*75.00*12~          ← Claim: billed $500, paid $350, patient $75
CAS*CO*45*75.00~                                ← Contractual adjustment: $75 (CO-45)
CAS*PR*1*50.00*2*25.00~                         ← Patient responsibility: $50 deductible + $25 copay
SVC*HC:99213*150.00*100.00**1~                  ← Line: billed $150, paid $100
SVC*HC:99214:25*350.00*250.00**1~              ← Line: billed $350, paid $250
CLP*CLM-1003*2*1200.00*0.00*0.00*12~            ← DENIED claim: $0 paid
CAS*CO*50*1200.00~                              ← Denial: CO-50 (not medically necessary)
```

**Data flow:**
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

**Sample input** (`sample_data/fhir_patients.json`):
```json
{
  "resourceType": "Patient",
  "id": "PAT-001",
  "identifier": [{"type": {"coding": [{"code": "MR"}]}, "value": "INS100001"}],
  "name": [{"use": "official", "family": "Doe", "given": ["Jane", "Marie"]}],
  "birthDate": "1985-03-15",
  "gender": "female",
  "address": [{"use": "home", "line": ["123 Main St", "Apt 4B"],
               "city": "Los Angeles", "state": "CA", "postalCode": "90001"}],
  "extension": [{"url": "http://example.org/insurance", "valueString": "AETNA-HMO-12345"}]
}
```

**Data flow:**
```
FHIR JSON → normalize_fhir_patient()
  → Extracts: MRN (identifier where type="MR"), name (use="official"),
    DOB, gender, address (use="home"), insurance_id (extension)
  → validate_patient(): patient_id, DOB required
  → SCD Type 1: INSERT or UPDATE on patient_id
  → emit_event("patient.insert" | "patient.update")
```

### 4. Patients (HL7 v2)

**Sample input** (`sample_data/adt_patients.hl7`):
```
MSH|^~\&|EHR_EPIC|FACILITY_A|MEDBILL|ARBSYS|20250615120000||ADT^A01|MSG0001|P|2.5
PID|1|INS200001|INS200001^^^MRN||Roberts^Michael^James||19780520|M|||456 Oak Ave^^San Francisco^CA^94102
IN1|1|BCBS-HMO-99876|||BCBS_CA
```

- `MSH-9`: Message type (`ADT^A01` = admit, `A04` = register, `A08` = update)
- `PID-3`: Patient ID (MRN)
- `PID-5`: Name (`Last^First^Middle`)
- `PID-7`: DOB (`YYYYMMDD`)
- `PID-8`: Gender (`M`/`F`)
- `PID-11`: Address (`Street^^City^State^Zip`)
- `IN1-2`: Insurance ID

**Data flow:**
```
HL7 v2 ADT message → parse_hl7v2_message() → normalize_hl7v2_patient()
  → Validates: MSH + PID segments present, MSH-9 contains "ADT"
  → Extracts from PID: patient_id (PID-3), name (PID-5), DOB (PID-7),
    gender (PID-8), address (PID-11), insurance_id (IN1-2)
  → Same validation + insert flow as FHIR patients
```

### 5. Fee Schedules (CSV — SCD Type 2)

**Sample input** (`sample_data/medicare_fee_schedule_2025.csv`):
```csv
cpt_code,modifier,geo_region,rate,rate_type,effective_date
99213,,CA-01,120.00,medicare_pfs,2025-01-01
99214,,CA-01,175.00,medicare_pfs,2025-01-01
99214,25,CA-01,192.50,medicare_pfs,2025-01-01
99283,,CA-01,225.00,medicare_pfs,2025-01-01
27236,,CA-01,950.00,medicare_pfs,2025-01-01
```

**Data flow:**
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

**Sample input** (`sample_data/nppes_providers.csv`):
```csv
NPI,Provider Last Name (Legal Name),Provider First Name,Entity Type Code,Healthcare Provider Taxonomy Code_1,Provider Business Practice Location Address City Name,Provider Business Practice Location Address State Name,Provider Business Practice Location Address Postal Code,NPI Deactivation Reason Code,NPI Deactivation Date
1234567890,SMITH,JOHN,1,207Q00000X,Los Angeles,CA,90001,,
2345678901,GARCIA,MARIA,1,208000000X,San Francisco,CA,94102,,
```

**Data flow:**
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

**Sample input** (`sample_data/eob_extracted.json`):
```json
{
  "source_file": "eob_cigna_clm1005.pdf",
  "payer_id": "CIGNA",
  "fields": {
    "ClaimNumber": {"value": "CLM-1005", "confidence": 0.95},
    "PaidAmount": {"value": "180.00", "confidence": 0.92},
    "AllowedAmount": {"value": "200.00", "confidence": 0.91},
    "PatientResp": {"value": "40.00", "confidence": 0.88},
    "AdjustmentReason": {"value": "CO-45", "confidence": 0.85}
  },
  "avg_confidence": 0.917
}
```

Field names vary by payer — the parser normalizes via FIELD_MAPPINGS (e.g., `ClaimNumber` / `Claim_No` / `ClaimID` all map to `claim_number`).

**Data flow:**
```
Doc Intelligence JSON → parse_eob_extractions()
  → Normalizes payer-specific field names via FIELD_MAPPINGS
  → check_evidence_duplicate(): SHA-256 hash
  → Store as evidence_artifact
  → If avg_confidence >= 0.9: match claim_id, create remittance
  → If avg_confidence < 0.9: flag for manual review
```

### 8. Contracts (Document Intelligence)

**Sample input** (`sample_data/contract_extracted.json`):
```json
{
  "source_file": "contract_aetna_2025.pdf",
  "payer_id": "AETNA",
  "provider_npi": "1234567890",
  "effective_date": "2025-01-01",
  "tables": [
    [
      {"col_0": "CPT Code", "col_1": "Rate"},
      {"col_0": "99213", "col_1": "$130.00"},
      {"col_0": "99214", "col_1": "$190.00"}
    ]
  ]
}
```

**Data flow:**
```
Doc Intelligence JSON → parse_contract_extraction()
  → Extracts: payer_id, provider_npi, effective_date, rate tables
  → check_evidence_duplicate(): SHA-256 hash
  → Store as evidence_artifact (always ocr_status='needs_review')
```

### 9. Backfill Claims (CSV)

**Sample input** (`sample_data/backfill_claims.csv`):
```csv
claim_id,patient_id,provider_npi,payer_id,date_of_service,total_billed,cpt_codes,diagnosis_codes
CLM-H001,INS100001,1234567890,AETNA,2024-03-15,320.00,99213;99214,J06.9
CLM-H002,INS100002,2345678901,UHC,2024-04-20,550.00,99283,S72.001A
CLM-H003,INS100001,1234567890,AETNA,2024-06-10,180.00,99213,M54.5
```

Note: `cpt_codes` are semicolon-separated. The parser splits them into individual service lines and distributes `total_billed` evenly (e.g., $320 / 2 codes = $160 per line).

**Data flow:**
```
CSV → parse_backfill_csv()
  → Splits semicolon-separated CPT codes into individual lines
  → Distributes total_billed evenly across lines
  → apply_crosswalk(): maps legacy codes if source_system provided
  → validate_claim() → check_backfill_duplicate()
  → INSERT with status='historical', source='backfill'
```

### 10. Regulations (Plain Text)

**Sample input** (`sample_data/nsa_regulation.txt`):
```
No Surprises Act — Independent Dispute Resolution Process
Federal Register / Vol. 87, No. 229 / Rules and Regulations

Section 149.510. Determination of payment amount through open negotiation.

(a) In general. If an item or service furnished by a nonparticipating provider
or nonparticipating emergency facility is covered under a group health plan...
and the total payment by the plan or coverage and the individual is less than
the qualifying payment amount, the provider or facility may initiate open
negotiation.

(b) Open negotiation period. The open negotiation period begins on the day
the provider or facility receives an initial payment or notice of denial,
and ends 30 business days after such date.
```

The parser splits on section headers (`Section`, `§`, `PART`, `SUBPART`, `Article`) and creates overlapping chunks (200-token overlap) for RAG retrieval.

**Data flow:**
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
