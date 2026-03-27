"""Silver Layer — Cleaned, conformed, joined data.

Simulates Fabric Notebook transforms: Bronze → Silver.
In production: PySpark MERGE INTO Delta tables with schema enforcement.

Silver tables:
  - silver_claims: claims + claim_lines denormalized
  - silver_remittances: remittances with payer name, adjustment detail
  - silver_claim_remittance: joined claim↔remittance for underpayment analysis
  - silver_fee_schedule: current rates with point-in-time lookup support
  - silver_providers: current provider records (SCD2 resolved)
  - silver_patients: cleaned patient demographics
  - silver_disputes: disputes enriched with claim/payer context and QPA
  - silver_cases: cases with dispute counts and financial summary
  - silver_deadlines: deadlines with SLA compliance tracking
"""

import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

SILVER_SCHEMA = """
-- Claims with lines denormalized
CREATE TABLE IF NOT EXISTS silver_claims (
    claim_id        TEXT PRIMARY KEY,
    patient_id      TEXT,
    provider_npi    TEXT,
    payer_id        TEXT,
    date_of_service TEXT,
    date_filed      TEXT,
    facility_type   TEXT,
    total_billed    REAL,
    status          TEXT,
    source          TEXT,
    line_count      INTEGER,
    cpt_codes       TEXT,   -- JSON array of CPT codes
    total_units     INTEGER,
    _silver_ts      TEXT NOT NULL
);

-- Remittances enriched
CREATE TABLE IF NOT EXISTS silver_remittances (
    remit_id        INTEGER PRIMARY KEY,
    claim_id        TEXT,
    payer_id        TEXT,
    payer_name      TEXT,
    era_date        TEXT,
    paid_amount     REAL,
    allowed_amount  REAL,
    adjustment_reason TEXT,
    denial_code     TEXT,
    check_number    TEXT,
    source          TEXT,
    _silver_ts      TEXT NOT NULL
);

-- Joined claim + remittance for underpayment analysis
CREATE TABLE IF NOT EXISTS silver_claim_remittance (
    claim_id        TEXT,
    patient_id      TEXT,
    provider_npi    TEXT,
    payer_id        TEXT,
    payer_name      TEXT,
    date_of_service TEXT,
    total_billed    REAL,
    total_paid      REAL,
    total_allowed   REAL,
    underpayment    REAL,
    cpt_codes       TEXT,
    line_count      INTEGER,
    remittance_count INTEGER,
    has_denial      INTEGER DEFAULT 0,
    claim_status    TEXT,
    _silver_ts      TEXT NOT NULL,
    PRIMARY KEY (claim_id)
);

-- Fee schedule (current rates only, flattened)
CREATE TABLE IF NOT EXISTS silver_fee_schedule (
    id              INTEGER PRIMARY KEY,
    payer_id        TEXT,
    cpt_code        TEXT,
    modifier        TEXT,
    geo_region      TEXT,
    rate            REAL,
    rate_type       TEXT,
    valid_from      TEXT,
    valid_to        TEXT,
    is_current      INTEGER,
    source          TEXT,
    _silver_ts      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_silver_fs_lookup
    ON silver_fee_schedule (payer_id, cpt_code, geo_region, is_current);

-- Providers (SCD2 resolved — current view)
CREATE TABLE IF NOT EXISTS silver_providers (
    npi                 TEXT PRIMARY KEY,
    name_first          TEXT,
    name_last           TEXT,
    specialty_taxonomy  TEXT,
    state               TEXT,
    city                TEXT,
    zip                 TEXT,
    status              TEXT,
    _silver_ts          TEXT NOT NULL
);

-- Patients (cleaned)
CREATE TABLE IF NOT EXISTS silver_patients (
    patient_id      TEXT PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    dob             TEXT,
    gender          TEXT,
    state           TEXT,
    zip             TEXT,
    insurance_id    TEXT,
    _silver_ts      TEXT NOT NULL
);

-- Disputes enriched with claim and payer context
CREATE TABLE IF NOT EXISTS silver_disputes (
    dispute_id          TEXT PRIMARY KEY,
    claim_id            TEXT,
    case_id             TEXT,
    payer_id            TEXT,
    payer_name          TEXT,
    dispute_type        TEXT,
    status              TEXT,
    billed_amount       REAL,
    paid_amount         REAL,
    qpa_amount          REAL,
    underpayment_amount REAL,
    filed_date          TEXT,
    provider_npi        TEXT,
    date_of_service     TEXT,
    cpt_codes           TEXT,
    _silver_ts          TEXT NOT NULL
);

-- Cases with dispute summary
CREATE TABLE IF NOT EXISTS silver_cases (
    case_id             TEXT PRIMARY KEY,
    assigned_analyst    TEXT,
    status              TEXT,
    priority            TEXT,
    created_date        TEXT,
    last_activity_date  TEXT,
    outcome             TEXT,
    award_amount        REAL,
    dispute_count       INTEGER DEFAULT 0,
    total_billed        REAL DEFAULT 0,
    total_underpayment  REAL DEFAULT 0,
    age_days            INTEGER DEFAULT 0,
    _silver_ts          TEXT NOT NULL
);

-- Deadlines with SLA tracking
CREATE TABLE IF NOT EXISTS silver_deadlines (
    deadline_id         INTEGER PRIMARY KEY,
    case_id             TEXT,
    dispute_id          TEXT,
    type                TEXT,
    due_date            TEXT,
    status              TEXT,
    completed_date      TEXT,
    days_remaining      INTEGER,
    is_at_risk          INTEGER DEFAULT 0,
    _silver_ts          TEXT NOT NULL
);
"""


def init_silver(olap_conn: sqlite3.Connection):
    olap_conn.executescript(SILVER_SCHEMA)
    olap_conn.commit()


def transform_silver(olap_conn: sqlite3.Connection) -> dict:
    """Transform Bronze → Silver.

    Reads bronze_* tables, cleans/joins, writes to silver_* tables.
    Idempotent — uses INSERT OR REPLACE.

    Returns: {table: row_count}
    """
    init_silver(olap_conn)
    now = datetime.utcnow().isoformat()
    summary = {}

    # --- silver_patients ---
    summary["silver_patients"] = _transform_patients(olap_conn, now)

    # --- silver_providers ---
    summary["silver_providers"] = _transform_providers(olap_conn, now)

    # --- silver_claims (claims + lines denormalized) ---
    summary["silver_claims"] = _transform_claims(olap_conn, now)

    # --- silver_remittances ---
    summary["silver_remittances"] = _transform_remittances(olap_conn, now)

    # --- silver_claim_remittance (joined) ---
    summary["silver_claim_remittance"] = _transform_claim_remittance(olap_conn, now)

    # --- silver_fee_schedule ---
    summary["silver_fee_schedule"] = _transform_fee_schedule(olap_conn, now)

    # --- silver_disputes ---
    summary["silver_disputes"] = _transform_disputes(olap_conn, now)

    # --- silver_cases ---
    summary["silver_cases"] = _transform_cases(olap_conn, now)

    # --- silver_deadlines ---
    summary["silver_deadlines"] = _transform_deadlines(olap_conn, now)

    olap_conn.commit()
    logger.info("Silver transform complete: %s", summary)
    return summary


def _parse_bronze(olap_conn, table):
    """Read bronze rows and parse JSON payloads, skipping deletes."""
    rows = olap_conn.execute(
        f"SELECT _cdc_operation, payload FROM bronze_{table}"
    ).fetchall()
    return [json.loads(r[1]) for r in rows if r[0] != "D"]


def _transform_patients(conn, now):
    patients = _parse_bronze(conn, "patients")
    count = 0
    for p in patients:
        conn.execute("""
            INSERT OR REPLACE INTO silver_patients
                (patient_id, first_name, last_name, dob, gender, state, zip,
                 insurance_id, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (p["patient_id"], p.get("first_name"), p.get("last_name"),
              p.get("dob"), p.get("gender"), p.get("state"), p.get("zip"),
              p.get("insurance_id"), now))
        count += 1
    return count


def _transform_providers(conn, now):
    providers = _parse_bronze(conn, "providers")
    count = 0
    for p in providers:
        if not p.get("is_current"):
            continue
        conn.execute("""
            INSERT OR REPLACE INTO silver_providers
                (npi, name_first, name_last, specialty_taxonomy, state, city, zip,
                 status, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (p["npi"], p.get("name_first"), p.get("name_last"),
              p.get("specialty_taxonomy"), p.get("state"), p.get("city"),
              p.get("zip"), p.get("status"), now))
        count += 1
    return count


def _transform_claims(conn, now):
    claims = _parse_bronze(conn, "claims")
    lines = _parse_bronze(conn, "claim_lines")

    # Group lines by claim_id
    lines_by_claim = {}
    for line in lines:
        cid = line["claim_id"]
        if cid not in lines_by_claim:
            lines_by_claim[cid] = []
        lines_by_claim[cid].append(line)

    count = 0
    for c in claims:
        cid = c["claim_id"]
        claim_lines = lines_by_claim.get(cid, [])
        cpt_codes = list(set(l["cpt_code"] for l in claim_lines))
        total_units = sum(l.get("units", 1) for l in claim_lines)

        conn.execute("""
            INSERT OR REPLACE INTO silver_claims
                (claim_id, patient_id, provider_npi, payer_id, date_of_service,
                 date_filed, facility_type, total_billed, status, source,
                 line_count, cpt_codes, total_units, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (cid, c.get("patient_id"), c.get("provider_npi"),
              c.get("payer_id"), c.get("date_of_service"), c.get("date_filed"),
              c.get("facility_type"), c.get("total_billed"), c.get("status"),
              c.get("source"), len(claim_lines), json.dumps(cpt_codes),
              total_units, now))
        count += 1
    return count


def _transform_remittances(conn, now):
    remittances = _parse_bronze(conn, "remittances")
    payers = {p["payer_id"]: p for p in _parse_bronze(conn, "payers")}

    count = 0
    for r in remittances:
        payer = payers.get(r.get("payer_id"), {})
        conn.execute("""
            INSERT OR REPLACE INTO silver_remittances
                (remit_id, claim_id, payer_id, payer_name, era_date,
                 paid_amount, allowed_amount, adjustment_reason, denial_code,
                 check_number, source, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (r["remit_id"], r.get("claim_id"), r.get("payer_id"),
              payer.get("name"), r.get("era_date"), r.get("paid_amount"),
              r.get("allowed_amount"), r.get("adjustment_reason"),
              r.get("denial_code"), r.get("check_number"),
              r.get("source"), now))
        count += 1
    return count


def _transform_claim_remittance(conn, now):
    """Join silver_claims with silver_remittances for underpayment view."""
    conn.execute("DELETE FROM silver_claim_remittance")

    # Build payer name lookup from bronze
    payers = {p["payer_id"]: p.get("name", p["payer_id"])
              for p in _parse_bronze(conn, "payers")}

    # Use claim's own payer_id for the payer_name (not remittance's)
    conn.execute("""
        INSERT INTO silver_claim_remittance
            (claim_id, patient_id, provider_npi, payer_id, payer_name,
             date_of_service, total_billed, total_paid, total_allowed,
             underpayment, cpt_codes, line_count, remittance_count,
             has_denial, claim_status, _silver_ts)
        SELECT
            c.claim_id,
            c.patient_id,
            c.provider_npi,
            c.payer_id,
            NULL,
            c.date_of_service,
            c.total_billed,
            COALESCE(r.total_paid, 0),
            COALESCE(r.total_allowed, 0),
            c.total_billed - COALESCE(r.total_paid, 0) AS underpayment,
            c.cpt_codes,
            c.line_count,
            COALESCE(r.remit_count, 0),
            COALESCE(r.has_denial, 0),
            c.status,
            ?
        FROM silver_claims c
        LEFT JOIN (
            SELECT
                claim_id,
                SUM(paid_amount) AS total_paid,
                SUM(allowed_amount) AS total_allowed,
                COUNT(*) AS remit_count,
                MAX(CASE WHEN denial_code IS NOT NULL AND denial_code != '' THEN 1 ELSE 0 END) AS has_denial
            FROM silver_remittances
            GROUP BY claim_id
        ) r ON c.claim_id = r.claim_id
    """, (now,))

    # Set payer_name from lookup
    for payer_id, name in payers.items():
        conn.execute(
            "UPDATE silver_claim_remittance SET payer_name = ? WHERE payer_id = ?",
            (name, payer_id)
        )

    count = conn.execute("SELECT COUNT(*) FROM silver_claim_remittance").fetchone()[0]
    return count


def _transform_fee_schedule(conn, now):
    fs_rows = _parse_bronze(conn, "fee_schedule")
    count = 0
    for f in fs_rows:
        conn.execute("""
            INSERT OR REPLACE INTO silver_fee_schedule
                (id, payer_id, cpt_code, modifier, geo_region, rate, rate_type,
                 valid_from, valid_to, is_current, source, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (f["id"], f["payer_id"], f["cpt_code"], f.get("modifier", ""),
              f["geo_region"], f["rate"], f["rate_type"],
              f["valid_from"], f["valid_to"], f.get("is_current", 1),
              f.get("source"), now))
        count += 1
    return count


def _transform_disputes(conn, now):
    """Enrich disputes with claim context and payer name."""
    disputes = _parse_bronze(conn, "disputes")
    if not disputes:
        return 0

    # Build lookups
    payers = {p["payer_id"]: p.get("name", p["payer_id"])
              for p in _parse_bronze(conn, "payers")}
    claims_lookup = {c["claim_id"]: c for c in _parse_bronze(conn, "claims")}

    # Get CPT codes from silver_claims (already denormalized)
    cpt_lookup = {}
    for row in conn.execute("SELECT claim_id, cpt_codes FROM silver_claims").fetchall():
        cpt_lookup[row[0]] = row[1]

    count = 0
    for d in disputes:
        claim = claims_lookup.get(d.get("claim_id"), {})
        payer_id = claim.get("payer_id", d.get("payer_id"))

        conn.execute("""
            INSERT OR REPLACE INTO silver_disputes
                (dispute_id, claim_id, case_id, payer_id, payer_name,
                 dispute_type, status, billed_amount, paid_amount,
                 qpa_amount, underpayment_amount, filed_date,
                 provider_npi, date_of_service, cpt_codes, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["dispute_id"], d.get("claim_id"), d.get("case_id"),
              payer_id, payers.get(payer_id),
              d.get("dispute_type"), d.get("status"),
              d.get("billed_amount"), d.get("paid_amount"),
              d.get("qpa_amount"), d.get("underpayment_amount"),
              d.get("filed_date"),
              claim.get("provider_npi"), claim.get("date_of_service"),
              cpt_lookup.get(d.get("claim_id")), now))
        count += 1
    return count


def _transform_cases(conn, now):
    """Enrich cases with dispute counts and financial summary."""
    cases = _parse_bronze(conn, "cases")
    if not cases:
        return 0

    count = 0
    for c in cases:
        case_id = c["case_id"]

        # Aggregate from silver_disputes
        agg = conn.execute("""
            SELECT COUNT(*) AS cnt,
                   COALESCE(SUM(billed_amount), 0) AS total_billed,
                   COALESCE(SUM(underpayment_amount), 0) AS total_underpay
            FROM silver_disputes WHERE case_id = ?
        """, (case_id,)).fetchone()

        created = c.get("created_date")
        age_days = 0
        if created:
            try:
                age_row = conn.execute(
                    "SELECT CAST(julianday('now') - julianday(?) AS INTEGER)",
                    (created,)
                ).fetchone()
                age_days = age_row[0] if age_row and age_row[0] else 0
            except Exception:
                pass

        conn.execute("""
            INSERT OR REPLACE INTO silver_cases
                (case_id, assigned_analyst, status, priority, created_date,
                 last_activity_date, outcome, award_amount, dispute_count,
                 total_billed, total_underpayment, age_days, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (case_id, c.get("assigned_analyst"), c.get("status"),
              c.get("priority"), created, c.get("last_activity_date"),
              c.get("outcome"), c.get("award_amount"),
              agg[0], agg[1], agg[2], age_days, now))
        count += 1
    return count


def _transform_deadlines(conn, now):
    """Transform deadlines with SLA tracking (days remaining, at-risk flag)."""
    deadlines = _parse_bronze(conn, "deadlines")
    if not deadlines:
        return 0

    count = 0
    for dl in deadlines:
        due_date = dl.get("due_date")
        days_remaining = None
        is_at_risk = 0

        if due_date and dl.get("status") not in ("met", "missed"):
            try:
                rem = conn.execute(
                    "SELECT CAST(julianday(?) - julianday('now') AS INTEGER)",
                    (due_date,)
                ).fetchone()
                days_remaining = rem[0] if rem else None
                if days_remaining is not None and days_remaining <= 5:
                    is_at_risk = 1
            except Exception:
                pass

        conn.execute("""
            INSERT OR REPLACE INTO silver_deadlines
                (deadline_id, case_id, dispute_id, type, due_date, status,
                 completed_date, days_remaining, is_at_risk, _silver_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (dl["deadline_id"], dl.get("case_id"), dl.get("dispute_id"),
              dl.get("type"), due_date, dl.get("status"),
              dl.get("completed_date"), days_remaining, is_at_risk, now))
        count += 1
    return count
