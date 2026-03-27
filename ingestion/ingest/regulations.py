"""Regulations & Rules ingestion — Section 5.10 of the design doc.

Regulatory documents are chunked, stored in evidence_artifacts, and indexed
in a mock vector search table for RAG retrieval.

In production:
  - Documents stored in Azure Blob (regulations container)
  - Chunks indexed in Azure AI Search with text-embedding-ada-002 vectors
  - Filterable by jurisdiction, effective_date, cfr_citation

For testing, chunks are stored in a SQLite table (regulation_chunks).
"""

import json
import logging
import sqlite3
from datetime import datetime

from ingestion.parsers.regulation_parser import parse_regulation_text, compute_document_hash
from ingestion.dedup import check_evidence_duplicate
from ingestion.audit import log_action
from ingestion.events import emit_event

logger = logging.getLogger(__name__)

REGULATION_CHUNKS_SCHEMA = """
CREATE TABLE IF NOT EXISTS regulation_chunks (
    chunk_id        TEXT PRIMARY KEY,
    document_hash   TEXT NOT NULL,
    section_title   TEXT,
    section_index   INTEGER,
    chunk_text      TEXT NOT NULL,
    jurisdiction    TEXT NOT NULL DEFAULT 'federal',
    effective_date  TEXT,
    superseded_date TEXT,
    document_title  TEXT,
    cfr_citation    TEXT,
    indexed_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_reg_chunks_jurisdiction
    ON regulation_chunks (jurisdiction, effective_date);
"""


def init_regulation_tables(conn: sqlite3.Connection):
    conn.executescript(REGULATION_CHUNKS_SCHEMA)
    conn.commit()


def ingest_regulation(conn: sqlite3.Connection, content: str,
                      metadata: dict, uploaded_by: str = "analyst") -> dict:
    """Ingest a regulatory document: chunk, store artifact, index chunks.

    Args:
        content: Full text of the regulation.
        metadata: {document_title, jurisdiction, effective_date,
                   superseded_date, cfr_citation, source_file}

    Returns: {stored: bool, duplicate: bool, chunks_indexed: int}
    """
    init_regulation_tables(conn)
    content_hash = compute_document_hash(content)
    summary = {"stored": False, "duplicate": False, "chunks_indexed": 0}

    # Dedup by content hash
    if check_evidence_duplicate(conn, content_hash):
        summary["duplicate"] = True
        return summary

    # Store as evidence artifact
    conn.execute("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_conf,
             ocr_status, content_hash, uploaded_date, uploaded_by)
        VALUES (?, 'regulation', ?, ?, 1.0, 'completed', ?, ?, ?)
    """, (None,
          f"regulations/{metadata.get('source_file', 'unknown')}",
          json.dumps({"title": metadata.get("document_title"),
                      "jurisdiction": metadata.get("jurisdiction"),
                      "cfr_citation": metadata.get("cfr_citation"),
                      "effective_date": metadata.get("effective_date")}),
          content_hash,
          datetime.utcnow().isoformat(),
          uploaded_by))

    # Parse into chunks and index
    chunks = parse_regulation_text(content, metadata)
    now = datetime.utcnow().isoformat()

    for chunk in chunks:
        conn.execute("""
            INSERT OR REPLACE INTO regulation_chunks
                (chunk_id, document_hash, section_title, section_index,
                 chunk_text, jurisdiction, effective_date, superseded_date,
                 document_title, cfr_citation, indexed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chunk["chunk_id"], content_hash, chunk["section_title"],
              chunk["section_index"], chunk["text"],
              chunk["jurisdiction"], chunk.get("effective_date"),
              chunk.get("superseded_date"), chunk.get("document_title"),
              chunk.get("cfr_citation"), now))
        summary["chunks_indexed"] += 1

    log_action(conn, "regulation", content_hash[:12], "upload", user_id=uploaded_by)
    emit_event(conn, "regulation.indexed", "regulation", content_hash[:12],
               {"document_title": metadata.get("document_title"),
                "chunks": summary["chunks_indexed"],
                "jurisdiction": metadata.get("jurisdiction")})

    summary["stored"] = True
    conn.commit()
    return summary


def search_regulations(conn: sqlite3.Connection, query: str,
                       jurisdiction: str = None,
                       as_of_date: str = None, limit: int = 5) -> list:
    """Mock vector search — keyword-based fallback for testing.

    In production, this would call Azure AI Search with:
      - Vector similarity (embedding of query)
      - BM25 keyword match (hybrid search)
      - Filters: jurisdiction, effective_date range
    """
    init_regulation_tables(conn)
    sql = "SELECT * FROM regulation_chunks WHERE 1=1"
    params = []

    if jurisdiction:
        sql += " AND jurisdiction = ?"
        params.append(jurisdiction)

    if as_of_date:
        sql += " AND (effective_date IS NULL OR effective_date <= ?)"
        sql += " AND (superseded_date IS NULL OR superseded_date > ?)"
        params.extend([as_of_date, as_of_date])

    # Mock keyword search: match any word from query in chunk_text
    keywords = query.lower().split()
    if keywords:
        conditions = " OR ".join(["LOWER(chunk_text) LIKE ?" for _ in keywords])
        sql += f" AND ({conditions})"
        params.extend([f"%{kw}%" for kw in keywords])

    sql += f" LIMIT {limit}"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
