"""Regulation document parser — chunks regulatory text for vector indexing.

In production, Azure AI Search with text-embedding-ada-002 handles the
vector index. This mock implementation chunks text by section headers
and returns structured chunks with metadata.

Sample input format (regulation plain text):

    No Surprises Act — Independent Dispute Resolution Process
    Federal Register / Vol. 87, No. 229 / Rules and Regulations

    Section 149.510. Determination of payment amount through open negotiation.

    (a) In general. If an item or service furnished by a nonparticipating
    provider or nonparticipating emergency facility is covered under a group
    health plan... the provider or facility may initiate open negotiation.

    Section 149.520. Determination of the qualifying payment amount.

    (a) The qualifying payment amount means the median of the contracted
    rates recognized by the plan or issuer...

Splits on section headers (Section, §, PART, SUBPART, Article) with
200-token overlap between adjacent chunks.

Sample parsed output per chunk::

    {
        "chunk_id": "a1b2c3d4e5f6g7h8",
        "text": "(a) In general. If an item or service...",
        "section_title": "Section 149.510. Determination of payment amount...",
        "section_index": 1,
        "jurisdiction": "federal",
        "effective_date": "2022-01-01",
        "document_title": "No Surprises Act — IDR Process",
        "cfr_citation": "45 CFR 149.510",
        "overlap_prefix": "...last 200 tokens of previous section..."
    }

Required metadata dict::

    {
        "document_title": "No Surprises Act — IDR Process",
        "jurisdiction": "federal",
        "effective_date": "2022-01-01",
        "cfr_citation": "45 CFR 149.510",
        "source_file": "nsa_regulation.txt"
    }
"""

import hashlib
import re
from typing import List


def parse_regulation_text(content: str, metadata: dict) -> List[dict]:
    """Parse a regulatory document into section-based chunks.

    Args:
        content: Raw text of the regulation document.
        metadata: Must include jurisdiction, effective_date, document_title, cfr_citation.

    Returns: List of chunk dicts with text, metadata, and overlap.
    """
    sections = _split_by_sections(content)
    chunks = []
    overlap_tokens = 200  # Token overlap between adjacent chunks

    for i, section in enumerate(sections):
        if not section["text"].strip():
            continue
        chunk_text = section["text"]
        chunk_id = hashlib.sha256(
            f"{metadata.get('cfr_citation', '')}:{section['title']}:{i}".encode()
        ).hexdigest()[:16]

        chunk = {
            "chunk_id": chunk_id,
            "text": chunk_text,
            "section_title": section["title"],
            "section_index": i,
            "jurisdiction": metadata.get("jurisdiction", "federal"),
            "effective_date": metadata.get("effective_date"),
            "superseded_date": metadata.get("superseded_date"),
            "document_title": metadata.get("document_title"),
            "cfr_citation": metadata.get("cfr_citation"),
            "source_file": metadata.get("source_file"),
        }

        # Add overlap from previous section for context continuity
        if i > 0 and sections[i - 1]["text"]:
            prev_words = sections[i - 1]["text"].split()
            overlap_text = " ".join(prev_words[-min(overlap_tokens, len(prev_words)):])
            chunk["overlap_prefix"] = overlap_text

        chunks.append(chunk)

    return chunks


def _split_by_sections(text: str) -> List[dict]:
    """Split regulatory text by section headers."""
    # Match common regulatory section patterns:
    # "Section 1.", "§ 149.510", "(a)", "PART 149", etc.
    header_pattern = re.compile(
        r'^((?:Section|§|PART|SUBPART|Article)\s+[\d.]+[A-Za-z]*\.?\s*.*?)$',
        re.MULTILINE
    )

    splits = header_pattern.split(text)
    sections = []

    if splits and not header_pattern.match(splits[0]):
        # Text before first header
        sections.append({"title": "Preamble", "text": splits[0].strip()})
        splits = splits[1:]

    # Pair headers with their body text
    for j in range(0, len(splits) - 1, 2):
        title = splits[j].strip()
        body = splits[j + 1].strip() if j + 1 < len(splits) else ""
        sections.append({"title": title, "text": body})

    # If no sections found, treat entire text as one chunk
    if not sections:
        sections.append({"title": "Full Document", "text": text.strip()})

    return sections


def compute_document_hash(content: str) -> str:
    """SHA-256 hash of regulation content for versioning."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
