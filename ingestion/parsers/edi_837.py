"""EDI X12 837 Parser — Claims (Professional & Institutional).

Sample input format (EDI X12 837P):

    ISA*00*          *00*          *ZZ*CLEARINGHOUSE  *ZZ*MEDBILL        *250101*1200*^*00501*...~
    GS*HC*CLEARINGHOUSE*MEDBILL*20250101*1200*1*X*005010X222A1~
    ST*837*0001*005010X222A1~
    NM1*85*1*SMITH*JOHN*A***XX*1234567890~      # Provider NPI
    NM1*IL*1*DOE*JANE****MI*INS100001~          # Patient ID (MRN)
    NM1*PR*2*AETNA*****PI*AETNA~               # Payer
    CLM*CLM-1001*500***11:B:1*Y*A*Y*Y~         # Claim ID, $500 billed, freq=1 (original)
    DTP*472*D8*20250615~                        # Date of service
    HI*ABK:J06.9*ABF:R05.9~                    # Diagnosis codes (ICD-10)
    SV1*HC:99213*150*UN*1***1~                  # Line: CPT 99213, $150, 1 unit
    SV1*HC:99214:25*350*UN*1***1~              # Line: CPT 99214 mod 25, $350

Parsed output per claim::

    {
        "claim_id": "CLM-1001",
        "total_billed": 500.0,
        "date_of_service": "2025-06-15",
        "provider_npi": "1234567890",
        "patient_id": "INS100001",
        "payer_id": "AETNA",
        "frequency_code": "1",        # 1=original, 7=replacement, 8=void
        "diagnosis_codes": ["J06.9", "R05.9"],
        "lines": [
            {"cpt_code": "99213", "modifier": None, "units": 1, "billed_amount": 150.0},
            {"cpt_code": "99214", "modifier": "25", "units": 1, "billed_amount": 350.0}
        ]
    }
"""

from typing import List, Dict, Optional


class EDI837Parser:
    def __init__(self, raw_edi: str):
        self.raw = raw_edi.strip()
        if not self.raw.startswith("ISA"):
            raise ValueError("EDI file does not start with ISA segment")
        self.element_sep = self.raw[3]
        self.seg_terminator = self.raw[105]
        self.segments = self._split_segments()

    def _split_segments(self) -> List[List[str]]:
        lines = self.raw.split(self.seg_terminator)
        return [line.strip().split(self.element_sep) for line in lines if line.strip()]

    def _find_segments(self, seg_id: str) -> List[List[str]]:
        return [s for s in self.segments if s and s[0] == seg_id]

    def detect_transaction_type(self) -> str:
        for seg in self._find_segments("ST"):
            st03 = seg[3] if len(seg) > 3 else ""
            if "005010X223" in st03:
                return "837I"
        return "837P"

    def parse(self) -> dict:
        tx_type = self.detect_transaction_type()
        claims = []
        current_patient_id = None
        current_payer_id = None
        current_provider_npi = None

        for seg in self.segments:
            seg_id = seg[0] if seg else ""

            # Provider NPI from NM1*85
            if seg_id == "NM1" and len(seg) > 9 and seg[1] == "85":
                current_provider_npi = seg[9] if len(seg) > 9 else None

            # Patient ID from NM1*IL
            if seg_id == "NM1" and len(seg) > 4 and seg[1] == "IL":
                current_patient_id = seg[9] if len(seg) > 9 else None

            # Payer from NM1*PR
            if seg_id == "NM1" and len(seg) > 9 and seg[1] == "PR":
                current_payer_id = seg[9]

        # Second pass: extract claims
        i = 0
        while i < len(self.segments):
            seg = self.segments[i]
            if seg[0] == "CLM":
                claim = self._parse_claim(i, tx_type, current_provider_npi,
                                          current_patient_id, current_payer_id)
                claims.append(claim)
            i += 1

        # Resolve per-claim patient/payer by re-scanning HL/NM1 hierarchy
        self._resolve_claim_references(claims)

        return {"transaction_type": tx_type, "claims": claims}

    def _parse_claim(self, start_idx: int, tx_type: str,
                     provider_npi: str, patient_id: str, payer_id: str) -> dict:
        seg = self.segments[start_idx]
        clm05 = seg[5] if len(seg) > 5 else ""
        freq_code = "1"
        if ":" in clm05:
            parts = clm05.split(":")
            if len(parts) >= 3:
                freq_code = parts[2]

        claim = {
            "claim_id": seg[1],
            "total_billed": float(seg[2]) if len(seg) > 2 else 0.0,
            "facility_type": clm05,
            "frequency_code": freq_code,
            "transaction_type": tx_type,
            "provider_npi": provider_npi,
            "patient_id": patient_id,
            "payer_id": payer_id,
            "date_of_service": None,
            "diagnosis_codes": [],
            "lines": [],
        }

        # Scan forward for this claim's segments until next CLM or end
        j = start_idx + 1
        while j < len(self.segments):
            s = self.segments[j]
            sid = s[0] if s else ""
            if sid == "CLM":
                break
            if sid == "DTP" and len(s) > 3 and s[1] == "472":
                claim["date_of_service"] = s[3]
            if sid == "HI":
                for element in s[1:]:
                    if ":" in element:
                        code = element.split(":")[1]
                        if code:
                            claim["diagnosis_codes"].append(code)
            sv_id = "SV1" if tx_type == "837P" else "SV2"
            if sid == sv_id and len(s) > 2:
                proc_info = s[1]
                cpt = proc_info.split(":")[1] if ":" in proc_info else proc_info
                modifier = None
                if ":" in proc_info:
                    parts = proc_info.split(":")
                    modifier = parts[2] if len(parts) > 2 and parts[2] else None
                line = {
                    "cpt_code": cpt,
                    "billed_amount": float(s[2]) if s[2] else 0.0,
                    "units": int(s[4]) if len(s) > 4 and s[4] else 1,
                    "modifier": modifier,
                }
                claim["lines"].append(line)
            # Update patient/payer if NM1 appears within claim scope
            if sid == "NM1" and len(s) > 4:
                if s[1] == "IL" and len(s) > 9:
                    claim["patient_id"] = s[9]
                if s[1] == "PR" and len(s) > 9:
                    claim["payer_id"] = s[9]
            j += 1

        return claim

    def _resolve_claim_references(self, claims: List[dict]):
        """Walk the HL hierarchy to resolve patient/payer per claim."""
        current_patient = None
        current_payer = None
        current_provider = None
        claim_idx = 0

        for i, seg in enumerate(self.segments):
            sid = seg[0] if seg else ""
            if sid == "NM1" and len(seg) > 2:
                if seg[1] == "85" and len(seg) > 9:
                    current_provider = seg[9]
                elif seg[1] == "IL" and len(seg) > 9:
                    current_patient = seg[9]
                elif seg[1] == "PR" and len(seg) > 9:
                    current_payer = seg[9]
            if sid == "CLM" and claim_idx < len(claims):
                c = claims[claim_idx]
                c["provider_npi"] = current_provider
                c["patient_id"] = current_patient
                c["payer_id"] = current_payer
                claim_idx += 1
