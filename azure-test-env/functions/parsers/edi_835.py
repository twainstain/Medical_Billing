"""EDI X12 835 Parser — Electronic Remittance Advice.

Sample input format (EDI X12 835):

    TRN*1*TRC-50001*1999999999~                     # Trace number
    N1*PR*AETNA*XV*AETNA~                           # Payer
    CLP*CLM-1001*1*500.00*350.00*75.00*12~          # Claim: billed $500, paid $350
    CAS*CO*45*75.00~                                # Contractual adjustment $75
    SVC*HC:99213*150.00*100.00**1~                  # Line: billed $150, paid $100

See ingestion/parsers/edi_835.py for full docstring with parsed output schema.
"""

from typing import List, Dict, Optional


class EDI835Parser:
    def __init__(self, raw_edi: str):
        self.raw = raw_edi.strip()
        if not self.raw.startswith("ISA"):
            raise ValueError("EDI file does not start with ISA segment")
        self.element_sep = self.raw[3]
        self.seg_terminator = self.raw[105]
        self.segments = [
            s.strip().split(self.element_sep)
            for s in self.raw.split(self.seg_terminator) if s.strip()
        ]

    def parse(self) -> List[dict]:
        remittances = []
        trace_number = None
        payer_id = None
        check_number = None

        for seg in self.segments:
            seg_id = seg[0] if seg else ""

            if seg_id == "TRN" and len(seg) > 2:
                trace_number = seg[2]

            if seg_id == "BPR" and len(seg) > 1:
                # Check/EFT info sometimes in BPR
                pass

            if seg_id == "N1" and len(seg) > 2 and seg[1] == "PR":
                payer_id = seg[4] if len(seg) > 4 else seg[2]

            if seg_id == "CLP":
                remittance = {
                    "payer_claim_id": seg[1] if len(seg) > 1 else None,
                    "claim_status": seg[2] if len(seg) > 2 else None,
                    "total_billed": float(seg[3]) if len(seg) > 3 and seg[3] else 0.0,
                    "paid_amount": float(seg[4]) if len(seg) > 4 and seg[4] else 0.0,
                    "patient_responsibility": float(seg[5]) if len(seg) > 5 and seg[5] else 0.0,
                    "trace_number": trace_number,
                    "payer_id": payer_id,
                    "check_number": check_number,
                    "adjustments": [],
                    "service_lines": [],
                }
                remittances.append(remittance)

            if seg_id == "CAS" and remittances:
                group_code = seg[1] if len(seg) > 1 else ""
                idx = 2
                while idx + 1 < len(seg):
                    reason = seg[idx] if seg[idx] else None
                    amount = float(seg[idx + 1]) if seg[idx + 1] else 0.0
                    if reason:
                        remittances[-1]["adjustments"].append({
                            "group_code": group_code,
                            "reason_code": reason,
                            "amount": amount,
                        })
                    idx += 3

            if seg_id == "SVC" and remittances:
                proc_info = seg[1] if len(seg) > 1 else ""
                cpt = proc_info.split(":")[1] if ":" in proc_info else proc_info
                svc_line = {
                    "cpt_code": cpt,
                    "billed_amount": float(seg[2]) if len(seg) > 2 and seg[2] else 0.0,
                    "paid_amount": float(seg[3]) if len(seg) > 3 and seg[3] else 0.0,
                }
                remittances[-1]["service_lines"].append(svc_line)

        return remittances

    @staticmethod
    def extract_denial_code(adjustments: List[dict]) -> Optional[str]:
        """Extract primary denial code from adjustments (CO group, denial-type CARC)."""
        denial_carcs = {"4", "16", "18", "29", "50", "96", "197", "204"}
        for adj in adjustments:
            if adj["group_code"] == "CO" and adj["reason_code"] in denial_carcs:
                return adj["reason_code"]
        return None
