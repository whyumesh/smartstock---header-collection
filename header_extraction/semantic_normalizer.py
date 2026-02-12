"""
Module 4: Semantic Header Understanding.
Offline semantic matching: fuzzy text similarity, token similarity, character distance,
and domain synonym dictionary (Indian pharma/FMCG context).
Normalizes e.g. Qty → Quantity, Net Qty → Quantity, Batch → Batch No.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

try:
    from rapidfuzz import fuzz
    from rapidfuzz.distance import Levenshtein
except ImportError:
    fuzz = None  # type: ignore
    Levenshtein = None  # type: ignore

from .config import PipelineConfig
from .types import NormalizedHeader, ReconstructedHeader

logger = logging.getLogger(__name__)


# Domain synonym dictionary: Indian pharma / FMCG distribution
DOMAIN_SYNONYMS: Dict[str, str] = {
    "qty": "Quantity",
    "quantity": "Quantity",
    "net qty": "Net Quantity",
    "net quantity": "Net Quantity",
    "sales qty": "Sales Quantity",
    "free qty": "Free Quantity",
    "product name": "Product Name",
    "product": "Product Name",
    "item name": "Product Name",
    "item": "Product Name",
    "batch": "Batch No",
    "batch no": "Batch No",
    "batch no.": "Batch No",
    "batch number": "Batch No",
    "expiry": "Expiry Date",
    "expiry date": "Expiry Date",
    "exp date": "Expiry Date",
    "exp.": "Expiry Date",
    "mrp": "MRP",
    "m.r.p": "MRP",
    "ptr": "PTR",
    "pts": "PTS",
    "rate": "Rate",
    "price": "Rate",
    "amount": "Amount",
    "value": "Amount",
    "scheme": "Scheme",
    "free": "Free Quantity",
    "gst": "GST",
    "gst %": "GST",
    "tax": "GST",
    "invoice no": "Invoice No",
    "invoice no.": "Invoice No",
    "invoice number": "Invoice No",
    "inv no": "Invoice No",
    "date": "Date",
    "invoice date": "Invoice Date",
    "bill date": "Invoice Date",
    "product code": "Product Code",
    "code": "Product Code",
    "hsn": "HSN",
    "hsn code": "HSN",
    "discount": "Discount",
    "pack": "Pack",
    "pack size": "Pack Size",
    "unit": "Unit",
    "strip": "Strip",
    "box": "Box",
    # Stock & sales report column names
    "lstsl": "LstSL",
    "lstmove": "LstMove",
    "stk.value": "Stk.Value",
    "stk value": "Stk.Value",
    "recd": "Recd.",
    "recd.": "Recd.",
    "cl qty": "Cl Qty",
    "cl val": "Cl Val",
    "cl val.": "Cl Val",
    "gdin": "Gd.In",
    "gd.in": "Gd.In",
    "tot": "Tot",
    "pur": "Purchase",
    "op": "Op",
    "cl": "Cl",
    "inqty": "InQty",
    "outqty": "OutQty",
    "co op qty": "Co Op Qty",
    "product name": "Product Name",
    "exp": "Exp",
    "exp.": "Exp",
    "out": "Out",
    "out.": "Out.",
    "closing": "Closing",
    "order": "Order",
    "pend": "Pend",
    "near": "Near",
    "gd": "Gd",
    "sale": "Sale",
    "sales": "Sales",
    "stock": "Stock",
}


def _normalize_key(s: str) -> str:
    """Lowercase, collapse spaces, remove punctuation for matching."""
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


class SemanticHeaderNormalizer:
    """
    Normalizes raw header text to canonical domain terms.
    Uses exact domain dict first, then fuzzy match.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._domain_keys = {_normalize_key(k): v for k, v in DOMAIN_SYNONYMS.items()}

    def normalize(
        self, reconstructed: List[ReconstructedHeader]
    ) -> Tuple[List[NormalizedHeader], float]:
        """
        Normalize each reconstructed header.
        Returns (list of NormalizedHeader in same order, avg semantic score 0-1).
        """
        result: List[NormalizedHeader] = []
        scores: List[float] = []
        for r in reconstructed:
            norm, score = self._normalize_one(r.text)
            result.append(norm)
            scores.append(score)
        avg = sum(scores) / len(scores) if scores else 0.0
        return result, min(1.0, avg)

    def _normalize_one(self, raw: str) -> Tuple[NormalizedHeader, float]:
        """Normalize single header string. Prefer domain dict then fuzzy."""
        key = _normalize_key(raw)
        if not key:
            return NormalizedHeader(original=raw, normalized=raw, semantic_score=0.5, source="exact"), 0.5

        # 1) Exact domain match
        if self.config.domain_dict_priority and key in self._domain_keys:
            canonical = self._domain_keys[key]
            return (
                NormalizedHeader(
                    original=raw,
                    normalized=canonical,
                    semantic_score=1.0,
                    source="domain",
                ),
                1.0,
            )

        # 2) Partial key match (e.g. "batch no." contains "batch no"); skip when key is very short (avoid "in" -> Invoice No)
        if len(key) > 3:
            for dk, canonical in self._domain_keys.items():
                if dk in key or key in dk:
                    return (
                        NormalizedHeader(
                            original=raw,
                            normalized=canonical,
                            semantic_score=0.95,
                            source="domain",
                        ),
                        0.95,
                    )
        # Short tokens that should stay as-is (column headers like In, Op, Cl)
        if key in ("in", "op", "cl", "tot", "exp", "out", "gd", "sale"):
            return (
                NormalizedHeader(original=raw, normalized=raw.strip(), semantic_score=1.0, source="exact"),
                1.0,
            )

        # 3) Fuzzy match against domain keys
        if fuzz is not None and self.config.fuzzy_min_score > 0:
            best_score = 0
            best_canonical = raw
            for dk, canonical in self._domain_keys.items():
                ratio = fuzz.ratio(key, dk)
                if ratio >= self.config.fuzzy_min_score and ratio > best_score:
                    best_score = ratio
                    best_canonical = canonical
            if best_score > 0:
                return (
                    NormalizedHeader(
                        original=raw,
                        normalized=best_canonical,
                        semantic_score=best_score / 100.0,
                        source="fuzzy",
                    ),
                    best_score / 100.0,
                )

        # 4) No match: keep original, slight cleanup
        cleaned = raw.strip()
        if len(cleaned) > 1 and cleaned[0].isupper():
            pass
        else:
            cleaned = raw.strip().title() if raw.strip() else raw
        return (
            NormalizedHeader(
                original=raw,
                normalized=cleaned,
                semantic_score=0.6,
                source="exact",
            ),
            0.6,
        )


def get_domain_canonicals() -> List[str]:
    """Return list of canonical header names for validation/UI."""
    return list(dict.fromkeys(DOMAIN_SYNONYMS.values()))
