"""
Module 5: Column Data Cross Validation (State-of-art part).
Validates headers using column data:
- Dates → Expiry / Invoice Date
- Decimals → Price / Rate / MRP
- Integers → Quantity
- Alphanumeric codes → Batch / Product Code
Rejects headers that do not match column data distribution.
"""

import logging
import re
from collections import Counter
from typing import List, Optional, Tuple

from .config import PipelineConfig
from .layout_reader import LayoutDocument
from .types import ColumnDataProfile, HeaderZone, NormalizedHeader, WordBlock

logger = logging.getLogger(__name__)


# Header → expected data type (for validation)
HEADER_TO_EXPECTED_TYPE: dict = {
    "Expiry Date": "date",
    "Invoice Date": "date",
    "Date": "date",
    "Invoice No": "alphanumeric",
    "Product Code": "alphanumeric",
    "Batch No": "alphanumeric",
    "HSN": "alphanumeric",
    "Product Name": "text",
    "Quantity": "integer",
    "Net Quantity": "integer",
    "Sales Quantity": "integer",
    "Free Quantity": "integer",
    "MRP": "decimal",
    "PTR": "decimal",
    "PTS": "decimal",
    "Rate": "decimal",
    "Amount": "decimal",
    "GST": "decimal",
    "Discount": "decimal",
    "Pack": "text",
    "Unit": "text",
}

# Domain header priority (Indian pharma/FMCG): boost confidence when present
DOMAIN_PRIORITY_HEADERS = frozenset({
    "product", "name", "pack", "batch", "mrp", "ptr", "pts", "qty", "value",
    "sales", "purchase", "stock", "open", "close", "in", "out", "order",
    "pending", "expiry", "product name", "batch no", "expiry date", "quantity",
    "amount", "rate", "cl qty", "cl val", "co op qty",
})


def _infer_value_type(value: str) -> str:
    """Infer single value type: date, decimal, integer, alphanumeric, text."""
    v = value.strip()
    if not v:
        return "text"
    # Date patterns (DD/MM/YYYY, DD-MM-YYYY, etc.)
    if re.match(r"^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$", v):
        return "date"
    if re.match(r"^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$", v):
        return "date"
    # Decimal
    if re.match(r"^-?\d+\.\d+$", v) or re.match(r"^-?\d+,\d{2}$", v):
        return "decimal"
    if re.match(r"^-?\d+\.\d*$", v):
        return "decimal"
    # Integer
    if re.match(r"^-?\d+$", v):
        return "integer"
    # Alphanumeric (short codes)
    if re.match(r"^[A-Za-z0-9\-/]+$", v) and len(v) <= 30:
        return "alphanumeric"
    return "text"


class ColumnDataValidator:
    """
    Samples cell values below header zone per column and infers type.
    Scores how well normalized headers match expected types.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def validate(
        self,
        layout: LayoutDocument,
        zone: HeaderZone,
        column_intervals: List[Tuple[float, float]],
        normalized_headers: List[NormalizedHeader],
        page_no: int = 0,
    ) -> Tuple[List[ColumnDataProfile], float, int]:
        """
        For each column interval, sample words below header zone, infer type.
        Compare with expected type from header; return profiles, overall score,
        and data_type_mismatch_count (for precision penalties).
        """
        words_below = self._get_data_zone_words(layout, zone, page_no)
        if not words_below:
            return [], 0.5, 0

        # Assign each word to column by x position
        def col_for_word(w: WordBlock) -> int:
            mid = w.mid_x
            best = 0
            best_dist = float("inf")
            for i, (a, b) in enumerate(column_intervals):
                if a <= mid <= b:
                    return i
                dist = min(abs(mid - a), abs(mid - b))
                if dist < best_dist:
                    best_dist = dist
                    best = i
            return best

        col_samples: dict = {}
        for w in words_below:
            c = col_for_word(w)
            col_samples.setdefault(c, []).append(w.text)
        # Cap samples per column
        n_sample = self.config.sample_cells_per_column
        for c in col_samples:
            col_samples[c] = col_samples[c][:n_sample]

        profiles: List[ColumnDataProfile] = []
        match_scores: List[float] = []
        data_type_mismatch_count = 0
        domain_boost = getattr(self.config, "domain_header_boost", 0.0)
        for i in range(max(col_samples.keys()) + 1 if col_samples else 0):
            samples = col_samples.get(i, [])
            if not samples:
                profiles.append(
                    ColumnDataProfile(
                        column_index=i,
                        sample_values=[],
                        inferred_type="unknown",
                        match_ratio=0.0,
                        validation_score=0.5,
                    )
                )
                match_scores.append(0.5)
                continue
            types = [_infer_value_type(s) for s in samples]
            counter = Counter(types)
            inferred = counter.most_common(1)[0][0]
            match_ratio = counter[inferred] / len(types)
            # Expected type from header (if we have normalized header for this column)
            expected = "unknown"
            if i < len(normalized_headers):
                expected = HEADER_TO_EXPECTED_TYPE.get(
                    normalized_headers[i].normalized, "unknown"
                )
            if expected == "unknown":
                validation_score = 0.5 + match_ratio * 0.3  # Slight boost for consistent type
                is_mismatch = False
            elif expected == inferred:
                validation_score = 0.7 + match_ratio * 0.3
                is_mismatch = False
            else:
                # Allow integer for decimal (quantity vs rate) sometimes
                if expected == "decimal" and inferred == "integer":
                    validation_score = 0.6
                    is_mismatch = False
                elif expected == "integer" and inferred == "decimal":
                    validation_score = 0.5
                    is_mismatch = False
                else:
                    validation_score = 0.3
                    is_mismatch = True
            if is_mismatch:
                data_type_mismatch_count += 1
            # Domain header priority boost
            if i < len(normalized_headers):
                norm_lower = normalized_headers[i].normalized.strip().lower()
                if norm_lower in DOMAIN_PRIORITY_HEADERS:
                    validation_score = min(1.0, validation_score + domain_boost)
            profiles.append(
                ColumnDataProfile(
                    column_index=i,
                    sample_values=samples[:5],
                    inferred_type=inferred,
                    match_ratio=match_ratio,
                    validation_score=validation_score,
                )
            )
            match_scores.append(validation_score)

        overall = sum(match_scores) / len(match_scores) if match_scores else 0.5
        return profiles, min(1.0, overall), data_type_mismatch_count

    def _get_data_zone_words(
        self, layout: LayoutDocument, zone: HeaderZone, page_no: int
    ) -> List[WordBlock]:
        """Words below header zone (first data rows)."""
        words = layout.words_for_page(page_no)
        # Take words with y > zone.y_max, limit to first N rows (e.g. 100 words or 20 lines)
        below = [w for w in words if w.y0 > zone.y_max]
        below.sort(key=lambda w: (w.y0, w.x0))
        # Heuristic: take up to first 200 words as "data" to sample
        return below[:200]
