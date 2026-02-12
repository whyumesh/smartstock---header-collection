"""
Post-filter: drop columns that are clearly not headers (page numbers, logos, data spill).
Anti-noise lexical filter + header word shape validation for precision.
"""

import re
import logging
from typing import List, Tuple

from .types import ReconstructedHeader

logger = logging.getLogger(__name__)

# Tokens that indicate non-header (footer, page, logo)
SKIP_HEADER_PATTERNS = [
    r"^page\s*\d*$",
    r"^\d+$",
    r"^:$",
    r"^\.$",
    r"^\d+\s*/\s*\d+$",  # page 1/5
]
SKIP_HEADER_EXACT = {"page", ":", ".", ""}

# Anti-noise: reject if isolated and not column-like (title/footer/watermark).
# Allow if part of compound known header (e.g. "Date" in "Invoice Date" is allowed).
HEADER_NEGATIVE_DICTIONARY = frozenset({
    "page", "report", "statement", "stock", "sales report", "date:", "time:",
    "generated", "confidential", "gstin", "address", "phone", "email",
    "invoice", "document", "copy", "original", "duplicate",
    "page no", "page no.", "page number", "of",
})
# Known compound headers where a negative token may appear (allow these)
COMPOUND_HEADERS_ALLOWING_NEGATIVE = frozenset({
    "invoice date", "invoice no", "expiry date", "product name",
    "batch no", "sales quantity", "stock statement", "stock report",
})


def _is_noise_isolated(text: str) -> bool:
    """True if text is an isolated noise word (negative dict) and not part of allowed compound."""
    t = text.strip().lower()
    if not t:
        return True
    # Allow compound known headers even if they contain a negative term
    if t in COMPOUND_HEADERS_ALLOWING_NEGATIVE:
        return False
    for comp in COMPOUND_HEADERS_ALLOWING_NEGATIVE:
        if comp in t or t in comp:
            return False
    # Reject if full phrase is in negative (e.g. "Sales Report", "Page")
    if t in HEADER_NEGATIVE_DICTIONARY:
        return True
    # Single token in negative list
    tokens = t.split()
    if len(tokens) == 1 and tokens[0] in HEADER_NEGATIVE_DICTIONARY:
        return True
    return False


def _fails_header_word_shape(text: str) -> bool:
    """
    True if text looks like non-header: full sentence, date string, page pattern, company name.
    """
    t = text.strip()
    if not t or len(t) > 120:
        return True
    lower = t.lower()
    # Full sentence fragment (ends with period, multiple clauses)
    if t.endswith(".") and len(t.split()) >= 5:
        return True
    # Date string patterns (DD/MM/YYYY etc.)
    if re.search(r"\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}", t):
        return True
    if re.search(r"\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}", t):
        return True
    # Page numbering: "Page 1 of 5", "1 / 10"
    if re.match(r"^page\s*\d+", lower) or re.match(r"^\d+\s*/\s*\d+$", lower):
        return True
    # Company name heuristic: long mixed case with no typical header keyword
    if len(t) > 40 and t != t.upper() and t != t.lower():
        return True
    return False


def is_likely_header(text: str) -> bool:
    """Return False if text is clearly not a column header."""
    t = text.strip()
    if not t or len(t) > 120:
        return False
    lower = t.lower()
    if lower in SKIP_HEADER_EXACT:
        return False
    for pat in SKIP_HEADER_PATTERNS:
        if re.match(pat, lower):
            return False
    # Single digit or single letter
    if len(t) == 1 and (t.isdigit() or t.isalpha()):
        return False
    # Too many space-separated tokens = likely data row merged
    if len(t.split()) > 12:
        return False
    # Anti-noise: isolated negative words
    if _is_noise_isolated(t):
        return False
    # Header word shape: reject sentence/date/page/company
    if _fails_header_word_shape(t):
        return False
    return True


def filter_reconstructed_with_noise_tracking(
    headers: List[ReconstructedHeader],
) -> Tuple[List[ReconstructedHeader], List[str]]:
    """
    Drop reconstructed headers that fail is_likely_header; track removed noise tokens for debug.
    Returns (filtered_headers, noise_tokens_removed).
    """
    noise_removed: List[str] = []
    filtered: List[ReconstructedHeader] = []
    for h in headers:
        if is_likely_header(h.text):
            filtered.append(h)
        else:
            noise_removed.append(h.text.strip())
    # Re-index
    result = []
    for i, h in enumerate(filtered):
        result.append(
            ReconstructedHeader(
                text=h.text,
                column_index=i,
                x_min=h.x_min,
                x_max=h.x_max,
                tokens=h.tokens,
                reconstruction_score=h.reconstruction_score,
            )
        )
    return result, noise_removed


def filter_reconstructed(headers: List[ReconstructedHeader]) -> List[ReconstructedHeader]:
    """Drop reconstructed headers that fail is_likely_header; preserve order and indices."""
    filtered, _ = filter_reconstructed_with_noise_tracking(headers)
    return filtered
