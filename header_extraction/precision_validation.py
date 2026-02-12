"""
Precision validation layer: prove header using table + column + data + structure evidence.
Reject candidate if evidence < threshold. Optimize for precision, not recall.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from .config import PipelineConfig
from .data_column_detector import detect_data_column_count
from .types import (
    HeaderCandidate,
    HeaderZone,
    NormalizedHeader,
    ReconstructedHeader,
    WordBlock,
)

logger = logging.getLogger(__name__)


def column_count_consistency_gate(
    header_column_count: int,
    data_column_count: int,
    max_diff: int,
) -> Tuple[bool, str]:
    """
    HARD RULE: header_column_count MUST approximately match data_column_count.
    Difference allowed = max(±max_diff). If mismatch → candidate INVALID.
    Exception: header > data by up to 6 when header in 5–20 (merged/undercounted data cells).
    """
    if data_column_count <= 0:
        return True, "no_data_to_compare"
    diff = abs(header_column_count - data_column_count)
    if diff <= max_diff:
        return True, "column_count_ok: header=%d data=%d" % (header_column_count, data_column_count)
    # Allow header > data when data likely undercounted (merged cells / multi-line header splitting)
    excess = header_column_count - data_column_count
    if header_column_count >= data_column_count and excess <= 8 and 5 <= header_column_count <= 25:
        return True, "column_count_ok_header_ge_data: header=%d data=%d" % (header_column_count, data_column_count)
    if diff > max_diff:
        return False, "column_count_mismatch: header=%d data=%d diff=%d" % (
            header_column_count,
            data_column_count,
            diff,
        )
    return True, "column_count_ok: header=%d data=%d" % (header_column_count, data_column_count)


def compute_lexical_purity_score(
    headers: List[str],
    noise_tokens_removed: List[str],
    domain_boost_headers: set,
) -> float:
    """
    Score 0-1: higher when headers are column-like and domain-known, lower when noise was removed.
    """
    if not headers:
        return 0.0
    penalty = len(noise_tokens_removed) * 0.12  # per noise token
    boost = sum(0.08 for h in headers if h.strip() and h.strip().lower() in domain_boost_headers)
    base = 1.0 - penalty + boost
    return max(0.0, min(1.0, base))


def compute_keyword_score(headers: List[str], header_keywords: set) -> float:
    """Fraction of header tokens that are known header keywords (stock/sales domain)."""
    if not headers:
        return 0.0
    total = 0
    hits = 0
    for h in headers:
        tokens = h.strip().lower().split()
        for t in tokens:
            if len(t) < 2:
                continue
            total += 1
            if t in header_keywords or any(kw in t for kw in header_keywords):
                hits += 1
    if total == 0:
        return 0.5
    return hits / total


def compute_column_alignment_score(
    header_column_count: int,
    data_column_count: int,
    max_diff: int,
) -> float:
    """1.0 when exact match, lower when diff increases within allowed."""
    if data_column_count <= 0:
        return 0.5
    diff = abs(header_column_count - data_column_count)
    if diff > max_diff:
        return 0.0
    if diff == 0:
        return 1.0
    return 1.0 - (diff / (max_diff + 1)) * 0.5


def apply_confidence_penalties(
    base_confidence: float,
    merged_column_penalty: bool,
    noise_count: int,
    data_type_mismatch_count: int,
    title_zone_count: int,
    config: PipelineConfig,
) -> Tuple[float, Dict[str, Any]]:
    """
    Apply precision penalties. Returns (adjusted_confidence, penalty_breakdown).
    """
    c = config
    adj = base_confidence
    breakdown: Dict[str, Any] = {}
    if merged_column_penalty:
        adj -= c.merged_column_penalty
        breakdown["merged_column_penalty"] = c.merged_column_penalty
    if noise_count > 0:
        p = min(0.4, noise_count * c.noise_word_penalty)
        adj -= p
        breakdown["noise_word_penalty"] = p
    if data_type_mismatch_count > 0:
        p = min(0.4, data_type_mismatch_count * c.data_type_mismatch_penalty)
        adj -= p
        breakdown["data_type_mismatch_penalty"] = p
    if title_zone_count > 0:
        p = min(0.4, title_zone_count * c.title_zone_penalty)
        adj -= p
        breakdown["title_zone_penalty"] = p
    adj = max(0.0, min(1.0, adj))
    return adj, breakdown


def validate_candidate_with_evidence(
    candidate: HeaderCandidate,
    data_column_count: int,
    layout_num_pages: int,
    source_pages: List[int],
    noise_tokens_removed: List[str],
    data_type_mismatch_count: int,
    title_zone_token_count: int,
    config: PipelineConfig,
    header_keywords: set,
    domain_boost_headers: set,
) -> Tuple[bool, str, float, Dict[str, Any]]:
    """
    Prove this is header using table + column + data + structure evidence.
    Returns (is_valid, reject_reason_or_empty, adjusted_confidence, debug_dict).
    If evidence < threshold → reject (is_valid=False).
    """
    debug: Dict[str, Any] = {}
    headers = candidate.headers
    header_cols = len(headers)

    # 0) Minimum column count: real table headers have at least 3 columns
    if header_cols < 3:
        return False, "too_few_columns: %d" % header_cols, 0.0, {"column_count_gate": {"passed": False, "reason": "too_few_columns"}}

    # 1) Column Count Consistency Gate (HARD)
    ok, msg = column_count_consistency_gate(
        header_cols,
        data_column_count,
        config.max_header_data_column_diff,
    )
    debug["column_count_gate"] = {"passed": ok, "reason": msg}
    if not ok:
        return False, msg, 0.0, debug

    # 2) Penalties
    merged_penalty = header_cols != data_column_count and data_column_count > 0
    adj_confidence, penalty_breakdown = apply_confidence_penalties(
        candidate.combined_confidence,
        merged_penalty,
        len(noise_tokens_removed),
        data_type_mismatch_count,
        title_zone_token_count,
        config,
    )
    debug["penalty_breakdown"] = penalty_breakdown
    debug["adjusted_confidence"] = adj_confidence

    # 3) Multi-candidate style scores for evidence
    keyword_score = compute_keyword_score(headers, header_keywords)
    column_align_score = compute_column_alignment_score(
        header_cols, data_column_count, config.max_header_data_column_diff
    )
    lexical_purity = compute_lexical_purity_score(
        headers, noise_tokens_removed, domain_boost_headers
    )
    # Multi-page presence: higher if header appears on more pages
    if layout_num_pages <= 1:
        multi_page_score = 1.0
    else:
        multi_page_score = len(source_pages) / max(1, layout_num_pages)
    debug["evidence_scores"] = {
        "keyword_score": keyword_score,
        "column_alignment_score": column_align_score,
        "data_validation_score": candidate.data_validation_score,
        "lexical_purity_score": lexical_purity,
        "multi_page_presence_score": multi_page_score,
    }
    # Evidence = raw scores (threshold check); penalties only affect adjusted_confidence
    evidence = (
        keyword_score * 0.25
        + column_align_score * 0.25
        + candidate.data_validation_score * 0.25
        + lexical_purity * 0.15
        + multi_page_score * 0.10
    )
    evidence = max(0.0, min(1.0, evidence))
    debug["evidence_total"] = evidence
    debug["evidence_before_penalties"] = evidence

    if evidence < config.precision_evidence_threshold:
        return (
            False,
            "evidence_below_threshold: %.2f < %.2f" % (evidence, config.precision_evidence_threshold),
            adj_confidence,
            debug,
        )
    return True, "", adj_confidence, debug
