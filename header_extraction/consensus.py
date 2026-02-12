"""
Module 6: Multi-Page Header Stability & Header Confidence Scoring.
- Extract header candidate per page; build consensus via majority voting + confidence.
- Combine visual_score, reconstruction_score, semantic_score, data_validation_score,
  consensus_score into final confidence; pick best header set.
"""

import logging
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from .config import PipelineConfig
from .types import HeaderCandidate, NormalizedHeader

logger = logging.getLogger(__name__)


def build_consensus(
    per_page_candidates: List[HeaderCandidate],
    config: PipelineConfig,
) -> HeaderCandidate:
    """
    When PDF has multiple pages, build one consensus header from per-page candidates.
    Uses majority voting on column position + confidence weighting.
    """
    if not per_page_candidates:
        raise ValueError("No per-page candidates")
    if len(per_page_candidates) == 1:
        return per_page_candidates[0]

    # Weight by combined_confidence; prefer earlier page when tie (avoid footer from last pages)
    weighted: List[Tuple[HeaderCandidate, float, int]] = [
        (c, c.combined_confidence, c.source_pages[0] if c.source_pages else 999)
        for c in per_page_candidates
    ]
    weighted.sort(key=lambda x: (-x[1], x[2]))

    # Option A: Pick highest-confidence page's header as consensus (earlier page on tie)
    best = weighted[0][0]
    # Option B: For each column index, vote over pages (if same column count)
    lengths = [len(c.headers) for c in per_page_candidates]
    if len(set(lengths)) == 1 and lengths[0] > 0:
        n_cols = lengths[0]
        voted_headers: List[str] = []
        for col_idx in range(n_cols):
            col_values = [
                c.headers[col_idx]
                for c in per_page_candidates
                if col_idx < len(c.headers)
            ]
            if not col_values:
                voted_headers.append(best.headers[col_idx] if col_idx < len(best.headers) else "")
                continue
            # Majority vote with confidence weight
            counter: Dict[str, float] = {}
            for c in per_page_candidates:
                if col_idx >= len(c.headers):
                    continue
                val = c.headers[col_idx]
                counter[val] = counter.get(val, 0) + c.combined_confidence
            if counter:
                winner = max(counter.items(), key=lambda x: x[1])
                voted_headers.append(winner[0])
            else:
                voted_headers.append(col_values[0])
        consensus_score = sum(1 for i in range(n_cols) if i < len(best.headers) and voted_headers[i] == best.headers[i]) / max(1, n_cols)
        return HeaderCandidate(
            headers=voted_headers,
            visual_score=best.visual_score,
            reconstruction_score=best.reconstruction_score,
            semantic_score=best.semantic_score,
            data_validation_score=best.data_validation_score,
            consensus_score=consensus_score,
            combined_confidence=combine_confidence(
                best.visual_score,
                best.reconstruction_score,
                best.semantic_score,
                best.data_validation_score,
                consensus_score,
                config,
            ),
            source_pages=[c.source_pages[0] for c in per_page_candidates if c.source_pages],
            debug={"voted_headers": voted_headers, "per_page": [c.headers for c in per_page_candidates]},
        )
    return best


def combine_confidence(
    visual: float,
    reconstruction: float,
    semantic: float,
    data_validation: float,
    consensus: float,
    config: PipelineConfig,
) -> float:
    """Weighted combination of scores. Weights must sum to 1.0."""
    w = config
    return (
        w.weight_visual * visual
        + w.weight_reconstruction * reconstruction
        + w.weight_semantic * semantic
        + w.weight_data_validation * data_validation
        + w.weight_consensus * consensus
    )


def select_best_candidate(candidates: List[HeaderCandidate]) -> HeaderCandidate:
    """Select single best by combined_confidence. Deterministic (max)."""
    if not candidates:
        raise ValueError("No candidates")
    return max(candidates, key=lambda c: c.combined_confidence)
