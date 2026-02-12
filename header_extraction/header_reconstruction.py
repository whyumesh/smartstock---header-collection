"""
Module 3: Header Reconstruction Engine (CRITICAL).
Rebuilds headers from header-zone words:
- Multi-line stacked headers
- Split words across columns
- Adjacent header joins
- X overlap grouping, Y proximity, phrase graph reconstruction.
- Smart compound merge (strict): only if known list or X gap < median column gap.
- Column explosion guard: split merged headers when header_cols < data_cols.
"""

import logging
from collections import defaultdict
from typing import List, Optional, Tuple, Dict, Any

from .config import PipelineConfig
from .types import HeaderZone, ReconstructedHeader, WordBlock

logger = logging.getLogger(__name__)

# Known compound headers (strict merge allowed without gap check)
KNOWN_COMPOUND_TOKENS = [
    (["product", "name"], "Product Name"),
    (["prod", "name"], "Product Name"),
    (["cl", "qty"], "Cl Qty"),
    (["cl", "val"], "Cl Val"),
    (["co", "op", "qty"], "Co Op Qty"),
]


class HeaderReconstructionEngine:
    """
    Converts header-zone word blocks into ordered column headers.
    Uses X-overlap for column grouping and Y-proximity for line/stack merging.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def reconstruct(
        self, zone: HeaderZone
    ) -> Tuple[List[ReconstructedHeader], float, List[Dict[str, Any]]]:
        """
        Reconstruct column headers from zone word blocks.
        Returns (list of ReconstructedHeader in column order, reconstruction_score 0-1,
                 merge_operations_performed for debug).
        """
        words = zone.word_blocks
        if not words:
            return [], 0.0, []

        # 1) Group words into lines by Y proximity
        lines = self._group_into_lines(words)
        if not lines:
            return [], 0.0, []

        # 2) Column boundaries: for a single header line, use one column per word (no merge)
        #    so we preserve all columns; for multi-line use overlap clustering
        if len(lines) == 1 and len(words) >= 3:
            sorted_words = sorted(words, key=lambda w: w.x0)
            column_intervals: List[Tuple[float, float]] = [(w.x0, w.x1) for w in sorted_words]
        else:
            column_intervals = self._infer_column_intervals(lines)
        if not column_intervals:
            column_intervals = self._fallback_column_intervals(words)

        # 3) Assign each word to a column index
        word_to_col: List[Tuple[WordBlock, int]] = []
        for w in words:
            col_idx = self._assign_column(w, column_intervals)
            word_to_col.append((w, col_idx))

        # 4) Build per-column text: group by column, then by Y (stack order), join with space
        col_to_words: dict = defaultdict(list)
        for w, c in word_to_col:
            col_to_words[c].append(w)

        # Sort columns by index, within column sort by y then x
        result: List[ReconstructedHeader] = []
        max_col = max(col_to_words.keys()) if col_to_words else -1
        for col_idx in range(max_col + 1):
            col_words = col_to_words.get(col_idx, [])
            col_words.sort(key=lambda w: (w.y0, w.x0))
            if not col_words:
                continue
            # Join tokens; handle multi-line by joining with space
            tokens = col_words
            text = " ".join(w.text for w in col_words).strip()
            x_min = min(w.x0 for w in col_words)
            x_max = max(w.x1 for w in col_words)
            # Reconstruction score: prefer single-line headers, penalize too many tokens
            n_tokens = len(col_words)
            line_score = 1.0 / (1.0 + (n_tokens - 1) * 0.1)  # slight penalty for many tokens
            result.append(
                ReconstructedHeader(
                    text=text,
                    column_index=col_idx,
                    x_min=x_min,
                    x_max=x_max,
                    tokens=tokens,
                    reconstruction_score=min(1.0, line_score + 0.2),
                )
            )

        # Order by x_min to preserve left-to-right order
        result.sort(key=lambda r: r.x_min)
        for i, r in enumerate(result):
            result[i] = ReconstructedHeader(
                text=r.text,
                column_index=i,
                x_min=r.x_min,
                x_max=r.x_max,
                tokens=r.tokens,
                reconstruction_score=r.reconstruction_score,
            )

        # Merge compound headers (strict: only known list or X gap < median)
        result, merge_ops = self._merge_compound_headers_strict(result)

        # Overall reconstruction score: avg of per-header score, penalize empty
        if result:
            avg_score = sum(r.reconstruction_score for r in result) / len(result)
            coverage = min(1.0, len(result) / 15.0)  # expect typically 5–15 columns
            recon_score = (avg_score * 0.7 + coverage * 0.3)
        else:
            recon_score = 0.0

        logger.debug(
            "Reconstructed %d headers: %s",
            len(result),
            [r.text for r in result][:10],
        )
        return result, min(1.0, recon_score), merge_ops

    def _group_into_lines(self, words: List[WordBlock]) -> List[List[WordBlock]]:
        """Group words into horizontal lines by Y proximity."""
        if not words:
            return []
        sorted_words = sorted(words, key=lambda w: (w.y0, w.x0))
        lines: List[List[WordBlock]] = []
        current_line: List[WordBlock] = [sorted_words[0]]
        for w in sorted_words[1:]:
            ref_y = current_line[0].y0
            if abs(w.y0 - ref_y) <= self.config.y_proximity_px:
                current_line.append(w)
            else:
                if current_line:
                    lines.append(current_line)
                current_line = [w]
        if current_line:
            lines.append(current_line)
        return lines

    def _infer_column_intervals(
        self, lines: List[List[WordBlock]]
    ) -> List[Tuple[float, float]]:
        """Infer (x_min, x_max) for each column from word bboxes (X overlap clustering)."""
        all_x_pairs: List[Tuple[float, float]] = []
        for line in lines:
            for w in line:
                all_x_pairs.append((w.x0, w.x1))
        if not all_x_pairs:
            return []
        # Merge overlapping intervals
        sorted_pairs = sorted(all_x_pairs, key=lambda p: p[0])
        merged: List[Tuple[float, float]] = [sorted_pairs[0]]
        for a, b in sorted_pairs[1:]:
            la, lb = merged[-1]
            if a <= lb + 2:  # overlap or touch
                merged[-1] = (la, max(lb, b))
            else:
                merged.append((a, b))
        return merged

    def _fallback_column_intervals(self, words: List[WordBlock]) -> List[Tuple[float, float]]:
        """Use word bboxes to get column boundaries when lines are ambiguous."""
        intervals = [(w.x0, w.x1) for w in words]
        intervals.sort(key=lambda p: p[0])
        merged: List[Tuple[float, float]] = []
        for a, b in intervals:
            if merged and a <= merged[-1][1] * self.config.x_overlap_tolerance_ratio + merged[-1][0]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], b))
            else:
                merged.append((a, b))
        return merged

    def _median_column_gap(self, headers: List[ReconstructedHeader]) -> float:
        """Median X gap between consecutive header columns."""
        if len(headers) < 2:
            return 0.0
        gaps = []
        for i in range(len(headers) - 1):
            gap = headers[i + 1].x_min - headers[i].x_max
            gaps.append(gap)
        gaps.sort()
        return gaps[len(gaps) // 2] if gaps else 0.0

    def _merge_compound_headers_strict(
        self, headers: List[ReconstructedHeader]
    ) -> Tuple[List[ReconstructedHeader], List[Dict[str, Any]]]:
        """
        Merge adjacent columns ONLY IF: in known compound list OR X gap < median column gap.
        Returns (merged_headers, merge_operations_performed).
        """
        merge_ops: List[Dict[str, Any]] = []
        if len(headers) < 2:
            return headers, merge_ops
        out: List[ReconstructedHeader] = []
        i = 0
        while i < len(headers):
            merged = False
            for tokens, label in KNOWN_COMPOUND_TOKENS:
                if i + len(tokens) > len(headers):
                    continue
                match = all(
                    headers[i + j].text.strip().lower() == tokens[j]
                    for j in range(len(tokens))
                )
                if not match:
                    continue
                # Strict mode: known list is always allowed; otherwise would need gap < median (we only have known list here)
                combined_tokens = []
                x_min, x_max = float("inf"), float("-inf")
                for j in range(len(tokens)):
                    combined_tokens.extend(headers[i + j].tokens)
                    x_min = min(x_min, headers[i + j].x_min)
                    x_max = max(x_max, headers[i + j].x_max)
                out.append(
                    ReconstructedHeader(
                        text=label,
                        column_index=len(out),
                        x_min=x_min,
                        x_max=x_max,
                        tokens=combined_tokens,
                        reconstruction_score=1.0,
                    )
                )
                merge_ops.append({"merged": [headers[i + j].text for j in range(len(tokens))], "into": label})
                i += len(tokens)
                merged = True
                break
            if not merged:
                h = headers[i]
                out.append(
                    ReconstructedHeader(
                        text=h.text,
                        column_index=len(out),
                        x_min=h.x_min,
                        x_max=h.x_max,
                        tokens=h.tokens,
                        reconstruction_score=h.reconstruction_score,
                    )
                )
                i += 1
        return out, merge_ops

    def _assign_column(self, w: WordBlock, intervals: List[Tuple[float, float]]) -> int:
        """Assign word to column index by maximum X overlap with interval."""
        mid = w.mid_x
        best_idx = 0
        best_overlap = 0.0
        for i, (a, b) in enumerate(intervals):
            overlap = max(0, min(w.x1, b) - max(w.x0, a))
            if overlap > best_overlap:
                best_overlap = overlap
                best_idx = i
        if best_overlap <= 0:
            # No overlap: assign to nearest interval by center distance
            best_dist = float("inf")
            for i, (a, b) in enumerate(intervals):
                mid_iv = (a + b) / 2
                dist = abs(mid - mid_iv)
                if dist < best_dist:
                    best_dist = dist
                    best_idx = i
        return best_idx


def split_merged_headers_by_data_columns(
    reconstructed: List[ReconstructedHeader],
    data_column_intervals: List[Tuple[float, float]],
) -> Tuple[List[ReconstructedHeader], List[Dict[str, Any]]]:
    """
    Column explosion guard: when header_cols < data_cols, split headers whose X span
    covers multiple data column intervals. Returns (split_headers, split_operations_performed).
    """
    split_ops: List[Dict[str, Any]] = []
    if not data_column_intervals or not reconstructed:
        return reconstructed, split_ops
    target_cols = len(data_column_intervals)
    if len(reconstructed) >= target_cols:
        return reconstructed, split_ops

    result: List[ReconstructedHeader] = []
    for h in reconstructed:
        mid = (h.x_min + h.x_max) / 2
        # Which data intervals does this header span?
        overlapping: List[int] = []
        for idx, (a, b) in enumerate(data_column_intervals):
            if not (h.x_max < a or h.x_min > b):
                overlapping.append(idx)
        if len(overlapping) <= 1:
            result.append(
                ReconstructedHeader(
                    text=h.text,
                    column_index=len(result),
                    x_min=h.x_min,
                    x_max=h.x_max,
                    tokens=h.tokens,
                    reconstruction_score=h.reconstruction_score,
                )
            )
            continue
        # Split: assign tokens to intervals by mid_x, emit one header per interval
        for idx in overlapping:
            a, b = data_column_intervals[idx]
            mid_iv = (a + b) / 2
            tokens_in = [t for t in h.tokens if a <= t.mid_x <= b]
            if not tokens_in:
                tokens_in = [t for t in h.tokens if abs(t.mid_x - mid_iv) == min(abs(t.mid_x - mid_iv) for t in h.tokens)][:1]
            if not tokens_in:
                continue
            text = " ".join(t.text for t in tokens_in).strip()
            x_min = min(t.x0 for t in tokens_in)
            x_max = max(t.x1 for t in tokens_in)
            result.append(
                ReconstructedHeader(
                    text=text or h.text,
                    column_index=len(result),
                    x_min=x_min,
                    x_max=x_max,
                    tokens=tokens_in,
                    reconstruction_score=h.reconstruction_score,
                )
            )
            split_ops.append({"split": h.text, "into": text, "interval_idx": idx})
    # Re-index
    for i, r in enumerate(result):
        result[i] = ReconstructedHeader(
            text=r.text,
            column_index=i,
            x_min=r.x_min,
            x_max=r.x_max,
            tokens=r.tokens,
            reconstruction_score=r.reconstruction_score,
        )
    return result, split_ops
