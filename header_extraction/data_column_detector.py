"""
Data column count detection from table body.
Uses X histogram / word gap clustering from first N data rows below header zone
to infer how many columns the data has. Used for Column Count Consistency Gate.
"""

import logging
from collections import Counter, defaultdict
from typing import List, Optional, Tuple

from .layout_reader import LayoutDocument
from .types import HeaderZone, WordBlock

logger = logging.getLogger(__name__)

# Max data rows to sample for column inference
DATA_ROW_SAMPLE = 20
# Words per row cap for column clustering
MAX_WORDS_PER_ROW_FOR_CLUSTER = 100


def _group_into_lines(
    words: List[WordBlock], y_tolerance: float = 4.0
) -> List[List[WordBlock]]:
    """Group words into horizontal lines by Y. Returns list of lines (each line = list of words)."""
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (w.y0, w.x0))
    lines: List[List[WordBlock]] = []
    current_line: List[WordBlock] = [sorted_words[0]]
    ref_y = sorted_words[0].y0
    for w in sorted_words[1:]:
        if abs(w.y0 - ref_y) <= y_tolerance:
            current_line.append(w)
        else:
            if current_line:
                current_line.sort(key=lambda x: x.x0)
                lines.append(current_line)
            current_line = [w]
            ref_y = w.y0
    if current_line:
        current_line.sort(key=lambda x: x.x0)
        lines.append(current_line)
    return lines


def _column_count_for_line(line_words: List[WordBlock]) -> int:
    """
    Infer number of columns in one row by X gap clustering.
    Consecutive words with gap > median_gap (or > 1.5 * median_gap) are in different columns.
    """
    if not line_words:
        return 0
    if len(line_words) == 1:
        return 1
    mids = [w.mid_x for w in line_words]
    gaps = [mids[i + 1] - mids[i] for i in range(len(mids) - 1)]
    if not gaps:
        return 1
    median_gap = sorted(gaps)[len(gaps) // 2]
    # Threshold: gap larger than this = new column. Use fraction of median to avoid merging columns.
    threshold = max(median_gap * 0.7, 3.0)
    clusters = 1
    for g in gaps:
        if g > threshold:
            clusters += 1
    return clusters


def detect_data_column_count(
    layout: LayoutDocument,
    zone: HeaderZone,
    page_no: int,
    max_data_rows: int = DATA_ROW_SAMPLE,
) -> Tuple[int, List[List[WordBlock]]]:
    """
    Infer data column count from first N data rows below header zone.
    Returns (column_count, list of lines used) where column_count is the mode
    of per-row column counts (most stable value).
    """
    words = layout.words_for_page(page_no)
    below = [w for w in words if w.y0 > zone.y_max]
    below.sort(key=lambda w: (w.y0, w.x0))
    # First 20 lines of data
    lines = _group_into_lines(below, y_tolerance=5.0)
    lines = lines[:max_data_rows]
    if not lines:
        return 0, []

    col_counts: List[Tuple[int, int]] = []  # (n_col, word_count) per line
    for line in lines:
        if len(line) < 2:
            continue
        line_text = " ".join(w.text.strip().lower() for w in line)
        if "page" in line_text and ("of" in line_text or "no." in line_text):
            continue
        n_col = _column_count_for_line(line)
        if 1 <= n_col <= 50:
            col_counts.append((n_col, len(line)))

    if not col_counts:
        return 0, []

    # Prefer data-dense rows (5+ words)
    dense = [(nc, wc) for nc, wc in col_counts if wc >= 5]
    use_list = dense if len(dense) >= 2 else col_counts
    counter = Counter(nc for nc, _ in use_list)
    if not counter:
        return 0, lines
    # Use mode; if multiple modes or near-tie, prefer column count that is "central" (median of top counts)
    most_common = counter.most_common(5)
    mode_count = most_common[0][1]
    # If 12 appears often and 10 is mode, prefer 11 or 12 when diff from header matters - we don't have header here.
    # Prefer largest column count that appears at least 25% of rows (stable)
    min_occurrences = max(2, len(use_list) // 4)
    stable = [nc for nc, cnt in counter.items() if cnt >= min_occurrences]
    data_column_count = max(stable) if stable else most_common[0][0]
    logger.debug(
        "Data column count page %d: %d from %d rows (distribution: %s)",
        page_no, data_column_count, len(use_list), dict(counter.most_common(5)),
    )
    return data_column_count, lines


def infer_column_intervals_from_data(
    layout: LayoutDocument,
    zone: HeaderZone,
    page_no: int,
    max_data_rows: int = DATA_ROW_SAMPLE,
) -> List[Tuple[float, float]]:
    """
    Infer column X intervals from data row word positions (histogram clustering).
    Useful for column explosion guard (splitting merged headers to match data columns).
    """
    data_col_count, lines = detect_data_column_count(
        layout, zone, page_no, max_data_rows
    )
    if data_col_count <= 0 or not lines:
        return []

    # Collect all mid_x from first N rows
    all_mid_x: List[float] = []
    for line in lines[:max_data_rows]:
        for w in line:
            all_mid_x.append(w.mid_x)
    all_mid_x.sort()

    if len(all_mid_x) < data_col_count:
        return []

    # Partition into data_col_count buckets (equal count or by gap)
    # Use k-means style: initial boundaries by splitting sorted list into k segments
    n = len(all_mid_x)
    step = n // data_col_count
    boundaries: List[float] = []
    for i in range(1, data_col_count):
        idx = i * step
        if idx < n:
            boundaries.append((all_mid_x[idx - 1] + all_mid_x[idx]) / 2.0)
    # Build intervals: (boundary[i], boundary[i+1]) with boundary[0]=min, boundary[-1]=max
    intervals: List[Tuple[float, float]] = []
    lo = min(all_mid_x) - 2.0
    for b in boundaries:
        intervals.append((lo, b))
        lo = b
    intervals.append((lo, max(all_mid_x) + 2.0))
    return intervals
