"""
Table-aware header row detection for stock/sales PDFs.
Finds the SINGLE ROW that is the table header (line immediately above first data row),
or the line with strongest header-keyword signal in the top half of the page.
"""

import re
import logging
from collections import defaultdict
from typing import List, Optional, Set, Tuple

from .config import PipelineConfig
from .layout_reader import LayoutDocument
from .types import HeaderZone, WordBlock

logger = logging.getLogger(__name__)

# Tokens that strongly indicate a column header (stock/sales pharma/FMCG)
HEADER_KEYWORDS: Set[str] = {
    "product", "name", "pack", "unit", "qty", "qty.", "quantity",
    "open", "close", "op", "cl", "sales", "sale", "purchase", "pur", "recv", "recd",
    "in", "out", "inqty", "outqty", "clqty", "clval", "val", "amount",
    "batch", "exp", "expiry", "mrp", "ptr", "pts", "rate", "order", "pend",
    "lstsl", "lstmove", "stk", "value", "stock", "gd", "gdin", "tot",
    "exp", "out", "near", "co", "op", "unit",
}


def _is_numeric_token(s: str) -> bool:
    t = s.strip().replace(",", "")
    if re.match(r"^-?\d+\.?\d*$", t):
        return True
    if re.match(r"^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$", t):
        return True
    return False


def _header_keyword_count(words: List[WordBlock]) -> int:
    count = 0
    for w in words:
        t = w.text.strip().lower()
        if t in HEADER_KEYWORDS:
            count += 1
        else:
            for kw in HEADER_KEYWORDS:
                if kw in t or t in kw:
                    count += 1
                    break
    return count


def _is_footer_or_total_row(line_words: List[WordBlock]) -> bool:
    """True if line looks like GRAND TOTAL, subtotal, or mostly numeric (footer/summary row)."""
    if not line_words:
        return True
    line_text_lower = " ".join(w.text.strip().lower() for w in line_words)
    # GRAND TOTAL / Total row
    if "grand" in line_text_lower and "total" in line_text_lower:
        return True
    if line_text_lower.strip() in ("total", "grand total", "sub total", "subtotal"):
        return True
    # Mostly numeric => data/total row, not header
    numeric = sum(1 for w in line_words if _is_numeric_token(w.text))
    if len(line_words) >= 3 and numeric / len(line_words) >= 0.5:
        return True
    # Very few tokens and one is total/grand
    if len(line_words) <= 3:
        tokens = [w.text.strip().lower() for w in line_words]
        if "total" in tokens or "grand" in tokens:
            return True
    return False


def _is_title_or_report_line(line_words: List[WordBlock]) -> bool:
    """True if line looks like document/report title (not table header)."""
    if not line_words or len(line_words) < 4:
        return False
    line_text = " ".join(w.text.strip() for w in line_words)
    line_text_lower = line_text.lower()
    # Report title patterns
    if "analysis" in line_text_lower and ("stock" in line_text_lower or "sales" in line_text_lower):
        return True
    if "ltd." in line_text_lower or "limited" in line_text_lower or "pvt." in line_text_lower:
        return True
    # Date range (e.g. 01-11-2025 - 20-11-2025)
    if re.search(r"\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}\s*[-–]\s*\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}", line_text):
        return True
    # Single letters spelling a word (e.g. S T O C K S T A T E M E N T)
    if len(line_words) >= 8 and all(len(w.text.strip()) == 1 for w in line_words):
        return True
    return False


def _group_into_lines(words: List[WordBlock], y_tolerance: float = 3.0) -> List[Tuple[float, List[WordBlock]]]:
    """Group words by y (same line). Returns list of (y_center, words) sorted by y."""
    by_y: dict = defaultdict(list)
    for w in words:
        by_y[int(w.y0)].append(w)
    lines = []
    for y_key in sorted(by_y.keys()):
        line_words = by_y[y_key]
        line_words.sort(key=lambda w: w.x0)
        y_center = sum(w.mid_y for w in line_words) / len(line_words)
        lines.append((y_center, line_words))
    return lines


class TableHeaderDetector:
    """
    Detects the single table header ROW by:
    1) Finding the first data row (many numeric tokens) and taking the line above it, OR
    2) Picking the line in the top half with the most header keywords and 4–20 tokens.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def detect(self, layout: LayoutDocument, page_no: int = 0) -> Optional[HeaderZone]:
        words = layout.words_for_page(page_no)
        if not words:
            return None

        page_h = layout.page_heights[page_no] if page_no < len(layout.page_heights) else 842
        footer_y = page_h * 0.85
        words = [w for w in words if w.y0 < footer_y]
        if not words:
            return None

        median_font = self._median_font_size(words)
        if median_font > 0:
            words = [
                w for w in words
                if 0.4 * median_font <= (w.font_size or median_font) <= 2.5 * median_font
            ]
        if not words:
            return None

        lines = _group_into_lines(words)
        if not lines:
            return None

        # Strategy 1: Find first "data" row (row with several numbers); skip Page X of Y, GRAND TOTAL, etc.
        first_data_line_idx: Optional[int] = None
        for i, (y_center, line_words) in enumerate(lines):
            if len(line_words) < 4:
                continue
            # Skip footer/total rows (GRAND TOTAL + numbers)
            if _is_footer_or_total_row(line_words):
                continue
            # Skip lines that look like page footer (Page 1 of 1, Page No. 2)
            line_text_lower = " ".join(w.text.lower() for w in line_words)
            if "page" in line_text_lower and ("of" in line_text_lower or "no." in line_text_lower):
                continue
            numeric = sum(1 for w in line_words if _is_numeric_token(w.text))
            ratio = numeric / len(line_words)
            # Require at least 4 numeric tokens so "Page 1 of 1" (2 numeric) is skipped
            if ratio >= 0.35 and len(line_words) >= 4 and numeric >= 4:
                first_data_line_idx = i
                break

        if first_data_line_idx is not None and first_data_line_idx > 0:
            # Among lines above first data row, pick the one with most header keywords (real header row)
            best_kw = -1
            best_header_words: Optional[List[WordBlock]] = None
            for i in range(first_data_line_idx - 1, -1, -1):
                _, line_words = lines[i]
                if len(line_words) < 2:
                    continue
                if _is_footer_or_total_row(line_words) or _is_title_or_report_line(line_words):
                    continue
                kw = _header_keyword_count(line_words)
                if kw > best_kw:
                    best_kw = kw
                    best_header_words = line_words
            if best_header_words and best_kw >= 1:
                return self._zone_from_line(
                    best_header_words, page_no, 0.92,
                    "above_first_data_row_keywords=%d" % best_kw,
                )
            # Fallback: line immediately above first data row (if not footer)
            _, header_words = lines[first_data_line_idx - 1]
            if len(header_words) >= 2 and not _is_footer_or_total_row(header_words) and not _is_title_or_report_line(header_words):
                return self._zone_from_line(header_words, page_no, 0.75, "above_first_data_row")

        # Strategy 2: Line with most header keywords in top 55% of content
        y_min_page = min(w.y0 for w in words)
        y_max_page = max(w.y1 for w in words)
        content_span = y_max_page - y_min_page
        top_cut = y_min_page + content_span * 0.55 if content_span > 0 else page_h * 0.5

        best_score = -1
        best_line_words: Optional[List[WordBlock]] = None
        best_reason = ""
        for y_center, line_words in lines:
            if y_center > top_cut:
                continue
            if _is_footer_or_total_row(line_words) or _is_title_or_report_line(line_words):
                continue
            n = len(line_words)
            if n < 3 or n > 25:
                continue
            kw = _header_keyword_count(line_words)
            # Prefer 4–15 tokens (typical header column count)
            token_bonus = 1.0 if 4 <= n <= 15 else 0.7
            score = kw * token_bonus
            if score > best_score:
                best_score = score
                best_line_words = line_words
                best_reason = f"header_keywords={kw} tokens={n}"

        if best_line_words and best_score >= 1:
            return self._zone_from_line(
                best_line_words, page_no,
                min(0.9, 0.5 + best_score * 0.1),
                best_reason,
            )

        # Strategy 3: First line with 5+ tokens that has at least one header keyword (skip footer/total/title)
        for y_center, line_words in lines:
            if y_center > top_cut:
                continue
            if _is_footer_or_total_row(line_words) or _is_title_or_report_line(line_words):
                continue
            if len(line_words) >= 5 and _header_keyword_count(line_words) >= 1:
                return self._zone_from_line(line_words, page_no, 0.6, "first_multi_token_with_keyword")

        # Fallback: line with largest font in top 40% (header often bold); skip footer/total
        top_cut2 = y_min_page + content_span * 0.4 if content_span > 0 else page_h * 0.4
        best_font = 0
        best_line_words = None
        for y_center, line_words in lines:
            if y_center > top_cut2:
                continue
            if _is_footer_or_total_row(line_words) or _is_title_or_report_line(line_words):
                continue
            if len(line_words) < 3:
                continue
            avg_font = sum(w.font_size or 0 for w in line_words) / len(line_words)
            if avg_font > best_font:
                best_font = avg_font
                best_line_words = line_words
        if best_line_words:
            return self._zone_from_line(best_line_words, page_no, 0.5, "largest_font_top")

        return None

    def _zone_from_line(
        self,
        line_words: List[WordBlock],
        page_no: int,
        confidence: float,
        reason: str,
    ) -> HeaderZone:
        y_min = min(w.y0 for w in line_words)
        y_max = max(w.y1 for w in line_words)
        zone = HeaderZone(
            y_min=y_min,
            y_max=y_max,
            page_no=page_no,
            confidence=confidence,
            reason=reason,
            word_blocks=line_words,
        )
        logger.info(
            "Header row page %d: y=%.1f-%.1f confidence=%.2f reason=%s tokens=%s",
            page_no, y_min, y_max, confidence, reason,
            [w.text for w in line_words][:12],
        )
        return zone

    def _median_font_size(self, words: List[WordBlock]) -> float:
        sizes = [w.font_size for w in words if w.font_size and w.font_size > 0]
        if not sizes:
            return 0.0
        sizes.sort()
        return sizes[len(sizes) // 2]
