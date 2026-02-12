"""
Module 2: Header Zone Detection (Visual Intelligence).
Detects likely header band using:
- Highest text density horizontal band
- Larger font size cluster
- Text above first dense numeric rows
- Horizontal separator line proximity
- Alignment consistency
Output: header_zone_y_min, header_zone_y_max, confidence_score.
"""

import logging
from collections import defaultdict
from typing import List, Optional, Tuple

from .config import PipelineConfig
from .layout_reader import LayoutDocument
from .types import HeaderZone, WordBlock

logger = logging.getLogger(__name__)


class HeaderZoneDetector:
    """
    Detects the vertical band (y_min, y_max) that most likely contains column headers.
    Explainable: logs reason for choice.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def detect(self, layout: LayoutDocument, page_no: int = 0) -> Optional[HeaderZone]:
        """
        Detect header zone for one page.
        Returns HeaderZone with y_min, y_max, confidence, and reason.
        """
        words = layout.words_for_page(page_no)
        if not words:
            logger.debug("Page %d: no words", page_no)
            return None

        page_h = layout.page_heights[page_no] if page_no < len(layout.page_heights) else 600
        max_band_height = page_h * self.config.header_band_max_height_ratio

        # Ignore watermark/logo (extreme font sizes) and footer (bottom 15% of page)
        footer_y_min = page_h * 0.85
        words = [w for w in words if w.y0 < footer_y_min]
        if not words:
            return None
        median_font = self._median_font_size(words)
        if median_font > 0:
            # Drop very large (logo) or very small (watermark) outliers
            words = [
                w for w in words
                if 0.3 * median_font <= (w.font_size or median_font) <= 3.0 * median_font
            ]
        if not words:
            return None

        # 1) Slice page into horizontal bands (e.g. 20px height)
        band_height = 15.0
        band_scores: List[Tuple[float, float, float, str]] = []  # (y_center, score, density, reason)

        y_min_page = min(w.y0 for w in words)
        y_max_page = max(w.y1 for w in words)
        if median_font <= 0:
            median_font = 10.0

        # 2) Score each band: text density + font size boost + position (prefer top)
        y = y_min_page
        while y < min(y_max_page + band_height, page_h):
            y_end = y + band_height
            band_words = [w for w in words if w.y0 < y_end and w.y1 > y]
            if len(band_words) >= self.config.min_words_in_header_band:
                density = len(band_words) / (band_height / 20.0)  # normalize
                avg_font = sum(w.font_size or median_font for w in band_words) / len(band_words)
                font_boost = 1.0
                if median_font > 0 and avg_font >= median_font * self.config.font_size_boost_threshold:
                    font_boost = 1.5
                # Prefer bands near top (headers are usually in first 20% of content)
                content_span = y_max_page - y_min_page
                top_penalty = (y - y_min_page) / content_span if content_span > 0 else 0
                position_score = 1.0 - top_penalty * 0.5  # 0.5 to 1.0
                score = density * font_boost * position_score
                reason = "density=%.1f font_boost=%.1f top_score=%.2f" % (
                    density, font_boost, position_score
                )
                band_scores.append((y + band_height / 2, score, density, reason))
            y += band_height / 2  # Overlap bands slightly

        if not band_scores:
            # Fallback: take top N lines of text as header zone
            return self._fallback_zone(words, page_no, page_h)

        # 3) Pick best band; do NOT expand too much - keep header to first few lines only
        best = max(band_scores, key=lambda x: x[1])
        y_center, score, _, reason = best
        y_min = max(0, y_center - max_band_height / 2)
        y_max = min(page_h, y_center + max_band_height / 2)

        # 4) Include words in band; cap by distinct lines (max 4 lines) to avoid pulling in data
        header_words = [
            w for w in words
            if w.mid_y >= y_min - 10 and w.mid_y <= y_max + self.config.y_line_merge_threshold_px
        ]
        if header_words:
            distinct_ys = sorted(set(int(w.y0) for w in header_words))
            if len(distinct_ys) > 5:
                # Keep only top 5 lines (multi-line headers)
                y_cut = distinct_ys[4] + 15
                header_words = [w for w in header_words if w.y0 <= y_cut]
            if header_words:
                y_min = min(w.y0 for w in header_words)
                y_max = max(w.y1 for w in header_words)

        # 5) Separator line just below header boosts confidence
        h_lines = layout.horizontal_lines_for_page(page_no)
        for line in h_lines:
            if y_min <= line.y <= y_max + self.config.separator_line_proximity_px:
                score *= 1.1
                reason += "; separator_below"
                break

        confidence = min(1.0, score / 10.0)  # Normalize to 0-1
        zone = HeaderZone(
            y_min=y_min,
            y_max=y_max,
            page_no=page_no,
            confidence=confidence,
            reason=reason,
            word_blocks=header_words,
        )
        logger.info(
            "Header zone page %d: y=%.1f-%.1f confidence=%.2f reason=%s",
            page_no, y_min, y_max, confidence, reason[:80],
        )
        return zone

    def _median_font_size(self, words: List[WordBlock]) -> float:
        sizes = [w.font_size for w in words if w.font_size and w.font_size > 0]
        if not sizes:
            return 0.0
        sizes.sort()
        return sizes[len(sizes) // 2]

    def _fallback_zone(
        self, words: List[WordBlock], page_no: int, page_h: float
    ) -> Optional[HeaderZone]:
        """Fallback: treat first few lines (by y) as header."""
        by_y = defaultdict(list)
        for w in words:
            by_y[int(w.y0)].append(w)
        sorted_ys = sorted(by_y.keys())
        if not sorted_ys:
            return None
        # Take first 3–5 lines
        take = min(5, len(sorted_ys))
        first_ys = sorted_ys[:take]
        all_words = []
        for y in first_ys:
            all_words.extend(by_y[y])
        if not all_words:
            return None
        y_min = min(w.y0 for w in all_words)
        y_max = max(w.y1 for w in all_words)
        return HeaderZone(
            y_min=y_min,
            y_max=y_max,
            page_no=page_no,
            confidence=0.4,
            reason="fallback_first_lines",
            word_blocks=all_words,
        )
