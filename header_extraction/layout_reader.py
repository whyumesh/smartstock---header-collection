"""
Module 1: PDF Layout Reader.
Uses PyMuPDF for word extraction, coordinates, font size, line blocks, and drawing lines.
Fully offline.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None  # type: ignore

from .types import WordBlock, LineBlock

logger = logging.getLogger(__name__)


class PDFLayoutReader:
    """
    Extracts layout elements from PDFs: words with coordinates and font size,
    line blocks, and table drawing lines.
    """

    def __init__(self) -> None:
        if fitz is None:
            raise ImportError("PyMuPDF (fitz) is required. Install with: pip install PyMuPDF")

    def read(self, pdf_path: Path) -> "LayoutDocument":
        """Load PDF and extract all layout elements."""
        doc = fitz.open(pdf_path)
        try:
            words_all: List[WordBlock] = []
            lines_all: List[LineBlock] = []
            page_heights: List[float] = []
            page_widths: List[float] = []

            for page_no in range(len(doc)):
                page = doc[page_no]
                rect = page.rect
                page_w = rect.width
                page_h = rect.height
                page_widths.append(page_w)
                page_heights.append(page_h)

                # Word-level extraction (blocks with bbox and font size when available)
                words_page = self._extract_words(page, page_no)
                words_all.extend(words_page)

                # Drawing lines (paths, rects that look like table lines)
                lines_page = self._extract_lines(page, page_no)
                lines_all.extend(lines_page)

            return LayoutDocument(
                words=words_all,
                lines=lines_all,
                page_heights=page_heights,
                page_widths=page_widths,
                num_pages=len(doc),
            )
        finally:
            doc.close()

    def _extract_words(self, page: "fitz.Page", page_no: int) -> List[WordBlock]:
        """Extract words with bbox and font size from page. Uses get_text("words") and blocks for font."""
        words: List[WordBlock] = []
        # get_text("words") returns: (x0, y0, x1, y1, "word", block_no, line_no, word_no)
        raw_words = page.get_text("words", sort=True)
        # get_text("dict") gives blocks with lines and spans (font size in span)
        try:
            blocks_dict = page.get_text("dict")
        except Exception:
            blocks_dict = {"blocks": []}

        # Build y -> font size from block dict (line bbox y0)
        y_to_font: dict = {}
        for block in blocks_dict.get("blocks", []):
            for line in block.get("lines", []):
                y0_line = line.get("bbox", (0, 0, 0, 0))[1]
                for span in line.get("spans", []):
                    y_to_font[int(y0_line)] = span.get("size", 0)

        for item in raw_words:
            if len(item) < 5:
                continue
            x0, y0, x1, y1 = item[0], item[1], item[2], item[3]
            text = item[4].strip()
            if not text:
                continue
            fs = y_to_font.get(int(y0), 0.0)
            words.append(
                WordBlock(
                    text=text,
                    x0=x0,
                    y0=y0,
                    x1=x1,
                    y1=y1,
                    font_size=float(fs),
                    page_no=page_no,
                )
            )
        return words

    def _extract_lines(self, page: "fitz.Page", page_no: int) -> List[LineBlock]:
        """Extract horizontal/vertical lines from drawings (paths, rects)."""
        lines: List[LineBlock] = []
        try:
            paths = page.get_drawings()
        except Exception:
            paths = []
        for path in paths:
            items = path.get("items", [])
            for item in items:
                if item[0] == "l":  # line
                    _, p1, p2 = item
                    x0, y0 = p1[0], p1[1]
                    x1, y1 = p2[0], p2[1]
                    lines.append(
                        LineBlock(x0=x0, y0=y0, x1=x1, y1=y1, page_no=page_no)
                    )
                elif item[0] == "re":  # rect
                    try:
                        # PyMuPDF: item can be ("re", rect) with rect as 4-tuple, or ("re", x0, y0, w, h)
                        rest = item[1:]
                        if len(rest) == 1:
                            rect = rest[0]
                        else:
                            rect = rest[:4]
                        if len(rect) >= 4:
                            x0, y0 = float(rect[0]), float(rect[1])
                            r2, r3 = float(rect[2]), float(rect[3])
                            if r2 < x0 or r3 < y0:  # w, h (can be negative in some coords)
                                x1, y1 = x0 + r2, y0 + r3
                            else:
                                x1, y1 = r2, r3
                            w, h = x1 - x0, y1 - y0
                            if abs(w) > abs(h):
                                lines.append(LineBlock(x0=x0, y0=y1, x1=x1, y1=y1, page_no=page_no))
                            else:
                                lines.append(LineBlock(x0=x0, y0=y0, x1=x1, y1=y1, page_no=page_no))
                    except (IndexError, TypeError, ValueError):
                        pass

        # Also check for vector lines in page content
        try:
            for xref in page.get_xobjects():
                # Skip image xobjects
                pass
        except Exception:
            pass
        return lines


@dataclass
class LayoutDocument:
    """Container for extracted layout from one PDF."""

    words: List[WordBlock]
    lines: List[LineBlock]
    page_heights: List[float]
    page_widths: List[float]
    num_pages: int

    def words_for_page(self, page_no: int) -> List[WordBlock]:
        return [w for w in self.words if w.page_no == page_no]

    def lines_for_page(self, page_no: int) -> List[LineBlock]:
        return [l for l in self.lines if l.page_no == page_no]

    def horizontal_lines_for_page(self, page_no: int) -> List[LineBlock]:
        return [l for l in self.lines_for_page(page_no) if l.is_horizontal]
