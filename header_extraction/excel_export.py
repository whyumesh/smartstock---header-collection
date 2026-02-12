"""
Excel export: one file with structure Option A (preferred).
Sheet "Headers": columns PDF_File_Name | Header_1 | Header_2 | ...
Scalable for 1000+ PDFs.
"""

import logging
from pathlib import Path
from typing import List, Optional

from .config import PipelineConfig
from .types import PDFExtractionResult

logger = logging.getLogger(__name__)

try:
    import openpyxl
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
except ImportError:
    Workbook = None  # type: ignore
    openpyxl = None  # type: ignore


def export_to_excel(
    results: List[PDFExtractionResult],
    output_path: Path,
    config: Optional[PipelineConfig] = None,
) -> None:
    """
    Write one Excel file.
    Sheet 'Headers': each row = one PDF, columns = PDF_File_Name, Header_1, Header_2, ...
    """
    if openpyxl is None or Workbook is None:
        raise ImportError("openpyxl is required. Install with: pip install openpyxl")

    wb = Workbook()
    ws = wb.active
    ws.title = "Headers"

    if not results:
        ws.append(["PDF_File_Name", "Confidence"])
        wb.save(output_path)
        return

    # Max number of headers across all PDFs
    max_headers = max(len(r.selected_headers) for r in results)
    header_row = ["PDF_File_Name", "Confidence"] + [
        f"Header_{i+1}" for i in range(max_headers)
    ]
    ws.append(header_row)

    for r in results:
        pdf_name = Path(r.pdf_path).name
        row = [pdf_name, round(r.confidence, 4)] + list(r.selected_headers)
        # Pad to same column count
        while len(row) < len(header_row):
            row.append("")
        ws.append(row)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    logger.info("Saved Excel to %s (%d rows)", output_path, len(results) + 1)
