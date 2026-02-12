# Header Extraction System — Technical Documentation

This document describes the **Header Extraction System**: an offline, high-precision pipeline for extracting **column headers** from PDF tables, with a focus on Indian pharma/FMCG stockist documents (stock statements, sales reports, order summaries). The system produces a single Excel file with one row per PDF and one column per detected header.

---

## Table of Contents

1. [Project Purpose and Scope](#1-project-purpose-and-scope)
2. [Design Philosophy](#2-design-philosophy)
3. [Pipeline Overview](#3-pipeline-overview)
4. [Module-by-Module Description](#4-module-by-module-description)
5. [Precision Validation Layer](#5-precision-validation-layer)
6. [Data Structures and Types](#6-data-structures-and-types)
7. [Configuration Reference](#7-configuration-reference)
8. [Debug Output](#8-debug-output)
9. [How to Run](#9-how-to-run)
10. [Limitations and Future Work](#10-limitations-and-future-work)

---

## 1. Project Purpose and Scope

### What It Does

- **Input**: A folder of PDF files (invoices, stock statements, sales reports, etc.).
- **Output**: One Excel workbook with:
  - **Sheet "Headers"**: Each row = one PDF; columns = `PDF_File_Name`, `Confidence`, `Header_1`, `Header_2`, … (extracted column headers in order).
- **Optional**: Per-PDF debug JSON files (when `--debug-dir` is set) for auditing and tuning.

### Goals

- **Precision-first**: Prefer rejecting a candidate over accepting a wrong header (auditor-level correctness).
- **Evidence-based**: A row is accepted as the table header only when there is sufficient evidence from layout, column count, data types, and domain keywords.
- **Domain-aware**: Built-in Indian pharma/FMCG terminology (Product Name, Batch No, MRP, PTR, PTS, Qty, Expiry, etc.).
- **Robust to variation**: Multi-page PDFs, multi-line headers, merged header cells, different templates, and noisy elements (titles, footers, GRAND TOTAL rows, watermarks).

### What It Does *Not* Do

- It does **not** extract cell values or table body data—only the header row(s) as column labels.
- It does **not** use any cloud/API; everything runs offline (PyMuPDF, openpyxl, rapidfuzz).

---

## 2. Design Philosophy

### Old vs New Logic

| Old approach | New approach |
|--------------|--------------|
| “Find a likely header row” | “Prove this is the header using table + column + data + structure evidence” |
| Accept if score is high enough | **Reject** if evidence &lt; threshold; only accept when evidence is sufficient |
| Optimize for recall | **Optimize for precision** |

### Core Principles

1. **Evidence threshold**: Every header candidate is scored on keyword match, column alignment, data-type consistency, lexical purity, and multi-page presence. If the combined evidence falls below a configurable threshold, the candidate is **rejected**.
2. **Hard gates**: Some rules are non-negotiable (e.g. header column count must approximately match data column count; see [Section 5](#5-precision-validation-layer)).
3. **Anti-noise**: Title text, footer text, “GRAND TOTAL” rows, report banners, and watermark-like tokens are explicitly detected and **excluded** from being treated as headers.
4. **When in doubt, reject**: If the system is unsure, it rejects the candidate and may output no headers for that PDF rather than guessing.

---

## 3. Pipeline Overview

The pipeline runs **per PDF**, and for **each page** of that PDF it:

1. Reads layout (words, positions, font sizes, drawing lines).
2. Detects a **header zone** (the band of text that contains the table header row).
3. Infers **data column count** from the first N data rows below that zone (X histogram / gap clustering).
4. **Reconstructs** header text per column (handles multi-line and compound headers).
5. Applies **column explosion guard** if reconstructed headers are fewer than data columns (splits merged headers using data column intervals).
6. **Filters** reconstructed headers (removes noise tokens, title/footer words, invalid shapes).
7. **Normalizes** header text to canonical domain terms (e.g. “Qty” → “Quantity”, “Batch No.” → “Batch No”).
8. **Validates** against column data (date/decimal/integer vs expected type per header).
9. Runs the **precision validation layer**: column-count gate, evidence score, penalties; **rejects** if the candidate fails.
10. Keeps only **non–footer-like** candidates (e.g. rejects rows that look like “GRAND”, “Tot”).
11. For multi-page PDFs, **builds consensus** (voting + confidence) and picks the best header set.

Finally, results are written to Excel (and optionally to debug JSON).

```
PDF → Layout → Header Zone Detection → Data Column Count → Reconstruction
    → Explosion Guard → Filter (noise/title) → Normalize → Data Validation
    → Precision Gate (evidence + penalties) → Footer-like Filter
    → Multi-Candidate / Consensus → Excel (+ Debug JSON)
```

---

## 4. Module-by-Module Description

### 4.1 `layout_reader.py`

- **Role**: Extract raw layout from the PDF using PyMuPDF (fitz).
- **Output**: A `LayoutDocument` containing:
  - **Words**: list of `WordBlock` (text, bbox `x0,y0,x1,y1`, font size, page number).
  - **Lines**: drawing lines (horizontal/vertical) from paths and rectangles (used for separator detection).
  - **Page dimensions**: width and height per page.
- **Details**:
  - Words come from `page.get_text("words")` with font size inferred from block dict where possible.
  - Lines come from `page.get_drawings()`; both line items (`"l"`) and rect items (`"re"`) are handled, with defensive parsing for different PyMuPDF rect formats to avoid crashes on some PDFs.

### 4.2 `table_header_detector.py`

- **Role**: Find the **single row** that is the table header (the line immediately above the first data row, or the strongest header-keyword line in the top part of the page).
- **Strategies** (in order):
  1. **Above first data row**: Find the first row that looks like “data” (enough numeric tokens), then among the lines **above** it, pick the one with the most header keywords. Skips page footers (“Page 1 of 1”), **GRAND TOTAL** rows, and **title/report lines** (e.g. “STOCK & SALES ANALYSIS”, “LTD.”, date ranges, single-letter banners).
  2. **Best in top 55%**: In the top half of the page, choose the line with the most header keywords and 4–20 tokens (again skipping footer/total/title lines).
  3. **First multi-token with keyword**: First line with ≥5 tokens and at least one header keyword (still excluding footer/total/title).
  4. **Fallback**: Line with largest font in the top 40%, excluding footer/title.
- **Helpers**:
  - `_is_footer_or_total_row()`: Rejects lines that are “GRAND TOTAL”, mostly numeric, or very short with “total”/“grand”.
  - `_is_title_or_report_line()`: Rejects document titles, company names (“LTD.”, “Pvt.”), date ranges, and single-letter spelling (e.g. “S T O C K S T A T E M E N T”).
- **Output**: A `HeaderZone` (y_min, y_max, page_no, confidence, reason, word_blocks for that row).

### 4.3 `header_zone_detector.py`

- **Role**: **Fallback** when the table-aware detector returns nothing. Finds a vertical band (y_min–y_max) that looks like a header based on text density, font size, and position (e.g. top of content, near a separator line).
- **Use case**: PDFs without a clear “first data row” or with unusual layout.

### 4.4 `data_column_detector.py`

- **Role**: Infer **how many columns** the table data has, from the first N rows below the header zone.
- **Method**:
  - Take words below the header zone, group them into lines by Y.
  - For each of the first ~20 data lines, compute a **column count** using X gap clustering (consecutive words with gap &gt; threshold count as a new column).
  - Use the **mode** (or a stable high count) of these per-line counts as `data_column_count`.
- **Used for**:
  - **Column count consistency gate**: Reject header candidates whose column count does not approximately match this number (see [Section 5](#5-precision-validation-layer)).
  - **Column explosion guard**: If reconstructed headers have fewer columns than `data_column_count`, split headers by aligning to data column X intervals.
- **Functions**:
  - `detect_data_column_count(layout, zone, page_no)` → `(data_column_count, lines_used)`.
  - `infer_column_intervals_from_data(...)` → list of (x_min, x_max) per data column (for splitting).

### 4.5 `header_reconstruction.py`

- **Role**: Turn the header-zone word blocks into an **ordered list of column headers** (one text per column).
- **Steps**:
  1. Group words into lines by Y proximity.
  2. Build **column intervals** (X bounds): for a single line, one interval per word; for multiple lines, overlap-based clustering.
  3. Assign each word to a column index by X overlap (or nearest interval).
  4. Per column: concatenate tokens (with space), compute x_min/x_max, and form a `ReconstructedHeader`.
  5. **Compound merge (strict)**: Only merge adjacent columns that form **known** compounds (e.g. Product+Name → “Product Name”, Cl+Qty → “Cl Qty”, Co+Op+Qty → “Co Op Qty”). No speculative merging.
  6. Return the list of `ReconstructedHeader` plus a **reconstruction score** and **merge_operations_performed** (for debug).
- **Column explosion guard** (function in same module): If `header_cols < data_cols`, split reconstructed headers so that each header aligns to one data column interval (X-based); returns **split_operations_performed** for debug.

### 4.6 `header_filter.py`

- **Role**: Remove reconstructed “headers” that are clearly not column headers.
- **Checks**:
  - **Patterns**: Page numbers, pure digits, “Page 1/5”, single digit/letter, overly long or too many tokens.
  - **Anti-noise dictionary**: Isolated tokens that are known non-headers (e.g. “Page”, “Report”, “Statement”, “GSTIN”, “Address”, “Phone”, “Email”, “Generated”, “Confidential”) are rejected—**unless** they are part of an allowed compound (e.g. “Invoice Date”, “Batch No”, “Stock Report” in the allow list).
  - **Header word shape**: Rejects full-sentence fragments, date strings, page-numbering patterns, and long company-name-like text.
- **Output**: Filtered list of `ReconstructedHeader` and a list of **noise_tokens_removed** (for debug and penalties).

### 4.7 `semantic_normalizer.py`

- **Role**: Map raw header strings to **canonical domain terms** (Indian pharma/FMCG).
- **Logic**: Exact match in domain dictionary first; then partial/contains match; short tokens (In, Op, Cl, etc.) kept as-is; then fuzzy match (e.g. rapidfuzz) against domain keys; otherwise title-case cleanup.
- **Output**: List of `NormalizedHeader` (original, normalized, semantic_score, source) and an average semantic score.

### 4.8 `data_validation.py`

- **Role**: For each column, sample cell values **below** the header zone and infer data type (date, decimal, integer, alphanumeric, text). Compare with the **expected** type for the normalized header (e.g. “Expiry Date” → date, “MRP” → decimal, “Quantity” → integer).
- **Output**:
  - Per-column **profiles** (inferred type, match ratio, validation score).
  - **Overall data validation score** (0–1).
  - **data_type_mismatch_count**: number of columns where expected ≠ inferred (used for precision penalties).
- **Domain boost**: Headers that appear in the domain priority list (Product, Batch, MRP, Qty, etc.) get a small score boost.

### 4.9 `precision_validation.py`

- **Role**: Implement the **evidence-based** acceptance/rejection and confidence adjustments.
- **Main function**: `validate_candidate_with_evidence(...)` returns:
  - `is_valid`: whether the candidate passes all gates and evidence threshold.
  - `reject_reason`: empty if valid, otherwise a short reason string.
  - `adjusted_confidence`: base confidence minus penalties (merged column, noise words, data-type mismatch, title zone).
  - `debug`: dict with gate results, evidence scores, penalty breakdown.
- **Gates and scoring** are described in [Section 5](#5-precision-validation-layer).

### 4.10 `consensus.py`

- **Role**: When a PDF has **multiple pages**, there is one header candidate per page. Consensus:
  - Sorts candidates by **combined confidence** (and then by **earlier page** on tie, to avoid picking a footer row from a later page).
  - Takes the **best** candidate as the primary; if all candidates have the **same column count**, also does **per-column voting** (confidence-weighted) to form a single voted header list.
- **Output**: One `HeaderCandidate` (headers, scores, source_pages, debug).

### 4.11 `pipeline.py`

- **Role**: Orchestrates the full flow.
  - For each PDF: load layout; for each page run zone detection → data column count → reconstruction → explosion guard → filter (with noise tracking) → normalize → validate (with data_type_mismatch_count) → build candidate → **precision gate** → **footer-like filter** → add to per-page candidates.
  - If there are no candidates (all rejected), returns a result with empty headers and debug (candidate_header_rows, rejected_candidates_reason).
  - If there are candidates: **consensus** (if multi-page) or single best; then build the final result with **mandatory debug fields** (candidate_header_rows, rejected_candidates_reason, final_header_columns, data_column_count_detected, merge_operations_performed, split_operations_performed, noise_tokens_removed).
- **Footer-like filter**: Rejects any candidate whose headers look like a footer/total row (e.g. (“grand”, “tot”), or ≤2 columns with “total”/“grand”).

### 4.12 `excel_export.py`

- **Role**: Write one Excel file. Sheet “Headers”: header row = `PDF_File_Name`, `Confidence`, `Header_1`, `Header_2`, …; one data row per PDF with `Path(pdf).name`, confidence value, and the list of selected headers (padded with empty cells to match max column count).

### 4.13 `config.py`

- **Role**: Central configuration (paths, thresholds, weights, precision parameters, data column sample size, etc.). See [Section 7](#7-configuration-reference).

### 4.14 `types.py`

- **Role**: Dataclasses for `WordBlock`, `LineBlock`, `HeaderZone`, `ReconstructedHeader`, `NormalizedHeader`, `ColumnDataProfile`, `HeaderCandidate`, `PDFExtractionResult`. See [Section 6](#6-data-structures-and-types).

---

## 5. Precision Validation Layer

This section details the **evidence-based** rules that decide whether a header candidate is accepted or rejected.

### 5.1 Minimum Column Count (Hard)

- **Rule**: A valid table header must have **at least 3 columns**.
- **Reject**: If `len(headers) < 3` (e.g. “GRAND”, “Tot” or single-column noise).

### 5.2 Column Count Consistency Gate (Hard with Exception)

- **Rule**: `header_column_count` must approximately match `data_column_count` (from the data column detector).
- **Strict**: If `|header_cols - data_cols| ≤ max_header_data_column_diff` (default 1), the candidate **passes** the gate.
- **Exception**: If **header_cols ≥ data_cols** and:
  - `header_cols - data_cols ≤ 8`, and  
  - `5 ≤ header_cols ≤ 25`,  
  then the candidate **passes** (to handle merged data cells or undercounted data columns).
- **Reject**: Otherwise (e.g. header_cols = 2 and data_cols = 10, or header_cols = 15 and data_cols = 5 with no exception).

### 5.3 Evidence Score

Evidence is a weighted sum of:

- **keyword_score**: Fraction of header tokens that match known header keywords (stock/sales domain).
- **column_alignment_score**: 1.0 if header count matches data count exactly; lower if within allowed diff.
- **data_validation_score**: From the data validation module (column type match).
- **lexical_purity_score**: High when few noise tokens were removed and domain headers are present.
- **multi_page_presence_score**: Fraction of pages on which this header set appears (for multi-page PDFs).

**Threshold**: If `evidence < precision_evidence_threshold` (default 0.35), the candidate is **rejected**. (Threshold is applied to this raw evidence; penalties are **not** subtracted from evidence for the pass/fail check.)

### 5.4 Penalties (Adjust Confidence Only)

The following **reduce** the reported confidence but do **not** by themselves reject the candidate:

- **merged_column_penalty**: Applied when header column count ≠ data column count (within the allowed gate).
- **noise_word_penalty**: Per token removed as noise.
- **data_type_mismatch_penalty**: Per column where inferred data type does not match the expected type for the header.
- **title_zone_penalty**: Per token that falls in a title zone (if used).

These are summed (capped) and subtracted from the base confidence to produce **adjusted_confidence**.

### 5.5 Footer-like Candidate Filter (Pipeline)

- After the precision gate, the pipeline checks **footer-like** candidates.
- **Reject** if the header list is one of the known footer patterns (e.g. (“grand”, “tot”), (“grand”, “total”), (“total”,), (“grand”,)) or has ≤2 columns and contains “total”/“grand”.
- This avoids outputting “GRAND Tot” (or similar) as the header when a later page’s total row was wrongly considered.

---

## 6. Data Structures and Types

- **WordBlock**: text, x0, y0, x1, y1, font_size, page_no; properties `mid_x`, `mid_y`, `width`, `height`.
- **LineBlock**: x0, y0, x1, y1, page_no; `is_horizontal`, `y`.
- **HeaderZone**: y_min, y_max, page_no, confidence, reason, word_blocks.
- **ReconstructedHeader**: text, column_index, x_min, x_max, tokens (WordBlock list), reconstruction_score.
- **NormalizedHeader**: original, normalized, semantic_score, source (“exact”|“fuzzy”|“domain”).
- **ColumnDataProfile**: column_index, sample_values, inferred_type, match_ratio, validation_score.
- **HeaderCandidate**: headers (list of strings), visual_score, reconstruction_score, semantic_score, data_validation_score, consensus_score, combined_confidence, source_pages, debug.
- **PDFExtractionResult**: pdf_path, selected_headers, confidence, confidence_breakdown, header_zone_detected, raw_header_candidates, final_selected_header, debug.

---

## 7. Configuration Reference

Relevant options in `PipelineConfig`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `input_folder` | Path() | Folder containing PDFs. |
| `output_excel_path` | headers_output.xlsx | Output Excel file. |
| `debug_output_dir` | None | If set, per-PDF debug JSON is written here. |
| `max_header_data_column_diff` | 1 | Allowed ± difference for strict column count match. |
| `precision_evidence_threshold` | 0.35 | Reject if evidence below this. |
| `merged_column_penalty` | 0.15 | Confidence penalty when header_cols ≠ data_cols. |
| `noise_word_penalty` | 0.15 | Per noise token removed. |
| `data_type_mismatch_penalty` | 0.20 | Per column type mismatch. |
| `title_zone_penalty` | 0.30 | Per token in title zone. |
| `domain_header_boost` | 0.08 | Score boost for known domain headers. |
| `compound_merge_strict` | True | Only merge known compound pairs. |
| `data_row_sample_for_columns` | 20 | Number of data rows used to infer column count. |
| `sample_cells_per_column` | 50 | Cells sampled per column for data validation. |
| `weight_visual` … `weight_consensus` | 0.25, 0.25, 0.20, 0.20, 0.10 | Weights for combined confidence. |
| `max_pdfs` | None | Limit number of PDFs processed (None = no limit). |

---

## 8. Debug Output

When `--debug-dir` is set, each PDF gets a JSON file named `{stem}_debug.json`. It includes:

| Field | Description |
|-------|-------------|
| `pdf_path` | Path to the PDF. |
| `header_zone_detected` | Per-page zone (page_no, y_min, y_max, confidence, reason). |
| `raw_header_candidates` | Per-page accepted candidates (headers, scores, data_column_count_detected). |
| `final_selected_header` | Final list of headers chosen (same as selected_headers). |
| `confidence` | Final confidence. |
| `confidence_breakdown` | visual, reconstruction, semantic, data_validation, consensus. |
| `debug` | Extended debug, including: |
| `debug.candidate_header_rows` | All candidate rows (page_no, headers, data_column_count_detected, combined_confidence). |
| `debug.rejected_candidates_reason` | Per-page rejection reasons (page_no, reason, evidence_debug or headers). |
| `debug.final_header_columns` | Same as final_selected_header. |
| `debug.data_column_count_detected` | Data column count used for the chosen candidate. |
| `debug.merge_operations_performed` | List of { merged, into } from reconstruction. |
| `debug.split_operations_performed` | List of { split, into, interval_idx } from explosion guard. |
| `debug.noise_tokens_removed` | Tokens removed by the anti-noise filter. |
| `debug.precision_evidence` | When present: column_count_gate, penalty_breakdown, evidence_scores, evidence_total. |
| `debug.reason` | If no candidates: e.g. "no_header_candidates". |

---

## 9. How to Run

### Prerequisites

```bash
pip install -r requirements.txt
```

Typical dependencies: PyMuPDF (fitz), openpyxl, rapidfuzz.

### CLI

```bash
# Process all PDFs in a folder; output to default Excel
python run_extraction.py pdfs --output headers_output.xlsx

# With debug JSON and optional limit
python run_extraction.py pdfs --output headers_output.xlsx --debug-dir debug_out --max-pdfs 100

# Sample folder
python run_extraction.py pdfsamples --output headers_pdfsamples.xlsx --debug-dir debug_pdfsamples

# Help
python run_extraction.py --help
```

### Python API

```python
from pathlib import Path
from header_extraction import HeaderExtractionPipeline
from header_extraction.config import PipelineConfig

config = PipelineConfig(
    input_folder=Path("pdfs"),
    output_excel_path=Path("headers_output.xlsx"),
    debug_output_dir=Path("debug_out"),  # optional
    max_pdfs=100,  # optional
)
pipeline = HeaderExtractionPipeline(config=config)
results = pipeline.process_folder(Path("pdfs"))

for r in results:
    print(r.pdf_path, r.confidence, r.selected_headers)
```

### Output

- **Excel**: One sheet “Headers”; columns `PDF_File_Name`, `Confidence`, `Header_1`, `Header_2`, …; one row per PDF.
- **Debug**: One JSON per PDF in the debug dir (when set), with the structure described in [Section 8](#8-debug-output).

---

## 10. Limitations and Future Work

- **Reconstruction duplicates**: Some layouts (e.g. multi-line or complex grids) can produce duplicate column labels (e.g. “Product Name” repeated). The pipeline may still accept them to avoid empty output; a post-processing deduplication step could be added.
- **Data column count**: Inferred from the first N data rows by X gaps. Very irregular or sparse tables may get undercounted or overcounted; the “header ≥ data” exception (Section 5.2) partly compensates.
- **Language**: Tuned for English and Indian English domain terms; other languages would need additional synonym sets and possibly different noise lists.
- **Very large PDFs**: Processing is per-PDF and per-page; for thousands of pages, runtime may be significant; consider `max_pdfs` or batching.
- **Future hooks**: The codebase includes placeholders for template memory, stockist-specific learning, and cross-PDF consensus learning (e.g. in `future_hooks.py`).

---

*This documentation describes the Header Extraction System as implemented for the smartstock header collection project, including the precision correction layer and evidence-based validation.*
