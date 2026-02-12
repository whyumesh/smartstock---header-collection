# Header Extraction System

Offline, high-precision **document intelligence** for extracting **column headers** from PDFs (pharma/FMCG stockist documents). Produces one Excel file with one row per PDF.

## Features

- **Fully offline** — No API or cloud; uses PyMuPDF, openpyxl, rapidfuzz
- **Pipeline**: Layout → Header zone → Reconstruction → Semantic normalization → Data validation → Confidence scoring → Excel
- **Handles**: Multi-line headers, merged cells, broken OCR, misalignment, layout shifts, multiple pages
- **Domain**: Built-in Indian pharma/FMCG header synonyms (Product Name, Batch No, Expiry, MRP, PTR, GST, etc.)
- **Explainable**: Logs why headers were chosen; optional per-PDF debug JSON
- **Extensible**: Hooks for template memory, stockist learning, cross-PDF consensus

## Install

```bash
pip install -r requirements.txt
```

## Usage

**CLI (recommended)**

```bash
# Process all PDFs in a folder
python run_extraction.py pdfs --output headers_output.xlsx

# With debug JSON and limit
python run_extraction.py pdfs --output headers_output.xlsx --debug-dir debug_out --max-pdfs 100

# Options
python run_extraction.py --help
```

**Python API**

```python
from pathlib import Path
from header_extraction import HeaderExtractionPipeline
from header_extraction.config import PipelineConfig

config = PipelineConfig(
    input_folder=Path("pdfs"),
    output_excel_path=Path("headers_output.xlsx"),
    debug_output_dir=Path("debug_out"),  # optional
)
pipeline = HeaderExtractionPipeline(config=config)
results = pipeline.process_folder(Path("pdfs"))

for r in results:
    print(r.pdf_path, r.confidence, r.selected_headers)
```

## Output

- **Excel** (`headers_output.xlsx`): Sheet "Headers" with columns `PDF_File_Name`, `Confidence`, `Header_1`, `Header_2`, ...
- **Debug** (if `--debug-dir` set): Per-PDF JSON with `header_zone_detected`, `raw_header_candidates`, `final_selected_header`, `confidence_breakdown`.

## Architecture

| Stage | Module | Role |
|-------|--------|------|
| 1 | `layout_reader` | PyMuPDF: words, bbox, font size, drawing lines |
| 2 | `table_header_detector` | **Table-aware**: first data row → header row above it, or line with most header keywords |
| 2b | `header_zone_detector` | Fallback: visual band (density, font, separator) |
| 3 | `header_reconstruction` | Single-line = one column per word; compound merge (Product+Name, Cl+qty, etc.) |
| 4 | `header_filter` | Drop page numbers, logos, obvious non-headers |
| 5 | `semantic_normalizer` | Domain dict + fuzzy; short tokens (In, Op, Cl) kept as-is |
| 6 | `data_validation` | Column data type (date/decimal/int) vs header |
| 7 | `consensus` | Multi-page voting + weighted confidence |
| 8 | `excel_export` | One Excel file, one row per PDF |

## Validation (pdfsamples)

Run on the included sample folder and check the Excel:

```bash
python run_extraction.py pdfsamples --output headers_pdfsamples.xlsx --debug-dir debug_pdfsamples
```

Expected: **10000003** (D.S.AGENCIES) → 12 columns (Product Name, Pack, Op, Purchase, Gd.In, Tot, In, Sale, Closing, Purchase, Sale, Stock). **10000029** (ABBOTT) → 7 columns (Product Name, Unit, Co Op Qty, InQty, OutQty, Cl Qty, Cl Val). **10000047** (ABBOTT HEALTHCARE) → 11 columns (Product Name, Pack, LstSL, Op, Recd., Sales, Cl, Order, Pend, LstMove, Stk.Value).

## Performance

- Designed for **~1000 PDFs** per run without memory blow-up (process one PDF at a time).
- **Deterministic** for same input and config.

## Future Hooks

- `header_extraction.future_hooks.TemplateMemory` — remember headers per stockist
- `StockistHeaderLearning` — learn new synonyms from corrections
- `CrossPDFConsensusLearning` — consensus across PDFs

## License

Use as needed for the project.
"# smartstock---header-collection" 
