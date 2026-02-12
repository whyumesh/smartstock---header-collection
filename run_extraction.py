#!/usr/bin/env python3
"""
CLI entrypoint for the Header Extraction System.
Usage:
  python run_extraction.py <input_folder> [--output headers_output.xlsx] [--debug-dir debug]
  python run_extraction.py --help
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure package is importable when run from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from header_extraction import HeaderExtractionPipeline
from header_extraction.config import PipelineConfig


def setup_logging(level: str = "INFO") -> None:
    """Configure logging: explain WHY headers were chosen."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Offline high-precision header extraction from PDFs. Produces one Excel file."
    )
    parser.add_argument(
        "input_folder",
        type=Path,
        help="Folder containing PDF files",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("headers_output.xlsx"),
        help="Output Excel file path (default: headers_output.xlsx)",
    )
    parser.add_argument(
        "--debug-dir",
        type=Path,
        default=None,
        help="If set, write per-PDF debug JSON here",
    )
    parser.add_argument(
        "--max-pdfs",
        type=int,
        default=None,
        help="Max number of PDFs to process (default: no limit)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    setup_logging(args.log_level)

    if not args.input_folder.is_dir():
        print(f"Error: input folder does not exist: {args.input_folder}", file=sys.stderr)
        return 1

    config = PipelineConfig(
        input_folder=args.input_folder,
        output_excel_path=args.output,
        debug_output_dir=args.debug_dir,
        max_pdfs=args.max_pdfs,
        log_level=args.log_level,
    )

    pipeline = HeaderExtractionPipeline(config=config)
    results = pipeline.process_folder(
        args.input_folder,
        output_excel_path=args.output,
        debug_dir=args.debug_dir,
    )

    success = sum(1 for r in results if r.confidence > 0 and r.selected_headers)
    print(f"Done. Processed {len(results)} PDFs, {success} with extracted headers.")
    print(f"Output: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
