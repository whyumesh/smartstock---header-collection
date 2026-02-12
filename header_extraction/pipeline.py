"""
Header Intelligence Pipeline:
PDF → Layout Analysis → Header Zone Detection → Data Column Count → Header Text Reconstruction
→ Anti-Noise Filter → Column Explosion Guard → Semantic Normalization → Data Validation
→ Precision Evidence Gate → Multi-Candidate Voting → Final Header Selection → Excel Output.
Philosophy: Prove header using table + column + data + structure evidence; reject if evidence < threshold.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import PipelineConfig
from .consensus import build_consensus, combine_confidence, select_best_candidate
from .data_column_detector import detect_data_column_count, infer_column_intervals_from_data
from .data_validation import ColumnDataValidator, DOMAIN_PRIORITY_HEADERS
from .header_filter import filter_reconstructed_with_noise_tracking
from .header_reconstruction import (
    HeaderReconstructionEngine,
    split_merged_headers_by_data_columns,
)
from .header_zone_detector import HeaderZoneDetector
from .table_header_detector import TableHeaderDetector, HEADER_KEYWORDS
from .layout_reader import LayoutDocument, PDFLayoutReader
from .precision_validation import validate_candidate_with_evidence
from .semantic_normalizer import SemanticHeaderNormalizer
from .types import HeaderCandidate, PDFExtractionResult
from .excel_export import export_to_excel

logger = logging.getLogger(__name__)

# Header lists that indicate footer/total row misclassified as header (reject)
FOOTER_LIKE_HEADERS = frozenset({
    ("grand", "tot"), ("grand", "total"), ("total",), ("grand",),
})


def _is_footer_like_candidate(headers: List[str]) -> bool:
    """True if extracted headers look like a footer/total row, not table header."""
    if not headers or len(headers) <= 2:
        return True
    h_lower = tuple(h.strip().lower() for h in headers[:5])
    if h_lower in FOOTER_LIKE_HEADERS:
        return True
    if len(headers) <= 2 and any("total" in h.lower() or "grand" in h.lower() for h in headers):
        return True
    return False


class HeaderExtractionPipeline:
    """
    Production-grade header extraction pipeline.
    Fully offline, deterministic, explainable, extensible.
    """

    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        self.config = config or PipelineConfig()
        self.layout_reader = PDFLayoutReader()
        self.zone_detector = HeaderZoneDetector(self.config)
        self.table_header_detector = TableHeaderDetector(self.config)
        self.reconstructor = HeaderReconstructionEngine(self.config)
        self.normalizer = SemanticHeaderNormalizer(self.config)
        self.validator = ColumnDataValidator(self.config)

    def process_folder(
        self,
        input_folder: Path,
        output_excel_path: Optional[Path] = None,
        debug_dir: Optional[Path] = None,
    ) -> List[PDFExtractionResult]:
        """
        Process all PDFs in folder. Write one Excel file.
        Returns list of PDFExtractionResult per PDF.
        """
        input_folder = Path(input_folder)
        output_excel_path = Path(output_excel_path or self.config.output_excel_path)
        debug_dir = Path(debug_dir) if debug_dir else self.config.debug_output_dir

        pdf_files = list(input_folder.glob("*.pdf")) + list(input_folder.glob("*.PDF"))
        pdf_files = sorted(set(pdf_files), key=lambda p: p.name.lower())
        if self.config.max_pdfs is not None:
            pdf_files = pdf_files[: self.config.max_pdfs]

        logger.info("Processing %d PDFs from %s", len(pdf_files), input_folder)
        results: List[PDFExtractionResult] = []
        for i, pdf_path in enumerate(pdf_files):
            try:
                result = self.process_one_pdf(pdf_path)
                results.append(result)
                if debug_dir:
                    self._write_debug_json(result, debug_dir)
            except Exception as e:
                logger.exception("Failed to process %s: %s", pdf_path, e)
                results.append(
                    PDFExtractionResult(
                        pdf_path=str(pdf_path),
                        selected_headers=[],
                        confidence=0.0,
                        confidence_breakdown={},
                        debug={"error": str(e)},
                    )
                )

        export_to_excel(results, output_excel_path, self.config)
        logger.info("Wrote %s with %d PDFs", output_excel_path, len(results))
        return results

    def process_one_pdf(self, pdf_path: Path) -> PDFExtractionResult:
        """Run full pipeline on one PDF. Evidence-based: reject candidate if evidence < threshold."""
        pdf_path = Path(pdf_path)
        logger.info("Processing PDF: %s", pdf_path.name)

        # 1) Layout
        layout = self.layout_reader.read(pdf_path)
        if not layout.words:
            logger.warning("No words extracted from %s", pdf_path.name)
            return PDFExtractionResult(
                pdf_path=str(pdf_path),
                selected_headers=[],
                confidence=0.0,
                confidence_breakdown={},
                debug={"reason": "no_words"},
            )

        # 2) Per-page: zone → data column count → reconstruct → filter → explosion guard → normalize → validate → precision gate
        per_page_candidates: List[HeaderCandidate] = []
        header_zone_info: List[Dict[str, Any]] = []
        raw_candidates_debug: List[Dict[str, Any]] = []
        rejected_candidates_reason: List[Dict[str, Any]] = []
        candidate_header_rows: List[Dict[str, Any]] = []

        for page_no in range(layout.num_pages):
            zone = self.table_header_detector.detect(layout, page_no)
            if zone is None:
                zone = self.zone_detector.detect(layout, page_no)
            if zone is None:
                continue
            header_zone_info.append({
                "page_no": page_no,
                "y_min": zone.y_min,
                "y_max": zone.y_max,
                "confidence": zone.confidence,
                "reason": zone.reason,
            })
            # Data column count for consistency gate and explosion guard
            data_column_count, _ = detect_data_column_count(
                layout, zone, page_no,
                max_data_rows=getattr(self.config, "data_row_sample_for_columns", 20),
            )
            reconstructed, recon_score, merge_ops = self.reconstructor.reconstruct(zone)
            if not reconstructed:
                rejected_candidates_reason.append({
                    "page_no": page_no,
                    "reason": "reconstruct_empty",
                })
                continue
            # Column explosion guard: split if header_cols < data_cols
            split_ops: List[Dict[str, Any]] = []
            if data_column_count > 0 and len(reconstructed) < data_column_count:
                data_intervals = infer_column_intervals_from_data(
                    layout, zone, page_no,
                    max_data_rows=getattr(self.config, "data_row_sample_for_columns", 20),
                )
                if data_intervals:
                    reconstructed, split_ops = split_merged_headers_by_data_columns(
                        reconstructed, data_intervals
                    )
            filtered, noise_tokens_removed = filter_reconstructed_with_noise_tracking(reconstructed)
            if not filtered:
                rejected_candidates_reason.append({
                    "page_no": page_no,
                    "reason": "all_filtered_noise",
                    "noise_removed": noise_tokens_removed,
                })
                continue
            normalized, sem_score = self.normalizer.normalize(filtered)
            column_intervals = [(r.x_min, r.x_max) for r in filtered]
            _, data_score, data_type_mismatch_count = self.validator.validate(
                layout, zone, column_intervals, normalized, page_no
            )
            headers = [n.normalized for n in normalized]
            consensus_score = 1.0 if layout.num_pages == 1 else 0.5
            combined = combine_confidence(
                zone.confidence,
                recon_score,
                sem_score,
                data_score,
                consensus_score,
                self.config,
            )
            cand = HeaderCandidate(
                headers=headers,
                visual_score=zone.confidence,
                reconstruction_score=recon_score,
                semantic_score=sem_score,
                data_validation_score=data_score,
                consensus_score=consensus_score,
                combined_confidence=combined,
                source_pages=[page_no],
                debug={
                    "reconstructed": [r.text for r in filtered],
                    "normalized": headers,
                    "merge_operations_performed": merge_ops,
                    "split_operations_performed": split_ops,
                    "noise_tokens_removed": noise_tokens_removed,
                    "data_column_count_detected": data_column_count,
                },
            )
            candidate_header_rows.append({
                "page_no": page_no,
                "headers": headers,
                "data_column_count_detected": data_column_count,
                "combined_confidence": combined,
            })
            # Precision evidence gate: prove this is header; reject if evidence < threshold
            is_valid, reject_reason, adj_confidence, evidence_debug = validate_candidate_with_evidence(
                cand,
                data_column_count=data_column_count,
                layout_num_pages=layout.num_pages,
                source_pages=[page_no],
                noise_tokens_removed=noise_tokens_removed,
                data_type_mismatch_count=data_type_mismatch_count,
                title_zone_token_count=0,
                config=self.config,
                header_keywords=HEADER_KEYWORDS,
                domain_boost_headers=DOMAIN_PRIORITY_HEADERS,
            )
            if not is_valid:
                rejected_candidates_reason.append({
                    "page_no": page_no,
                    "reason": reject_reason,
                    "evidence_debug": evidence_debug,
                })
                logger.debug("Rejected candidate page %d: %s", page_no, reject_reason)
                continue
            if _is_footer_like_candidate(cand.headers):
                rejected_candidates_reason.append({
                    "page_no": page_no,
                    "reason": "footer_like_headers",
                    "headers": cand.headers,
                })
                logger.debug("Rejected footer-like candidate page %d: %s", page_no, cand.headers[:5])
                continue
            cand = HeaderCandidate(
                headers=cand.headers,
                visual_score=cand.visual_score,
                reconstruction_score=cand.reconstruction_score,
                semantic_score=cand.semantic_score,
                data_validation_score=cand.data_validation_score,
                consensus_score=cand.consensus_score,
                combined_confidence=adj_confidence,
                source_pages=cand.source_pages,
                debug={**cand.debug, "precision_evidence": evidence_debug},
            )
            per_page_candidates.append(cand)
            raw_candidates_debug.append({
                "page_no": page_no,
                "headers": headers,
                "combined_confidence": adj_confidence,
                "visual_score": zone.confidence,
                "reconstruction_score": recon_score,
                "semantic_score": sem_score,
                "data_validation_score": data_score,
                "data_column_count_detected": data_column_count,
            })

        if not per_page_candidates:
            return PDFExtractionResult(
                pdf_path=str(pdf_path),
                selected_headers=[],
                confidence=0.0,
                confidence_breakdown={},
                header_zone_detected=header_zone_info or None,
                raw_header_candidates=raw_candidates_debug or None,
                debug={
                    "reason": "no_header_candidates",
                    "candidate_header_rows": candidate_header_rows,
                    "rejected_candidates_reason": rejected_candidates_reason,
                },
            )

        # 3) Multi-candidate voting: pick best by combined (evidence-adjusted) confidence
        if len(per_page_candidates) > 1:
            best = build_consensus(per_page_candidates, self.config)
        else:
            best = per_page_candidates[0]

        breakdown = {
            "visual_score": best.visual_score,
            "reconstruction_score": best.reconstruction_score,
            "semantic_score": best.semantic_score,
            "data_validation_score": best.data_validation_score,
            "consensus_score": best.consensus_score,
        }

        logger.info(
            "Selected %d headers for %s (confidence=%.2f): %s",
            len(best.headers),
            pdf_path.name,
            best.combined_confidence,
            best.headers[:8],
        )

        # Mandatory debug outputs
        debug_out = dict(best.debug)
        debug_out["candidate_header_rows"] = candidate_header_rows
        debug_out["rejected_candidates_reason"] = rejected_candidates_reason
        debug_out["final_header_columns"] = best.headers
        debug_out["data_column_count_detected"] = (
            best.debug.get("data_column_count_detected")
            or (raw_candidates_debug[0].get("data_column_count_detected") if raw_candidates_debug else None)
        )
        debug_out["merge_operations_performed"] = best.debug.get("merge_operations_performed", [])
        debug_out["split_operations_performed"] = best.debug.get("split_operations_performed", [])
        debug_out["noise_tokens_removed"] = best.debug.get("noise_tokens_removed", [])

        return PDFExtractionResult(
            pdf_path=str(pdf_path),
            selected_headers=best.headers,
            confidence=best.combined_confidence,
            confidence_breakdown=breakdown,
            header_zone_detected=header_zone_info or None,
            raw_header_candidates=raw_candidates_debug,
            final_selected_header=best.headers,
            debug=debug_out,
        )

    def _write_debug_json(self, result: PDFExtractionResult, debug_dir: Path) -> None:
        """Write optional debug JSON per PDF."""
        debug_dir.mkdir(parents=True, exist_ok=True)
        name = Path(result.pdf_path).stem
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
        path = debug_dir / f"{safe}_debug.json"
        obj = {
            "pdf_path": result.pdf_path,
            "header_zone_detected": result.header_zone_detected,
            "raw_header_candidates": result.raw_header_candidates,
            "final_selected_header": result.final_selected_header,
            "confidence": result.confidence,
            "confidence_breakdown": result.confidence_breakdown,
            "debug": result.debug,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        logger.debug("Wrote debug JSON: %s", path)


def run_pipeline(
    input_folder: Path,
    output_excel_path: Optional[Path] = None,
    debug_dir: Optional[Path] = None,
    config: Optional[PipelineConfig] = None,
) -> List[PDFExtractionResult]:
    """Convenience entry: run pipeline on folder."""
    pipeline = HeaderExtractionPipeline(config=config)
    return pipeline.process_folder(input_folder, output_excel_path, debug_dir)
