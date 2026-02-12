"""
Pipeline configuration. Central place for tunable parameters.
Extensible for future learning (e.g. load from template memory).
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PipelineConfig:
    """Configuration for the header extraction pipeline."""

    # Paths
    input_folder: Path = field(default_factory=Path)
    output_excel_path: Path = field(default_factory=lambda: Path("headers_output.xlsx"))
    debug_output_dir: Optional[Path] = None  # If set, write per-PDF debug JSON

    # Header zone detection
    header_band_max_height_ratio: float = 0.25  # Max fraction of page height for header band
    min_words_in_header_band: int = 2
    font_size_boost_threshold: float = 1.15  # Text is "header" if >= this * median font
    separator_line_proximity_px: float = 20.0

    # Reconstruction
    x_overlap_tolerance_ratio: float = 0.3  # Words in same column if overlap >= this
    y_proximity_px: float = 5.0  # Same line if vertical distance <= this
    y_line_merge_threshold_px: float = 12.0  # Stacked lines merge if gap <= this

    # Semantic
    fuzzy_min_score: int = 80  # 0-100, rapidfuzz
    domain_dict_priority: bool = True  # Prefer domain synonyms

    # Data validation
    date_column_min_ratio: float = 0.5  # Min fraction of values that look like dates
    numeric_column_min_ratio: float = 0.6
    sample_cells_per_column: int = 50  # How many cells to check per column

    # Multi-page
    min_consensus_ratio: float = 0.5  # Header must appear on this fraction of pages
    consensus_vote_weight: str = "confidence"  # "confidence" or "count"

    # Confidence weights (must sum to 1.0 for interpretability)
    weight_visual: float = 0.25
    weight_reconstruction: float = 0.25
    weight_semantic: float = 0.20
    weight_data_validation: float = 0.20
    weight_consensus: float = 0.10

    # Precision layer: evidence-based rejection
    max_header_data_column_diff: int = 1  # HARD: allow max ±1 header vs data column count
    precision_evidence_threshold: float = 0.35  # Reject candidate if combined evidence < this
    merged_column_penalty: float = 0.15  # Penalty when header_cols != data_cols (within gate)
    noise_word_penalty: float = 0.15  # Per noise token
    data_type_mismatch_penalty: float = 0.20  # Per column type mismatch
    title_zone_penalty: float = 0.30  # Token in title zone
    domain_header_boost: float = 0.08  # Boost for known domain headers (Indian pharma)

    # Compound merge: strict mode (only merge if known list / freq / gap)
    compound_merge_strict: bool = True
    compound_merge_min_cross_pdf_count: int = 3  # Allow merge if pair seen this many times
    compound_merge_max_gap_ratio: float = 1.0  # Allow merge if gap < median_col_gap * this

    # Data column detection
    data_row_sample_for_columns: int = 20

    # Batch / performance
    max_pdfs: Optional[int] = None  # None = no limit (e.g. 1000)
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if isinstance(self.input_folder, str):
            self.input_folder = Path(self.input_folder)
        if isinstance(self.output_excel_path, str):
            self.output_excel_path = Path(self.output_excel_path)
        if self.debug_output_dir is not None and isinstance(self.debug_output_dir, str):
            self.debug_output_dir = Path(self.debug_output_dir)
