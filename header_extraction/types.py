"""
Shared data types for the header extraction pipeline.
Immutable-friendly and serializable for debug output.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional, Dict


@dataclass(frozen=True)
class WordBlock:
    """Single word or token with position and optional font info."""

    text: str
    x0: float
    y0: float
    x1: float
    y1: float
    font_size: float = 0.0
    page_no: int = 0

    @property
    def mid_x(self) -> float:
        return (self.x0 + self.x1) / 2

    @property
    def mid_y(self) -> float:
        return (self.y0 + self.y1) / 2

    @property
    def width(self) -> float:
        return self.x1 - self.x0

    @property
    def height(self) -> float:
        return self.y1 - self.y0


@dataclass
class LineBlock:
    """Horizontal line (separator) in the PDF."""

    x0: float
    y0: float
    x1: float
    y1: float
    page_no: int = 0

    @property
    def is_horizontal(self) -> bool:
        return abs(self.y1 - self.y0) < abs(self.x1 - self.x0)

    @property
    def y(self) -> float:
        return (self.y0 + self.y1) / 2


@dataclass
class HeaderZone:
    """Detected header band on a page."""

    y_min: float
    y_max: float
    page_no: int
    confidence: float
    reason: str = ""
    word_blocks: List[WordBlock] = field(default_factory=list)


@dataclass
class ReconstructedHeader:
    """One reconstructed column header (possibly multi-line)."""

    text: str
    column_index: int
    x_min: float
    x_max: float
    tokens: List[WordBlock] = field(default_factory=list)
    reconstruction_score: float = 1.0


@dataclass
class NormalizedHeader:
    """Header after semantic normalization."""

    original: str
    normalized: str
    semantic_score: float
    source: str  # "exact" | "fuzzy" | "domain"


@dataclass
class ColumnDataProfile:
    """Inferred type/distribution of values in a column."""

    column_index: int
    sample_values: List[str]
    inferred_type: str  # "date" | "decimal" | "integer" | "alphanumeric" | "text" | "unknown"
    match_ratio: float  # Fraction of samples matching inferred type
    validation_score: float  # 0-1


@dataclass
class HeaderCandidate:
    """Full header candidate with all scores (per page or consensus)."""

    headers: List[str]
    visual_score: float
    reconstruction_score: float
    semantic_score: float
    data_validation_score: float
    consensus_score: float
    combined_confidence: float
    source_pages: List[int] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PDFExtractionResult:
    """Result of processing one PDF."""

    pdf_path: str
    selected_headers: List[str]
    confidence: float
    confidence_breakdown: Dict[str, float]
    header_zone_detected: Optional[Dict[str, Any]] = None
    raw_header_candidates: Optional[List[Dict[str, Any]]] = None
    final_selected_header: Optional[List[str]] = None
    debug: Dict[str, Any] = field(default_factory=dict)
