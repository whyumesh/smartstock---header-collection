"""
Future-ready hooks for extensibility.
Design so later we can plug:
- Template memory (remember header layouts per stockist)
- Stockist header learning
- Cross-PDF consensus learning
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TemplateMemory:
    """
    Stub: future hook for remembering header templates per stockist/source.
    When a PDF is from a known source, prefer that source's template.
    """

    def __init__(self, store_path: Optional[Path] = None) -> None:
        self.store_path = store_path
        self._cache: Dict[str, List[str]] = {}

    def lookup(self, source_id: str) -> Optional[List[str]]:
        """Return saved header template for source_id if any."""
        return self._cache.get(source_id)

    def save(self, source_id: str, headers: List[str]) -> None:
        """Save header template for source (e.g. after human correction)."""
        self._cache[source_id] = list(headers)
        if self.store_path:
            self._persist()

    def _persist(self) -> None:
        """Optional: persist to disk."""
        pass


class StockistHeaderLearning:
    """
    Stub: future hook for learning headers from stockist-specific PDFs.
    Can feed corrections or high-confidence extractions to improve domain dict.
    """

    def suggest_synonym(self, raw: str, canonical: str, confidence: float) -> None:
        """Record suggestion for domain synonym (e.g. stockist-specific column name)."""
        logger.debug("Learning suggestion: %r -> %r (%.2f)", raw, canonical, confidence)

    def get_learned_synonyms(self) -> Dict[str, str]:
        """Return learned synonym map to merge with domain dict."""
        return {}


class CrossPDFConsensusLearning:
    """
    Stub: use cross-PDF consensus to boost confidence or detect templates.
    """

    def add_extraction(self, pdf_id: str, headers: List[str], confidence: float) -> None:
        """Record extraction for later consensus analysis."""
        pass

    def get_consensus_headers(self, min_frequency: float = 0.5) -> List[str]:
        """Return headers that appear in at least min_frequency of PDFs."""
        return []
