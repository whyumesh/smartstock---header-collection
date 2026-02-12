"""
Header Extraction System - Document intelligence for column header extraction from PDFs.
Fully offline, production-grade, extensible.
"""

__version__ = "1.0.0"

from .pipeline import HeaderExtractionPipeline
from .config import PipelineConfig

__all__ = ["HeaderExtractionPipeline", "PipelineConfig", "__version__"]
