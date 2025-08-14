# 1. Исправить src/auto_reviews_parser/parsers/__init__.py
from .base import BaseParser
from .drom import DromParser
from .drive2 import Drive2Parser
from ..models.review import Review

__all__ = [
    "BaseParser",
    "DromParser",
    "Drive2Parser",
    "Review",
]
