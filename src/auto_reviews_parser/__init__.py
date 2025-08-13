"""Top-level package for auto_reviews_parser.

This package exposes the main parser classes and configuration so that
consumers can simply ``import auto_reviews_parser`` and access the
parser, database and manager classes without dealing with the internal
module layout.
"""

from .services.auto_reviews_parser import (
    AutoReviewsParser,
    ParserManager,
    ReviewsDatabase,
    Config,
)

__all__ = [
    "AutoReviewsParser",
    "ParserManager",
    "ReviewsDatabase",
    "Config",
]
