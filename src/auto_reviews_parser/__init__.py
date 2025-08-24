"""Top-level package for auto_reviews_parser.

This package exposes the main parser classes and configuration so that
consumers can simply ``import auto_reviews_parser`` and access the
parser, database and manager classes without dealing with the internal
module layout.
"""

from .database.base import Database
from .services.parser_service import ParserService
from .services.queue_service import QueueService
from .services.parallel_parser import ParallelParserService

__all__ = [
    "Database",
    "ParserService",
    "QueueService",
    "ParallelParserService",
]
