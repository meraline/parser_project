"""Repository layer for working with reviews."""

from __future__ import annotations

from typing import Iterable

from auto_reviews_parser.parsers import Review
from auto_reviews_parser import ReviewsDatabase


class ReviewRepository:
    """Wrapper around :class:`ReviewsDatabase` providing a small repository API."""

    def __init__(self, db: ReviewsDatabase):
        self._db = db

    def save(self, review: Review) -> bool:
        """Persist a single review to the database."""
        return self._db.save_review(review)

    def save_many(self, reviews: Iterable[Review]) -> int:
        """Persist multiple reviews and return number of saved items."""
        count = 0
        for review in reviews:
            if self.save(review):
                count += 1
        return count

    def stats(self):
        """Return database statistics."""
        return self._db.get_parsing_stats()


__all__ = ["ReviewRepository"]
