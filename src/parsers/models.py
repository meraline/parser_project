"""Model definitions used by the parsers."""

# The project keeps the dataclass definition in ``auto_reviews_parser`` to
# simplify access for the unit tests.  Re-export it here so that parser
# modules can continue importing :class:`ReviewData` from
# ``parsers.models``.

from auto_reviews_parser import ReviewData


__all__ = ["ReviewData"]

