"""Lightweight module exposing parser classes for tests.

The test-suite imports ``DromParser`` and ``Drive2Parser`` from this file to
avoid importing heavy dependencies during collection.
"""

from parsers import DromParser, Drive2Parser


__all__ = ["DromParser", "Drive2Parser"]

