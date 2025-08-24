from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple
import logging


class ParallelParserService:
    """Run multiple parsers concurrently using a thread pool."""

    def __init__(self, parsers: Dict[str, Any], max_workers: int = 4) -> None:
        self.parsers = parsers
        self.max_workers = max_workers

    def _parse_source(
        self,
        brand: str,
        model: str,
        source: str,
        base_params: Dict[str, Any],
    ) -> Tuple[Tuple[str, str, str], List[Any]]:
        parser = self.parsers.get(source)
        if parser is None:
            raise ValueError(f"Parser for source '{source}' not found")

        params = dict(base_params)
        params.update({"brand": brand, "model": model})
        reviews = parser.parse_brand_model_reviews(params)
        return (brand, model, source), reviews

    def parse_multiple_sources(
        self,
        sources: List[Tuple[str, str, str]],
        **base_params: Any,
    ) -> List[Tuple[Tuple[str, str, str], List[Any]]]:
        """Parse several sources concurrently.

        Args:
            sources: List of tuples ``(brand, model, source)``.
            **base_params: Additional parameters passed to each parser.

        Returns:
            List of tuples ``((brand, model, source), reviews)``.
        """

        results: List[Tuple[Tuple[str, str, str], List[Any]]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._parse_source, brand, model, source, base_params
                ): (brand, model, source)
                for brand, model, source in sources
            }

            for future in as_completed(futures):
                source_info = futures[future]
                try:
                    result = future.result()
                except Exception:  # pragma: no cover - just logging
                    logging.error(
                        "Error parsing source %s", source_info, exc_info=True
                    )
                    result = (source_info, [])
                results.append(result)

        return results
