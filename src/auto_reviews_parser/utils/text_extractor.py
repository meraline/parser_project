from datetime import datetime
from typing import Optional
import re


class TextExtractor:
    """Class for extracting and formatting text data."""

    def normalize(self, text: str) -> str:
        """Normalize text by removing extra whitespace and newlines."""
        if not text:
            return ""
        return " ".join(text.strip().split())

    def extract_date(self, text: str) -> Optional[datetime]:
        """Extract date from text."""
        # Паттерн для поиска даты в форматах
        # DD.MM.YYYY или D.MM.YYYY или DD.M.YYYY
        pattern = r"(\d{1,2})\.(\d{1,2})\.(\d{4})"

        match = re.search(pattern, text)
        if match:
            day, month, year = map(int, match.groups())
            try:
                return datetime(year, month, day)
            except ValueError:
                return None
        return None
