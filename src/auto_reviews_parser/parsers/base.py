import asyncio
import random
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Optional


class DelayManager:
    """Utility to manage asynchronous delays."""

    async def sleep(self, min_delay: float = 0.0, max_delay: float = 0.0) -> None:
        delay = random.uniform(min_delay, max_delay) if max_delay >= min_delay else min_delay
        await asyncio.sleep(delay)


class TextExtractor:
    """Utility providing text normalisation and field extraction helpers."""

    def normalize(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text.strip())
        text = re.sub(r"<[^>]+>", "", text)
        return text

    def extract_year(self, text: str) -> Optional[int]:
        match = re.search(r"\b(19|20)\d{2}\b", text)
        if match:
            year = int(match.group())
            if 1980 <= year <= datetime.now().year:
                return year
        return None

    def extract_mileage(self, text: str) -> Optional[int]:
        patterns = [
            r"(\d+(?:\s*\d{3})*)\s*(?:тыс\.?\s*)?км",
            r"(\d+)\s*(?:k|К)\s*км",
            r"пробег[:\s]*(\d+(?:\s*\d{3})*)",
            r"(\d+(?:\s*\d{3})*)\s*(?:тысяч|тыс)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                mileage_str = match.group(1).replace(" ", "")
                try:
                    mileage = int(mileage_str)
                    if mileage < 1000:
                        mileage *= 1000
                    return mileage
                except ValueError:
                    continue
        return None

    def extract_engine_volume(self, text: str) -> Optional[float]:
        patterns = [
            r"(\d+(?:\.\d+)?)\s*л",
            r"(\d{4})\s*см³",
            r"(\d+\.\d+)",
        ]
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    volume = float(match)
                    if volume > 100:
                        volume = volume / 1000
                    if 0.8 <= volume <= 8.0:
                        return volume
                except ValueError:
                    continue
        return None

    def extract_fuel_type(self, text: str) -> str:
        low = text.lower()
        if "бензин" in low:
            return "бензин"
        if "дизель" in low:
            return "дизель"
        if "гибрид" in low:
            return "гибрид"
        return ""

    def extract_transmission(self, text: str) -> str:
        low = text.lower()
        if "автомат" in low or "акпп" in low:
            return "автомат"
        if "механ" in low or "мкпп" in low:
            return "механика"
        if "вариатор" in low:
            return "вариатор"
        return ""

    def extract_drive_type(self, text: str) -> str:
        low = text.lower()
        if "полный" in low or "4wd" in low:
            return "полный"
        if "передний" in low or "fwd" in low:
            return "передний"
        if "задний" in low or "rwd" in low:
            return "задний"
        return ""

    def parse_date(self, date_text: str) -> Optional[datetime]:
        try:
            date_text = re.sub(r"[^\d\.\s\w]", "", date_text).strip()
            now = datetime.now()
            low = date_text.lower()
            if "сегодня" in low:
                return now.replace(hour=12, minute=0, second=0, microsecond=0)
            if "вчера" in low:
                return (now - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
            if "назад" in low:
                if "дн" in low:
                    m = re.search(r"(\d+)\s*дн", low)
                    if m:
                        return now - timedelta(days=int(m.group(1)))
                if "час" in low:
                    m = re.search(r"(\d+)\s*час", low)
                    if m:
                        return now - timedelta(hours=int(m.group(1)))
            patterns = [r"(\d{1,2})\.(\d{1,2})\.(\d{4})", r"(\d{1,2})\s+(\w+)\s+(\d{4})", r"(\d{4})-(\d{2})-(\d{2})"]
            months = {
                "января": 1,
                "февраля": 2,
                "марта": 3,
                "апреля": 4,
                "мая": 5,
                "июня": 6,
                "июля": 7,
                "августа": 8,
                "сентября": 9,
                "октября": 10,
                "ноября": 11,
                "декабря": 12,
            }
            for pattern in patterns:
                match = re.search(pattern, date_text)
                if match:
                    g = match.groups()
                    if len(g) == 3:
                        if g[1].isdigit():
                            day, month, year = map(int, g)
                        else:
                            day = int(g[0])
                            month = months.get(g[1].lower(), 1)
                            year = int(g[2])
                        return datetime(year, month, day)
        except Exception:
            return None
        return None


class BaseParser(ABC):
    """Base asynchronous parser providing shared utilities."""

    def __init__(self, delay_manager: Optional[DelayManager] = None, text_extractor: Optional[TextExtractor] = None) -> None:
        self.delay_manager = delay_manager or DelayManager()
        self.text_extractor = text_extractor or TextExtractor()

    async def random_delay(self, min_delay: float = 0, max_delay: float = 0) -> None:
        await self.delay_manager.sleep(min_delay, max_delay)

    def extract_common_fields(self, text: str) -> Dict[str, Any]:
        normalized = self.text_extractor.normalize(text)
        return {
            "year": self.text_extractor.extract_year(normalized),
            "engine_volume": self.text_extractor.extract_engine_volume(normalized),
            "mileage": self.text_extractor.extract_mileage(normalized),
            "fuel_type": self.text_extractor.extract_fuel_type(normalized),
            "transmission": self.text_extractor.extract_transmission(normalized),
            "drive_type": self.text_extractor.extract_drive_type(normalized),
        }

    @abstractmethod
    async def parse_reviews(self, html: str, brand: str, model: str):
        """Parse reviews from provided HTML for a given brand/model."""
        raise NotImplementedError
