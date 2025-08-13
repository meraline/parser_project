import random
import time
import re
from datetime import datetime, timedelta
from typing import Optional

<<<<<<< HEAD
from src.utils.logger import get_logger
from src.utils.validators import validate_not_none

logger = get_logger(__name__)
=======
from src.utils.delay_manager import DelayManager
from src.utils.retry_decorator import retry_async
>>>>>>> origin/codex/implement-delay-manager-and-retry-decorator


class BaseParser:
    """Базовый парсер с общими утилитами"""

<<<<<<< HEAD
    def __init__(self, db):
        self.db = validate_not_none(db, "db")
=======
    def __init__(self, db, delay_manager: DelayManager | None = None):
        self.db = db
        self.delay_manager = delay_manager or DelayManager()
>>>>>>> origin/codex/implement-delay-manager-and-retry-decorator
        self.session_stats = {
            "parsed": 0,
            "saved": 0,
            "errors": 0,
            "skipped": 0,
        }

    def random_delay(self, min_delay: int = 5, max_delay: int = 15):
        """Случайная задержка между запросами"""
        self.delay_manager.min_delay = min_delay
        self.delay_manager.max_delay = max_delay
        self.delay_manager.sleep()

    @retry_async()
    async def _run_with_retry(self, func, *args, **kwargs):
        """Выполнение функции с повторными попытками"""
        return func(*args, **kwargs)

    def normalize_text(self, text: str) -> str:
        """Нормализация текста"""
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text.strip())
        text = re.sub(r"<[^>]+>", "", text)
        return text

    def extract_year(self, text: str) -> Optional[int]:
        """Извлечение года из текста"""
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if year_match:
            year = int(year_match.group())
            if 1980 <= year <= datetime.now().year:
                return year
        return None

    def extract_mileage(self, text: str) -> Optional[int]:
        """Извлечение пробега из текста"""
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
        """Извлечение объема двигателя из текста"""
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

    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """Парсинг даты из текста"""
        try:
            date_text = re.sub(r"[^\d\.\s\w]", "", date_text).strip()
            now = datetime.now()
            if "сегодня" in date_text.lower():
                return now.replace(hour=12, minute=0, second=0, microsecond=0)
            elif "вчера" in date_text.lower():
                return (now - timedelta(days=1)).replace(
                    hour=12, minute=0, second=0, microsecond=0
                )
            elif "назад" in date_text.lower():
                if "дн" in date_text:
                    days_match = re.search(r"(\d+)\s*дн", date_text)
                    if days_match:
                        days = int(days_match.group(1))
                        return now - timedelta(days=days)
                elif "час" in date_text:
                    hours_match = re.search(r"(\d+)\s*час", date_text)
                    if hours_match:
                        hours = int(hours_match.group(1))
                        return now - timedelta(hours=hours)
            patterns = [
                r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
                r"(\d{1,2})\s+(\w+)\s+(\d{4})",
                r"(\d{4})-(\d{2})-(\d{2})",
            ]
            months_map = {
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
                    groups = match.groups()
                    if len(groups) == 3:
                        if groups[1].isdigit():
                            day, month, year = map(int, groups)
                        else:
                            day = int(groups[0])
                            month = months_map.get(groups[1].lower(), 1)
                            year = int(groups[2])
                        return datetime(year, month, day)
        except Exception:
            pass
        return None
