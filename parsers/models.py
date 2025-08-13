from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class ReviewData:
    """Структура данных отзыва"""

    source: str  # drom.ru, drive2.ru
    type: str  # review, board_journal
    brand: str
    model: str
    generation: Optional[str] = None
    year: Optional[int] = None
    url: str = ""
    title: str = ""
    content: str = ""
    author: str = ""
    rating: Optional[float] = None
    pros: str = ""
    cons: str = ""
    mileage: Optional[int] = None
    engine_volume: Optional[float] = None
    fuel_type: str = ""
    transmission: str = ""
    body_type: str = ""
    drive_type: str = ""
    publish_date: Optional[datetime] = None
    views_count: Optional[int] = None
    likes_count: Optional[int] = None
    comments_count: Optional[int] = None
    parsed_at: datetime = None
    content_hash: str = ""

    def __post_init__(self):
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        content_for_hash = (
            f"{self.url}_{self.title}_{self.content[:100] if self.content else ''}"
        )
        self.content_hash = hashlib.md5(content_for_hash.encode()).hexdigest()
