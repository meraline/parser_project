from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import hashlib


@dataclass
class Review:
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
    # Оценки по категориям
    overall_rating: Optional[float] = None  # общая оценка машины
    exterior_rating: Optional[int] = None  # внешний вид
    interior_rating: Optional[int] = None  # салон
    engine_rating: Optional[int] = None  # двигатель
    driving_rating: Optional[int] = None  # ходовые качества
    pros: str = ""
    cons: str = ""
    mileage: Optional[int] = None
    engine_volume: Optional[float] = None
    engine_power: Optional[int] = None  # мощность в л.с.
    fuel_type: str = ""
    fuel_consumption_city: Optional[float] = None  # л/100км
    fuel_consumption_highway: Optional[float] = None  # л/100км
    transmission: str = ""
    body_type: str = ""
    drive_type: str = ""
    steering_wheel: str = ""  # Левый/Правый
    year_purchased: Optional[int] = None  # год приобретения
    publish_date: Optional[datetime] = None
    views_count: Optional[int] = None
    likes_count: Optional[int] = None
    comments_count: Optional[int] = None
    parsed_at: Optional[datetime] = None
    content_hash: str = ""

    def __post_init__(self) -> None:
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        self.content_hash = self.generate_hash()

    def generate_hash(self) -> str:
        content_for_hash = (
            f"{self.url}_{self.title}_{self.content[:100] if self.content else ''}"
        )
        return hashlib.md5(content_for_hash.encode()).hexdigest()
