from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any
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
    
    # Новые поля для совместимости с тестами
    id: Optional[str] = None
    city: Optional[str] = None
    date: Optional[str] = None
    useful_count: Optional[int] = None
    not_useful_count: Optional[int] = None

    def __post_init__(self) -> None:
        if self.parsed_at is None:
            self.parsed_at = datetime.now()
        self.content_hash = self.generate_hash()
        
        # Валидация данных
        self._validate()

    def _validate(self) -> None:
        """Валидация данных отзыва"""
        if self.rating is not None and not (1 <= self.rating <= 5):
            raise ValueError("Рейтинг должен быть от 1 до 5")
        
        if self.year is not None and not (1900 <= self.year <= datetime.now().year + 1):
            raise ValueError("Год должен быть разумным")
        
        if hasattr(self, 'content') and isinstance(self.content, dict) and not self.content:
            raise ValueError("Контент не может быть пустым")

    def generate_hash(self) -> str:
        """Генерация уникального хеша контента"""
        import hashlib
        content_str = str(self.content) if isinstance(self.content, dict) else (self.content or "")
        content_for_hash = (
            f"{self.url}_{self.title or ''}_{content_str[:100] if content_str else ''}"
        )
        return hashlib.md5(content_for_hash.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Review':
        """Создание из словаря"""
        return cls(**data)
    
    def __str__(self) -> str:
        """Строковое представление"""
        return f"{self.brand} {self.model} ({self.year}) - Рейтинг: {self.rating}"
    
    def __hash__(self) -> int:
        """Хеширование отзыва"""
        return hash(self.content_hash)
    
    def get_rating_description(self) -> str:
        """Получить текстовое описание рейтинга"""
        if self.rating is None:
            return "не оценено"
        
        descriptions = {
            1: "очень плохо",
            2: "плохо", 
            3: "средне",
            4: "хорошо",
            5: "отлично"
        }
        return descriptions.get(int(self.rating), "неизвестно")
    
    def get_age_days(self) -> int:
        """Получить возраст отзыва в днях"""
        if self.publish_date:
            return (datetime.now() - self.publish_date).days
        elif self.parsed_at:
            return (datetime.now() - self.parsed_at).days
        return 0
    
    def get_content_summary(self, max_length: int = 100) -> str:
        """Получить краткое содержание"""
        content = self.content or ""
        if isinstance(content, dict):
            # Если content это dict, объединяем все значения
            content = " ".join(str(v) for v in content.values() if v)
        
        if len(content) <= max_length:
            return content
        return content[:max_length-3] + "..."
    
    def is_useful(self, threshold: float = 0.7) -> bool:
        """Определить полезность отзыва по соотношению лайков"""
        if self.useful_count is None or self.not_useful_count is None:
            return True  # По умолчанию считаем полезным
        
        total = self.useful_count + self.not_useful_count
        if total == 0:
            return True
        
        ratio = self.useful_count / total
        return ratio >= threshold
