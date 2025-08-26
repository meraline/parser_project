"""
Расширенный менеджер базы данных PostgreSQL с поддержкой брендов, моделей и коротких отзывов
Версия: 2.0
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import asyncpg
import hashlib
from dataclasses import dataclass, asdict


@dataclass
class BrandData:
    """Данные о бренде автомобилей"""
    название: str
    название_в_url: str
    url: str
    количество_отзывов: int = 0


@dataclass
class ModelData:
    """Данные о модели автомобиля"""
    бренд_id: int
    название: str
    название_в_url: str
    поколение: Optional[str] = None
    тип_кузова: Optional[str] = None
    трансмиссия: Optional[str] = None
    тип_привода: Optional[str] = None
    руль: Optional[str] = None
    объем_двигателя_куб_см: Optional[int] = None
    мощность_двигателя_лс: Optional[int] = None
    тип_топлива: Optional[str] = None
    url: Optional[str] = None
    количество_отзывов: int = 0
    количество_коротких_отзывов: int = 0


@dataclass 
class FullReviewData:
    """Данные полного отзыва"""
    модель_id: int
    ссылка: str
    заголовок: Optional[str] = None
    содержание: Optional[str] = None
    плюсы: Optional[str] = None
    минусы: Optional[str] = None
    общий_рейтинг: Optional[float] = None
    рейтинг_владельца: Optional[float] = None
    год_приобретения: Optional[int] = None
    пробег_км: Optional[int] = None
    цвет_кузова: Optional[str] = None
    цвет_салона: Optional[str] = None
    количество_просмотров: int = 0
    количество_лайков: int = 0
    количество_дизлайков: int = 0
    количество_голосов: int = 0
    дата_публикации: Optional[datetime] = None
    длина_контента: Optional[int] = None
    хеш_содержания: Optional[str] = None
    статус_парсинга: str = 'успех'
    детали_ошибки: Optional[str] = None


@dataclass
class ShortReviewData:
    """Данные короткого отзыва"""
    модель_id: int
    внешний_id: str
    плюсы: Optional[str] = None
    минусы: Optional[str] = None
    поломки: Optional[str] = None
    год_автомобиля: Optional[int] = None
    объем_двигателя: Optional[float] = None
    тип_топлива: Optional[str] = None
    тип_трансмиссии: Optional[str] = None
    тип_привода: Optional[str] = None
    количество_фото: int = 0
    город_автора: Optional[str] = None
    дата_публикации: Optional[datetime] = None
    хеш_содержания: Optional[str] = None
    статус_парсинга: str = 'успех'


@dataclass
class ParseSessionData:
    """Данные сессии парсинга"""
    тип_парсинга: str  # 'catalog', 'full_reviews', 'short_reviews'
    бренд_id: Optional[int] = None
    модель_id: Optional[int] = None
    статус: str = 'в_процессе'
    обработано_страниц: int = 0
    найдено_отзывов: int = 0
    сохранено_отзывов: int = 0
    ошибок: int = 0
    детали_ошибки: Optional[str] = None
    версия_парсера: Optional[str] = None


class ExtendedPostgresManager:
    """Расширенный менеджер PostgreSQL с поддержкой брендов, моделей и коротких отзывов"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.pool: Optional[asyncpg.Pool] = None
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self):
        """Полная инициализация с созданием схемы"""
        await self.initialize_connection_only()
        await self.create_schema()
    
    async def initialize_connection_only(self):
        """Инициализация только подключения без создания схемы"""
        try:
            self.pool = await asyncpg.create_pool(**self.connection_params)
            await self.create_schema()
            self.logger.info("✓ PostgreSQL пул соединений инициализирован")
        except Exception as e:
            self.logger.error(f"Ошибка инициализации PostgreSQL: {e}")
            raise
    
    async def close(self):
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            self.logger.info("PostgreSQL пул соединений закрыт")
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для получения соединения"""
        if not self.pool:
            raise RuntimeError("Пул соединений не инициализирован")
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def create_schema(self):
        """Создание расширенной схемы базы данных"""
        schema_file = Path(__file__).parent / "create_extended_schema.sql"
        if not schema_file.exists():
            raise FileNotFoundError(f"Файл схемы не найден: {schema_file}")
        
        async with self.get_connection() as conn:
            schema_sql = schema_file.read_text(encoding='utf-8')
            await conn.execute(schema_sql)
            self.logger.info("✓ Расширенная схема PostgreSQL создана")
    
    # ==================== БРЕНДЫ ====================
    
    async def insert_brand(self, brand_data: BrandData) -> int:
        """Добавление бренда"""
        async with self.get_connection() as conn:
            brand_id = await conn.fetchval("""
                INSERT INTO auto_reviews.бренды (название, название_в_url, url, количество_отзывов)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (название_в_url) 
                DO UPDATE SET 
                    название = EXCLUDED.название,
                    url = EXCLUDED.url,
                    количество_отзывов = EXCLUDED.количество_отзывов,
                    дата_обновления = CURRENT_TIMESTAMP
                RETURNING id
            """, brand_data.название, brand_data.название_в_url, 
                brand_data.url, brand_data.количество_отзывов)
            
            self.logger.debug(f"Бренд сохранен: {brand_data.название} (ID: {brand_id})")
            return brand_id
    
    async def get_brand_by_slug(self, slug: str) -> Optional[Dict]:
        """Получение бренда по слагу"""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM auto_reviews.бренды WHERE название_в_url = $1
            """, slug)
            return dict(row) if row else None
    
    async def get_all_brands(self) -> List[Dict]:
        """Получение всех брендов"""
        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM auto_reviews.бренды ORDER BY название
            """)
            return [dict(row) for row in rows]
    
    # ==================== МОДЕЛИ ====================
    
    async def insert_model(self, model_data: ModelData) -> int:
        """Добавление модели"""
        async with self.get_connection() as conn:
            model_id = await conn.fetchval("""
                INSERT INTO auto_reviews.модели (
                    бренд_id, название, название_в_url, поколение, тип_кузова,
                    трансмиссия, тип_привода, руль, объем_двигателя_куб_см,
                    мощность_двигателя_лс, тип_топлива, url, количество_отзывов,
                    количество_коротких_отзывов
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (бренд_id, название_в_url, поколение, тип_кузова)
                DO UPDATE SET 
                    название = EXCLUDED.название,
                    трансмиссия = EXCLUDED.трансмиссия,
                    тип_привода = EXCLUDED.тип_привода,
                    руль = EXCLUDED.руль,
                    объем_двигателя_куб_см = EXCLUDED.объем_двигателя_куб_см,
                    мощность_двигателя_лс = EXCLUDED.мощность_двигателя_лс,
                    тип_топлива = EXCLUDED.тип_топлива,
                    url = EXCLUDED.url,
                    количество_отзывов = EXCLUDED.количество_отзывов,
                    количество_коротких_отзывов = EXCLUDED.количество_коротких_отзывов,
                    дата_обновления = CURRENT_TIMESTAMP
                RETURNING id
            """, model_data.бренд_id, model_data.название, model_data.название_в_url,
                model_data.поколение, model_data.тип_кузова, model_data.трансмиссия,
                model_data.тип_привода, model_data.руль, model_data.объем_двигателя_куб_см,
                model_data.мощность_двигателя_лс, model_data.тип_топлива, model_data.url,
                model_data.количество_отзывов, model_data.количество_коротких_отзывов)
            
            self.logger.debug(f"Модель сохранена: {model_data.название} (ID: {model_id})")
            return model_id
    
    async def get_model_by_brand_and_slug(self, brand_id: int, model_slug: str) -> Optional[Dict]:
        """Получение модели по ID бренда и слагу"""
        async with self.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM auto_reviews.модели 
                WHERE бренд_id = $1 AND название_в_url = $2
            """, brand_id, model_slug)
            return dict(row) if row else None
    
    async def get_models_by_brand(self, brand_id: int) -> List[Dict]:
        """Получение всех моделей бренда"""
        async with self.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM auto_reviews.модели 
                WHERE бренд_id = $1 
                ORDER BY название
            """, brand_id)
            return [dict(row) for row in rows]
    
    # ==================== ПОЛНЫЕ ОТЗЫВЫ ====================
    
    async def insert_full_review(self, review_data: FullReviewData, author_id: Optional[int] = None) -> int:
        """Добавление полного отзыва"""
        # Генерируем хеш содержания если не указан
        if not review_data.хеш_содержания and review_data.содержание:
            content_hash = hashlib.sha256(review_data.содержание.encode()).hexdigest()
            review_data.хеш_содержания = content_hash
        
        # Вычисляем длину контента если не указана
        if not review_data.длина_контента and review_data.содержание:
            review_data.длина_контента = len(review_data.содержание)
        
        async with self.get_connection() as conn:
            review_id = await conn.fetchval("""
                INSERT INTO auto_reviews.отзывы (
                    модель_id, автор_id, ссылка, заголовок, содержание, плюсы, минусы,
                    общий_рейтинг, рейтинг_владельца, год_приобретения, пробег_км,
                    цвет_кузова, цвет_салона, количество_просмотров, количество_лайков,
                    количество_дизлайков, количество_голосов, дата_публикации,
                    длина_контента, хеш_содержания, статус_парсинга, детали_ошибки
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                ON CONFLICT (ссылка) DO NOTHING
                RETURNING id
            """, review_data.модель_id, author_id, review_data.ссылка,
                review_data.заголовок, review_data.содержание, review_data.плюсы,
                review_data.минусы, review_data.общий_рейтинг, review_data.рейтинг_владельца,
                review_data.год_приобретения, review_data.пробег_км, review_data.цвет_кузова,
                review_data.цвет_салона, review_data.количество_просмотров,
                review_data.количество_лайков, review_data.количество_дизлайков,
                review_data.количество_голосов, review_data.дата_публикации,
                review_data.длина_контента, review_data.хеш_содержания,
                review_data.статус_парсинга, review_data.детали_ошибки)
            
            if review_id:
                self.logger.debug(f"Полный отзыв сохранен (ID: {review_id})")
            return review_id
    
    # ==================== КОРОТКИЕ ОТЗЫВЫ ====================
    
    async def insert_short_review(self, review_data: ShortReviewData, author_id: Optional[int] = None) -> int:
        """Добавление короткого отзыва"""
        # Генерируем хеш содержания
        if not review_data.хеш_содержания:
            content = f"{review_data.плюсы or ''}{review_data.минусы or ''}{review_data.поломки or ''}"
            if content:
                review_data.хеш_содержания = hashlib.sha256(content.encode()).hexdigest()
        
        async with self.get_connection() as conn:
            review_id = await conn.fetchval("""
                INSERT INTO auto_reviews.короткие_отзывы (
                    модель_id, автор_id, внешний_id, плюсы, минусы, поломки,
                    год_автомобиля, объем_двигателя, тип_топлива, тип_трансмиссии,
                    тип_привода, количество_фото, город_автора, дата_публикации,
                    хеш_содержания, статус_парсинга
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (модель_id, внешний_id) DO NOTHING
                RETURNING id
            """, review_data.модель_id, author_id, review_data.внешний_id,
                review_data.плюсы, review_data.минусы, review_data.поломки,
                review_data.год_автомобиля, review_data.объем_двигателя,
                review_data.тип_топлива, review_data.тип_трансмиссии,
                review_data.тип_привода, review_data.количество_фото,
                review_data.город_автора, review_data.дата_публикации,
                review_data.хеш_содержания, review_data.статус_парсинга)
            
            if review_id:
                self.logger.debug(f"Короткий отзыв сохранен (ID: {review_id})")
            return review_id
    
    # ==================== АВТОРЫ ====================
    
    async def get_or_create_author(self, псевдоним: str, настоящее_имя: Optional[str] = None, 
                                  город: Optional[str] = None) -> int:
        """Получение или создание автора"""
        async with self.get_connection() as conn:
            # Сначала пытаемся найти существующего автора
            author_id = await conn.fetchval("""
                SELECT id FROM auto_reviews.авторы WHERE псевдоним = $1
            """, псевдоним)
            
            if author_id:
                return author_id
            
            # Если автор не найден, создаем нового
            город_id = None
            if город:
                город_id = await self.get_or_create_city(город)
            
            author_id = await conn.fetchval("""
                INSERT INTO auto_reviews.авторы (псевдоним, настоящее_имя, город_id, дата_регистрации)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                RETURNING id
            """, псевдоним, настоящее_имя, город_id)
            
            self.logger.debug(f"Создан автор: {псевдоним} (ID: {author_id})")
            return author_id
    
    async def get_or_create_city(self, название: str) -> int:
        """Получение или создание города"""
        async with self.get_connection() as conn:
            город_id = await conn.fetchval("""
                INSERT INTO auto_reviews.города (название)
                VALUES ($1)
                ON CONFLICT (название) DO UPDATE SET название = EXCLUDED.название
                RETURNING id
            """, название)
            return город_id
    
    # ==================== СЕССИИ ПАРСИНГА ====================
    
    async def start_parse_session(self, session_data: ParseSessionData) -> int:
        """Начало сессии парсинга"""
        async with self.get_connection() as conn:
            session_id = await conn.fetchval("""
                INSERT INTO auto_reviews.сессии_парсинга (
                    тип_парсинга, бренд_id, модель_id, статус, версия_парсера
                )
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """, session_data.тип_парсинга, session_data.бренд_id,
                session_data.модель_id, session_data.статус, session_data.версия_парсера)
            
            self.logger.info(f"Начата сессия парсинга: {session_data.тип_парсинга} (ID: {session_id})")
            return session_id
    
    async def update_parse_session(self, session_id: int, updates: Dict[str, Any]):
        """Обновление сессии парсинга"""
        async with self.get_connection() as conn:
            set_clauses = []
            values = []
            param_index = 1
            
            for key, value in updates.items():
                set_clauses.append(f"{key} = ${param_index}")
                values.append(value)
                param_index += 1
            
            query = f"""
                UPDATE auto_reviews.сессии_парсинга 
                SET {', '.join(set_clauses)}
                WHERE id = ${param_index}
            """
            values.append(session_id)
            
            await conn.execute(query, *values)
            self.logger.debug(f"Обновлена сессия парсинга {session_id}")
    
    async def finish_parse_session(self, session_id: int, статус: str = 'завершен', 
                                  детали_ошибки: Optional[str] = None):
        """Завершение сессии парсинга"""
        async with self.get_connection() as conn:
            await conn.execute("""
                UPDATE auto_reviews.сессии_парсинга 
                SET статус = $1, конец_парсинга = CURRENT_TIMESTAMP, детали_ошибки = $2
                WHERE id = $3
            """, статус, детали_ошибки, session_id)
            
            self.logger.info(f"Завершена сессия парсинга {session_id} со статусом: {статус}")
    
    # ==================== СТАТИСТИКА ====================
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Получение статистики базы данных"""
        async with self.get_connection() as conn:
            stats = {}
            
            # Основные счетчики
            stats['бренды'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.бренды")
            stats['модели'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.модели")
            stats['полные_отзывы'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.отзывы")
            stats['короткие_отзывы'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.короткие_отзывы")
            stats['авторы'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.авторы")
            stats['города'] = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.города")
            
            # Топ брендов по количеству отзывов
            top_brands = await conn.fetch("""
                SELECT название, количество_отзывов 
                FROM auto_reviews.бренды 
                WHERE количество_отзывов > 0
                ORDER BY количество_отзывов DESC 
                LIMIT 10
            """)
            stats['топ_бренды'] = [dict(row) for row in top_brands]
            
            # Последние сессии парсинга
            recent_sessions = await conn.fetch("""
                SELECT тип_парсинга, статус, начало_парсинга, сохранено_отзывов
                FROM auto_reviews.сессии_парсинга 
                ORDER BY начало_парсинга DESC 
                LIMIT 5
            """)
            stats['последние_сессии'] = [dict(row) for row in recent_sessions]
            
            return stats
    
    # ==================== УТИЛИТЫ ====================
    
    async def bulk_insert_brands(self, brands: List[BrandData]) -> List[int]:
        """Массовая вставка брендов"""
        brand_ids = []
        async with self.get_connection() as conn:
            for brand in brands:
                brand_id = await self.insert_brand(brand)
                brand_ids.append(brand_id)
        
        self.logger.info(f"Массово вставлено брендов: {len(brands)}")
        return brand_ids
    
    async def bulk_insert_models(self, models: List[ModelData]) -> List[int]:
        """Массовая вставка моделей"""
        model_ids = []
        async with self.get_connection() as conn:
            for model in models:
                model_id = await self.insert_model(model)
                model_ids.append(model_id)
        
        self.logger.info(f"Массово вставлено моделей: {len(models)}")
        return model_ids


# Пример использования
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    async def test_extended_manager():
        # Параметры подключения
        connection_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'auto_reviews',
            'user': 'parser',
            'password': 'parser'
        }
        
        # Создаем менеджер
        manager = ExtendedPostgresManager(connection_params)
        
        try:
            await manager.initialize()
            
            # Пример добавления бренда
            brand = BrandData(
                название="Toyota",
                название_в_url="toyota",
                url="https://www.drom.ru/reviews/toyota/",
                количество_отзывов=50000
            )
            brand_id = await manager.insert_brand(brand)
            print(f"Добавлен бренд Toyota с ID: {brand_id}")
            
            # Пример добавления модели
            model = ModelData(
                бренд_id=brand_id,
                название="Camry",
                название_в_url="camry",
                тип_кузова="седан",
                url="https://www.drom.ru/reviews/toyota/camry/"
            )
            model_id = await manager.insert_model(model)
            print(f"Добавлена модель Camry с ID: {model_id}")
            
            # Статистика
            stats = await manager.get_database_stats()
            print("Статистика базы данных:", stats)
            
        finally:
            await manager.close()
    
    # Запуск теста
    asyncio.run(test_extended_manager())
