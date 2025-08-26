#!/usr/bin/env python3
"""
🚀 ЗАГРУЗКА ДАННЫХ HAVAL JOLION В POSTGRESQL
===========================================
Скрипт для загрузки первых 5 длинных отзывов и 10 коротких отзывов 
Haval Jolion в базу данных PostgreSQL.
"""

import asyncio
import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

try:
    from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo, ReviewData
    from auto_reviews_parser.database.extended_postgres_manager import (
        ExtendedPostgresManager, BrandData, ModelData, FullReviewData, ShortReviewData
    )
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Проверьте установку зависимостей: pip install asyncpg")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HavalJolionDataLoader:
    """Загрузчик данных Haval Jolion в PostgreSQL"""
    
    def __init__(self):
        self.db_params = {
            'host': os.getenv('DATABASE_HOST', 'localhost'),
            'port': int(os.getenv('DATABASE_PORT', '5432')),
            'database': os.getenv('DATABASE_NAME', 'auto_reviews'),
            'user': os.getenv('DATABASE_USER', 'parser'),
            'password': os.getenv('DATABASE_PASSWORD', 'parser')
        }
        self.db_manager = ExtendedPostgresManager(self.db_params)
        self.parser = MasterDromParser()
        
    async def load_all_data(self):
        """Загрузка всех данных"""
        logger.info("🚀 Начинаем загрузку данных Haval Jolion в PostgreSQL...")
        
        try:
            # Подключаемся к базе данных
            await self.db_manager.initialize()
            logger.info("✅ Подключение к PostgreSQL установлено")
            
            # 1. Создаем/обновляем бренд Haval
            await self.ensure_brand_exists()
            
            # 2. Создаем/обновляем модель Jolion
            await self.ensure_model_exists()
            
            # 3. Загружаем длинные отзывы (первые 5)
            await self.load_long_reviews()
            
            # 4. Загружаем короткие отзывы (первые 10)
            await self.load_short_reviews()
            
            # 5. Показываем статистику
            await self.show_statistics()
            
            logger.info("🎉 Загрузка данных завершена успешно!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке данных: {e}")
            raise
        finally:
            await self.db_manager.close()
            
    async def ensure_brand_exists(self):
        """Проверяем/создаем бренд Haval"""
        logger.info("📋 Проверяем бренд Haval...")
        
        brand_data = BrandData(
            название="Haval",
            название_в_url="haval",
            url="https://www.drom.ru/reviews/haval/",
            количество_отзывов=1947
        )
        
        brand_id = await self.db_manager.insert_brand(brand_data)
        logger.info(f"✅ Бренд Haval сохранен с ID: {brand_id}")
        return brand_id
        
    async def ensure_model_exists(self):
        """Проверяем/создаем модель Jolion"""
        logger.info("🚗 Проверяем модель Jolion...")
        
        # Сначала получаем ID бренда
        async with self.db_manager.get_connection() as conn:
            brand_result = await conn.fetchrow(
                "SELECT id FROM auto_reviews.бренды WHERE name_en = $1", "haval"
            )
            if not brand_result:
                raise ValueError("Бренд Haval не найден!")
            brand_id = brand_result['id']
        
        model_data = ModelData(
            brand_id=brand_id,
            name="Jolion",
            name_en="jolion",
            url="https://www.drom.ru/reviews/haval/jolion/",
            generation="",
            review_count=315,
            short_review_count=0,
            last_updated=datetime.now()
        )
        
        model_id = await self.db_manager.save_model(model_data)
        logger.info(f"✅ Модель Jolion сохранена с ID: {model_id}")
        return model_id
        
    async def load_long_reviews(self):
        """Загрузка первых 5 длинных отзывов"""
        logger.info("📄 Загружаем длинные отзывы Haval Jolion...")
        
        try:
            # Парсим страницу с длинными отзывами
            url = "https://www.drom.ru/reviews/haval/jolion/"
            reviews_data = await self.parser.parse_reviews(url, max_reviews=5)
            
            if not reviews_data or not reviews_data.get('reviews'):
                logger.warning("⚠️ Не удалось получить длинные отзывы")
                return
                
            reviews = reviews_data['reviews'][:5]  # Берем первые 5
            logger.info(f"📄 Найдено {len(reviews)} длинных отзывов для загрузки")
            
            # Получаем ID модели
            async with self.db_manager.get_connection() as conn:
                model_result = await conn.fetchrow(
                    """SELECT m.id, b.id as brand_id 
                       FROM auto_reviews.модели m 
                       JOIN auto_reviews.бренды b ON m.brand_id = b.id 
                       WHERE m.name_en = $1 AND b.name_en = $2""", 
                    "jolion", "haval"
                )
                if not model_result:
                    raise ValueError("Модель Jolion не найдена!")
                model_id = model_result['id']
                brand_id = model_result['brand_id']
            
            # Сохраняем отзывы
            saved_count = 0
            for review in reviews:
                try:
                    # Преобразуем данные отзыва в формат ReviewData
                    review_data = ReviewData(
                        brand_id=brand_id,
                        model_id=model_id,
                        url=review.get('url', ''),
                        title=review.get('title', ''),
                        content=review.get('content', ''),
                        author=review.get('author', ''),
                        rating=float(review.get('rating', 0)) if review.get('rating') else None,
                        pros=review.get('pros', ''),
                        cons=review.get('cons', ''),
                        year=review.get('car_characteristics', {}).get('year'),
                        mileage=self._parse_mileage(review.get('car_characteristics', {}).get('mileage')),
                        engine_volume=self._parse_engine_volume(review.get('car_characteristics', {}).get('engine', '')),
                        fuel_type=self._extract_fuel_type(review.get('car_characteristics', {}).get('engine', '')),
                        transmission=review.get('car_characteristics', {}).get('transmission'),
                        body_type=review.get('car_characteristics', {}).get('body_type'),
                        drive_type=review.get('car_characteristics', {}).get('drive_type'),
                        acquisition_year=review.get('car_characteristics', {}).get('acquisition_year'),
                        publish_date=review.get('publish_date'),
                        views_count=review.get('views_count'),
                        photos_count=len(review.get('photos', [])),
                        content_hash=str(hash(review.get('content', ''))),
                        parsed_at=datetime.now(),
                        source="drom.ru",
                        review_type="long"
                    )
                    
                    review_id = await self.db_manager.save_review(review_data)
                    saved_count += 1
                    logger.info(f"✅ Сохранен длинный отзыв ID: {review_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения отзыва: {e}")
                    continue
                    
            logger.info(f"✅ Загружено {saved_count} длинных отзывов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки длинных отзывов: {e}")
            
    async def load_short_reviews(self):
        """Загрузка первых 10 коротких отзывов"""
        logger.info("💬 Загружаем короткие отзывы Haval Jolion...")
        
        try:
            # Парсим страницу с короткими отзывами
            url = "https://www.drom.ru/reviews/haval/jolion/5kopeek/"
            short_reviews_data = await self.parser.parse_short_reviews(url, max_reviews=10)
            
            if not short_reviews_data or not short_reviews_data.get('reviews'):
                logger.warning("⚠️ Не удалось получить короткие отзывы")
                return
                
            short_reviews = short_reviews_data['reviews'][:10]  # Берем первые 10
            logger.info(f"💬 Найдено {len(short_reviews)} коротких отзывов для загрузки")
            
            # Получаем ID модели
            async with self.db_manager.get_connection() as conn:
                model_result = await conn.fetchrow(
                    """SELECT m.id, b.id as brand_id 
                       FROM auto_reviews.модели m 
                       JOIN auto_reviews.бренды b ON m.brand_id = b.id 
                       WHERE m.name_en = $1 AND b.name_en = $2""", 
                    "jolion", "haval"
                )
                if not model_result:
                    raise ValueError("Модель Jolion не найдена!")
                model_id = model_result['id']
                brand_id = model_result['brand_id']
            
            # Сохраняем короткие отзывы
            saved_count = 0
            for review in short_reviews:
                try:
                    short_review_data = ShortReviewData(
                        brand_id=brand_id,
                        model_id=model_id,
                        author=review.get('author', ''),
                        year=review.get('year'),
                        engine_volume=self._parse_engine_volume(review.get('engine', '')),
                        fuel_type=self._extract_fuel_type(review.get('engine', '')),
                        transmission=review.get('transmission'),
                        drive_type=review.get('drive_type'),
                        city=review.get('city', ''),
                        positive=review.get('positive', ''),
                        negative=review.get('negative', ''),
                        breakages=review.get('breakages', ''),
                        publish_date=review.get('publish_date'),
                        content_hash=str(hash(f"{review.get('positive', '')}{review.get('negative', '')}")),
                        parsed_at=datetime.now(),
                        source="drom.ru"
                    )
                    
                    short_review_id = await self.db_manager.save_short_review(short_review_data)
                    saved_count += 1
                    logger.info(f"✅ Сохранен короткий отзыв ID: {short_review_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка сохранения короткого отзыва: {e}")
                    continue
                    
            logger.info(f"✅ Загружено {saved_count} коротких отзывов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки коротких отзывов: {e}")
            
    async def show_statistics(self):
        """Показать статистику загруженных данных"""
        logger.info("📊 Получаем статистику базы данных...")
        
        try:
            async with self.db_manager.get_connection() as conn:
                # Статистика по брендам
                brands_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.бренды")
                
                # Статистика по моделям
                models_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.модели")
                
                # Статистика по длинным отзывам
                long_reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.отзывы")
                
                # Статистика по коротким отзывам  
                short_reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.короткие_отзывы")
                
                # Статистика по Haval Jolion
                haval_jolion_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(r.id) as long_reviews,
                        COUNT(sr.id) as short_reviews
                    FROM auto_reviews.модели m
                    JOIN auto_reviews.бренды b ON m.brand_id = b.id
                    LEFT JOIN auto_reviews.отзывы r ON r.model_id = m.id
                    LEFT JOIN auto_reviews.короткие_отзывы sr ON sr.model_id = m.id
                    WHERE b.name_en = 'haval' AND m.name_en = 'jolion'
                """)
                
                print("\n" + "="*60)
                print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ")
                print("="*60)
                print(f"🏢 Всего брендов: {brands_count}")
                print(f"🚗 Всего моделей: {models_count}")
                print(f"📄 Всего длинных отзывов: {long_reviews_count}")
                print(f"💬 Всего коротких отзывов: {short_reviews_count}")
                print()
                print("🎯 СТАТИСТИКА HAVAL JOLION:")
                print(f"📄 Длинные отзывы: {haval_jolion_stats['long_reviews'] if haval_jolion_stats else 0}")
                print(f"💬 Короткие отзывы: {haval_jolion_stats['short_reviews'] if haval_jolion_stats else 0}")
                print("="*60)
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            
    def _parse_mileage(self, mileage_str: str) -> int:
        """Парсинг пробега из строки"""
        if not mileage_str:
            return None
        try:
            # Извлекаем числа из строки
            import re
            numbers = re.findall(r'\d+', str(mileage_str).replace(' ', ''))
            if numbers:
                return int(numbers[0])
        except:
            pass
        return None
        
    def _parse_engine_volume(self, engine_str: str) -> float:
        """Парсинг объема двигателя из строки"""
        if not engine_str:
            return None
        try:
            import re
            # Ищем паттерн вроде "1.5" или "2.0"
            volume_match = re.search(r'(\d+\.?\d*)\s*(?:л|л\.|куб)', str(engine_str))
            if volume_match:
                return float(volume_match.group(1))
        except:
            pass
        return None
        
    def _extract_fuel_type(self, engine_str: str) -> str:
        """Извлечение типа топлива из строки двигателя"""
        if not engine_str:
            return None
        engine_lower = str(engine_str).lower()
        if 'бензин' in engine_lower or 'gasoline' in engine_lower:
            return 'бензин'
        elif 'дизель' in engine_lower or 'diesel' in engine_lower:
            return 'дизель'
        elif 'электр' in engine_lower or 'electric' in engine_lower:
            return 'электричество'
        elif 'гибрид' in engine_lower or 'hybrid' in engine_lower:
            return 'гибрид'
        return None


async def main():
    """Основная функция"""
    loader = HavalJolionDataLoader()
    await loader.load_all_data()


if __name__ == "__main__":
    asyncio.run(main())
