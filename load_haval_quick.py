#!/usr/bin/env python3
"""
🚀 БЫСТРАЯ ЗАГРУЗКА HAVAL JOLION БЕЗ ПЕРЕСОЗДАНИЯ СХЕМЫ
========================================================
"""

import asyncio
import sys
import os
import logging
from pathlib import Path
from datetime import datetime

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

try:
    from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo
    from auto_reviews_parser.database.extended_postgres_manager import (
        ExtendedPostgresManager, BrandData, ModelData
    )
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleDataLoader:
    def __init__(self):
        self.db_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'auto_reviews',
            'user': 'parser',
            'password': 'parser'
        }
        self.db_manager = ExtendedPostgresManager(self.db_params)
    
    async def connect_only(self):
        """Подключиться без создания схемы"""
        await self.db_manager.initialize_connection_only()
        logger.info("✅ Подключение к PostgreSQL установлено")
    
    async def ensure_brand_exists(self) -> int:
        """Убедиться что бренд Haval существует"""
        async with self.db_manager.get_connection() as conn:
            # Проверяем существование
            result = await conn.fetchrow(
                "SELECT id FROM auto_reviews.бренды WHERE название_в_url = $1", "haval"
            )
            
            if result:
                logger.info(f"✅ Бренд Haval уже существует с ID: {result['id']}")
                return result['id']
            
            # Создаем бренд
            brand_data = BrandData(
                название="Haval",
                название_в_url="haval", 
                url="https://www.drom.ru/reviews/haval/",
                количество_отзывов=1947
            )
            
            brand_id = await self.db_manager.insert_brand(brand_data)
            logger.info(f"✅ Бренд Haval создан с ID: {brand_id}")
            return brand_id
    
    async def ensure_model_exists(self, brand_id: int) -> int:
        """Убедиться что модель Jolion существует"""
        async with self.db_manager.get_connection() as conn:
            # Проверяем существование
            result = await conn.fetchrow(
                """SELECT id FROM auto_reviews.модели 
                   WHERE название_в_url = $1 AND бренд_id = $2""", 
                "jolion", brand_id
            )
            
            if result:
                logger.info(f"✅ Модель Jolion уже существует с ID: {result['id']}")
                return result['id']
            
            # Создаем модель
            model_data = ModelData(
                бренд_id=brand_id,
                название="Jolion",
                название_в_url="jolion",
                url="https://www.drom.ru/reviews/haval/jolion/",
                количество_отзывов=315
            )
            
            model_id = await self.db_manager.insert_model(model_data)
            logger.info(f"✅ Модель Jolion создана с ID: {model_id}")
            return model_id
    
    async def parse_and_show_reviews(self):
        """Парсим отзывы и показываем результат"""
        parser = MasterDromParser()
        
        # Парсим длинные отзывы
        logger.info("📄 Парсим длинные отзывы...")
        try:
            model_info = ModelInfo(
                name="Jolion",
                brand="Haval",
                url="https://www.drom.ru/reviews/haval/jolion/"
            )
            
            long_reviews = parser.parse_long_reviews(model_info, limit=5)
            logger.info(f"📄 Найдено {len(long_reviews)} длинных отзывов")
            
            for i, review in enumerate(long_reviews[:3], 1):
                logger.info(f"📄 Отзыв {i}: {review.title[:50] if review.title else 'Без заголовка'}...")
                
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга длинных отзывов: {e}")
        
        # Парсим короткие отзывы
        logger.info("💬 Парсим короткие отзывы...")
        try:
            short_model_info = ModelInfo(
                name="Jolion",
                brand="Haval",
                url="https://www.drom.ru/reviews/haval/jolion/5kopeek/"
            )
            
            short_reviews = parser.parse_short_reviews(short_model_info, limit=10)
            logger.info(f"💬 Найдено {len(short_reviews)} коротких отзывов")
            
            for i, review in enumerate(short_reviews[:3], 1):
                logger.info(f"💬 Короткий отзыв {i}: {review.author or 'Аноним'} - {review.year or 'Год не указан'}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга коротких отзывов: {e}")
    
    async def show_stats(self):
        """Показать статистику базы данных"""
        logger.info("📊 Статистика базы данных:")
        
        try:
            async with self.db_manager.get_connection() as conn:
                # Количество брендов
                brands_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.бренды")
                
                # Количество моделей
                models_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.модели")
                
                # Количество отзывов
                reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.отзывы")
                
                # Количество коротких отзывов
                short_reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.короткие_отзывы")
                
                print("\n" + "="*50)
                print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ")
                print("="*50)
                print(f"🏢 Бренды: {brands_count}")
                print(f"🚗 Модели: {models_count}")
                print(f"📄 Отзывы: {reviews_count}")
                print(f"💬 Короткие отзывы: {short_reviews_count}")
                print("="*50)
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
    
    async def run(self):
        """Основная функция"""
        try:
            logger.info("🚀 Загрузка тестовых данных Haval Jolion...")
            
            # Подключаемся без пересоздания схемы
            await self.connect_only()
            
            # Обеспечиваем наличие бренда и модели
            brand_id = await self.ensure_brand_exists()
            model_id = await self.ensure_model_exists(brand_id)
            
            # Парсим отзывы
            await self.parse_and_show_reviews()
            
            # Показываем статистику
            await self.show_stats()
            
            logger.info("🎉 Тестовая загрузка завершена!")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            await self.db_manager.close()

async def main():
    loader = SimpleDataLoader()
    await loader.run()

if __name__ == "__main__":
    asyncio.run(main())
