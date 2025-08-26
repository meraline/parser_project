#!/usr/bin/env python3
"""
🚀 МИНИМАЛЬНАЯ ЗАГРУЗКА HAVAL JOLION
===================================
Обходим проблему с пересозданием схемы
"""

import asyncio
import sys
import os
import logging
from pathlib import Path
import asyncpg

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

try:
    from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser, ModelInfo
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinimalLoader:
    def __init__(self):
        self.connection = None
    
    async def connect(self):
        """Прямое подключение к PostgreSQL"""
        self.connection = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='auto_reviews',
            user='parser',
            password='parser'
        )
        logger.info("✅ Подключение к PostgreSQL установлено")
    
    async def close(self):
        """Закрытие подключения"""
        if self.connection:
            await self.connection.close()
            logger.info("🔌 Подключение закрыто")
    
    async def ensure_brand_exists(self) -> int:
        """Убедиться что бренд Haval существует"""
        # Проверяем существование
        result = await self.connection.fetchrow(
            "SELECT ід FROM auto_reviews.бренды WHERE название_в_url = $1", "haval"
        )
        
        if result:
            logger.info(f"✅ Бренд Haval уже существует с ID: {result['ід']}")
            return result['ід']
        
        # Создаем бренд
        brand_id = await self.connection.fetchval(
            """INSERT INTO auto_reviews.бренды (название, название_в_url, полная_ссылка, количество_отзывов)
               VALUES ($1, $2, $3, $4) RETURNING ід""",
            "Haval", "haval", "https://www.drom.ru/reviews/haval/", 1947
        )
        
        logger.info(f"✅ Бренд Haval создан с ID: {brand_id}")
        return brand_id
    
    async def ensure_model_exists(self, brand_id: int) -> int:
        """Убедиться что модель Jolion существует"""
        # Проверяем существование
        result = await self.connection.fetchrow(
            """SELECT ід FROM auto_reviews.модели 
               WHERE название_в_url = $1 AND ід_бренда = $2""", 
            "jolion", brand_id
        )
        
        if result:
            logger.info(f"✅ Модель Jolion уже существует с ID: {result['ід']}")
            return result['ід']
        
        # Создаем модель
        model_id = await self.connection.fetchval(
            """INSERT INTO auto_reviews.модели (ід_бренда, название, название_в_url, полная_ссылка, количество_отзывов)
               VALUES ($1, $2, $3, $4, $5) RETURNING ід""",
            brand_id, "Jolion", "jolion", "https://www.drom.ru/reviews/haval/jolion/", 315
        )
        
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
            
            print("\n📄 ДЛИННЫЕ ОТЗЫВЫ:")
            print("="*50)
            for i, review in enumerate(long_reviews[:5], 1):
                print(f"{i}. {review.title[:70] if review.title else 'Без заголовка'}...")
                print(f"   Автор: {review.author or 'Не указан'}")
                print(f"   Год: {review.year or 'Не указан'}")
                print(f"   URL: {review.url}")
                if hasattr(review, 'rating') and review.rating:
                    print(f"   Рейтинг: {review.rating}")
                print("-" * 50)
                
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
            
            print("\n💬 КОРОТКИЕ ОТЗЫВЫ:")
            print("="*50)
            for i, review in enumerate(short_reviews[:10], 1):
                print(f"{i}. Автор: {review.author or 'Аноним'}")
                print(f"   Год: {review.year or 'Не указан'}")
                if hasattr(review, 'text') and review.text:
                    print(f"   Текст: {review.text[:100]}...")
                if hasattr(review, 'rating') and review.rating:
                    print(f"   Рейтинг: {review.rating}")
                print("-" * 30)
                
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга коротких отзывов: {e}")
    
    async def show_stats(self):
        """Показать статистику базы данных"""
        logger.info("📊 Статистика базы данных:")
        
        try:
            # Количество брендов
            brands_count = await self.connection.fetchval("SELECT COUNT(*) FROM auto_reviews.бренды")
            
            # Количество моделей
            models_count = await self.connection.fetchval("SELECT COUNT(*) FROM auto_reviews.модели")
            
            # Количество отзывов
            reviews_count = await self.connection.fetchval("SELECT COUNT(*) FROM auto_reviews.отзывы")
            
            # Количество коротких отзывов
            short_reviews_count = await self.connection.fetchval("SELECT COUNT(*) FROM auto_reviews.короткие_отзывы")
            
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
            
            # Подключаемся напрямую
            await self.connect()
            
            # Обеспечиваем наличие бренда и модели
            brand_id = await self.ensure_brand_exists()
            model_id = await self.ensure_model_exists(brand_id)
            
            # Парсим отзывы
            await self.parse_and_show_reviews()
            
            # Показываем статистику
            await self.show_stats()
            
            logger.info("🎉 Демо загрузки завершено!")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            raise
        finally:
            await self.close()

async def main():
    loader = MinimalLoader()
    await loader.run()

if __name__ == "__main__":
    asyncio.run(main())
