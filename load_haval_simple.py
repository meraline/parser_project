#!/usr/bin/env python3
"""
🚀 ПРОСТАЯ ЗАГРУЗКА ДАННЫХ HAVAL JOLION В POSTGRESQL
==================================================
Упрощенный скрипт для быстрой загрузки тестовых данных
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

async def main():
    """Основная функция"""
    logger.info("🚀 Загрузка тестовых данных Haval Jolion...")
    
    # Параметры подключения к БД
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'auto_reviews',
        'user': 'parser',
        'password': 'parser'
    }
    
    db_manager = ExtendedPostgresManager(db_params)
    
    try:
        # Подключаемся к базе
        await db_manager.initialize()
        logger.info("✅ Подключение к PostgreSQL установлено")
        
        # 1. Добавляем бренд Haval
        logger.info("📋 Добавляем бренд Haval...")
        brand_data = BrandData(
            название="Haval",
            название_в_url="haval", 
            url="https://www.drom.ru/reviews/haval/",
            количество_отзывов=1947
        )
        
        try:
            brand_id = await db_manager.insert_brand(brand_data)
            logger.info(f"✅ Бренд Haval добавлен с ID: {brand_id}")
        except Exception as e:
            logger.info(f"ℹ️ Бренд уже существует или ошибка: {e}")
            # Получаем существующий ID
            async with db_manager.get_connection() as conn:
                result = await conn.fetchrow(
                    "SELECT id FROM auto_reviews.бренды WHERE название_в_url = $1", "haval"
                )
                brand_id = result['id'] if result else None
                logger.info(f"📋 Использую существующий ID бренда: {brand_id}")
        
        if not brand_id:
            logger.error("❌ Не удалось получить ID бренда")
            return
            
        # 2. Добавляем модель Jolion
        logger.info("🚗 Добавляем модель Jolion...")
        model_data = ModelData(
            бренд_id=brand_id,
            название="Jolion",
            название_в_url="jolion",
            url="https://www.drom.ru/reviews/haval/jolion/",
            количество_отзывов=315
        )
        
        try:
            model_id = await db_manager.insert_model(model_data)
            logger.info(f"✅ Модель Jolion добавлена с ID: {model_id}")
        except Exception as e:
            logger.info(f"ℹ️ Модель уже существует или ошибка: {e}")
            # Получаем существующий ID
            async with db_manager.get_connection() as conn:
                result = await conn.fetchrow(
                    """SELECT id FROM auto_reviews.модели 
                       WHERE название_в_url = $1 AND бренд_id = $2""", 
                    "jolion", brand_id
                )
                model_id = result['id'] if result else None
                logger.info(f"🚗 Использую существующий ID модели: {model_id}")
        
        if not model_id:
            logger.error("❌ Не удалось получить ID модели")
            return
            
        # 3. Парсим и загружаем данные
        logger.info("📄 Начинаем парсинг отзывов...")
        parser = MasterDromParser()
        
        # Создаем объект модели для парсера
        model_info = ModelInfo(
            name="Jolion",
            brand="Haval",
            url="https://www.drom.ru/reviews/haval/jolion/"
        )
        
        # Парсим длинные отзывы (первые 5)
        logger.info("📄 Парсим длинные отзывы...")
        try:
            long_reviews = parser.parse_long_reviews(model_info, limit=5)
            logger.info(f"📄 Найдено {len(long_reviews)} длинных отзывов")
            
            # Здесь можно добавить код для сохранения отзывов
            # Пока просто показываем что получили
            for i, review in enumerate(long_reviews[:3], 1):
                logger.info(f"📄 Отзыв {i}: {review.title[:50] if review.title else 'Без заголовка'}...")
                
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга длинных отзывов: {e}")
        
        # Парсим короткие отзывы (первые 10)
        logger.info("💬 Парсим короткие отзывы...")
        try:
            # Создаем URL для коротких отзывов
            short_model_info = ModelInfo(
                name="Jolion",
                brand="Haval",
                url="https://www.drom.ru/reviews/haval/jolion/5kopeek/"
            )
            short_reviews = parser.parse_short_reviews(short_model_info, limit=10)
            logger.info(f"💬 Найдено {len(short_reviews)} коротких отзывов")
            
            # Показываем первые несколько
            for i, review in enumerate(short_reviews[:3], 1):
                logger.info(f"💬 Короткий отзыв {i}: {review.author or 'Аноним'} - {review.year or 'Год не указан'}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга коротких отзывов: {e}")
        
        # 4. Показываем статистику
        await show_database_stats(db_manager)
        
        logger.info("🎉 Тестовая загрузка завершена!")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
    finally:
        await db_manager.close()

async def show_database_stats(db_manager):
    """Показать статистику базы данных"""
    logger.info("📊 Статистика базы данных:")
    
    try:
        async with db_manager.get_connection() as conn:
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

if __name__ == "__main__":
    asyncio.run(main())
