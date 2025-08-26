#!/usr/bin/env python3
"""
🧪 ПРОСТОЙ ТЕСТ ПАРСИНГА - Toyota Camry короткие отзывы
======================================================
Демонстрация работы парсера на малом объеме данных
"""

import os
import sys
import asyncio
from typing import List, Dict

# Добавляем корневую папку проекта в путь
project_root = "/home/analityk/Документы/projects/parser_project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


async def test_simple_parsing():
    """Простой тест парсинга коротких отзывов"""
    
    print("🚀 ТЕСТ ПАРСИНГА КОРОТКИХ ОТЗЫВОВ")
    print("=" * 50)
    print("🎯 Цель: Toyota Camry, первая страница")
    print("📡 Начинаем парсинг...")
    
    # Инициализируем парсер
    parser = DromReviewsParser()
    
    try:
        # Парсим только первую страницу
        reviews = parser.parse_short_reviews("toyota", "camry", max_pages=1)
        
        if not reviews:
            print("❌ Отзывы не найдены")
            return
            
        print(f"✅ Найдено {len(reviews)} коротких отзывов")
        
        # Показываем первые 3 отзыва
        for i, review in enumerate(reviews[:3], 1):
            print(f"\n📝 Отзыв #{i}:")
            print(f"   👤 Автор: {review.get('author', 'Аноним')}")
            print(f"   📅 Год авто: {review.get('year', 'Не указан')}")
            print(f"   ⚙️  Двигатель: {review.get('engine_volume', 'Не указан')} л")
            print(f"   🔋 Топливо: {review.get('fuel_type', 'Не указано')}")
            print(f"   ⚙️  КПП: {review.get('transmission', 'Не указана')}")
            
            # Показываем плюсы (первые 100 символов)
            positive = review.get('positive_text', '')
            if positive:
                print(f"   ➕ Плюсы: {positive[:100]}{'...' if len(positive) > 100 else ''}")
            
            # Показываем минусы (первые 100 символов)  
            negative = review.get('negative_text', '')
            if negative:
                print(f"   ➖ Минусы: {negative[:100]}{'...' if len(negative) > 100 else ''}")
        
        print(f"\n📊 ИТОГО: получено {len(reviews)} отзывов")
        print("✅ Тест завершен успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка парсинга: {e}")
        import traceback
        print(f"📋 Детали: {traceback.format_exc()}")


def main():
    """Главная функция"""
    print("🔧 Запуск теста парсинга...")
    
    # Запускаем асинхронный тест
    asyncio.run(test_simple_parsing())


if __name__ == "__main__":
    main()
