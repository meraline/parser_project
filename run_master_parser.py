#!/usr/bin/env python3
"""
🚗 ЗАПУСК МАСТЕР-ПАРСЕРА DROM.RU
=================================

Скрипт для запуска объединенного мастер-парсера
с настройками согласно задаче:
- Первые 3 бренда
- По 3 длинных отзыва на бренд
- По 10 коротких отзывов на бренд

Автор: AI Assistant
Дата: 26.08.2025
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from src.auto_reviews_parser.parsers.master_drom_parser import MasterDromParser
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что находитесь в корне проекта")
    sys.exit(1)


def main():
    """Основная функция запуска"""
    
    print("🚗 МАСТЕР-ПАРСЕР DROM.RU")
    print("=" * 50)
    print("Задача: 3 бренда × 3 длинных + 10 коротких отзывов")
    print()
    
    start_time = time.time()
    
    try:
        # Инициализация мастер-парсера
        print("📦 Инициализация мастер-парсера...")
        parser = MasterDromParser(
            delay=1.0,              # 1 секунда между запросами
            cache_dir="data/cache", # Кэш для ускорения повторных запросов
            enable_database=False,  # Отключаем БД для демо
            enable_cache=True       # Включаем кэширование
        )
        print("✅ Мастер-парсер инициализирован")
        print()
        
        # Запуск парсинга согласно заданию
        print("🚀 Запуск парсинга...")
        results = parser.parse_limited_demo(
            max_brands=3,           # Первые 3 бренда
            max_long_reviews=3,     # По 3 длинных отзыва
            max_short_reviews=10    # По 10 коротких отзывов
        )
        
        # Выводим подробные результаты
        print("\n🎯 РЕЗУЛЬТАТЫ ПАРСИНГА:")
        print("=" * 50)
        
        print(f"📊 Общая статистика:")
        print(f"  • Время выполнения: {results.get('duration_seconds', 0):.1f} сек")
        print(f"  • Всего отзывов: {results['total_reviews']}")
        print(f"  • Длинных отзывов: {results['total_long_reviews']}")
        print(f"  • Коротких отзывов: {results['total_short_reviews']}")
        print()
        
        print(f"🏭 Обработанные бренды ({len(results['brands_processed'])}):")
        for i, brand_stat in enumerate(results['brands_processed'], 1):
            print(f"  {i}. {brand_stat['brand']} - {brand_stat['model']}")
            print(f"     Длинных: {brand_stat['long_reviews_parsed']}/{brand_stat['long_reviews_available']}")
            print(f"     Коротких: {brand_stat['short_reviews_parsed']}/{brand_stat['short_reviews_available']}")
            print(f"     Итого: {brand_stat['total_parsed']} отзывов")
            print()
        
        if results.get('saved_to'):
            print(f"💾 Результаты сохранены в: {results['saved_to']}")
            
        if results['errors']:
            print(f"\n⚠️  Ошибки ({len(results['errors'])}):")
            for error in results['errors']:
                print(f"  • {error}")
        
        # Статистика парсера
        print(f"\n🔧 Настройки парсера:")
        stats = parser.get_statistics()
        for key, value in stats.items():
            print(f"  • {key}: {value}")
        
        print(f"\n✅ Парсинг успешно завершен!")
        
    except KeyboardInterrupt:
        print("\n\n❌ Парсинг прерван пользователем")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        total_time = time.time() - start_time
        print(f"\n⏱️  Общее время работы: {total_time:.1f} секунд")


if __name__ == "__main__":
    main()
