#!/usr/bin/env python3
"""
🚀 ЗАПУСК МАСТЕР-ПАРСЕРА DROM.RU
===============================

Скрипт для запуска простого мастер-парсера
Объединяет всю лучшую логику из всех парсеров проекта

"""

import sys
import os
import json
from pathlib import Path

# Добавляем путь к проекту
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

try:
    from src.auto_reviews_parser.parsers.simple_master_parser import MasterDromParser
except ImportError:
    # Попробуем прямой импорт
    try:
        from auto_reviews_parser.parsers.simple_master_parser import MasterDromParser
    except ImportError:
        # Последний вариант
        try:
            sys.path.insert(0, str(project_root / "src" / "auto_reviews_parser" / "parsers"))
            from simple_master_parser import MasterDromParser
        except ImportError as e:
            print(f"❌ Ошибка импорта: {e}")
            print("Убедитесь что файл simple_master_parser.py находится в правильной директории")
            sys.exit(1)


def main():
    """Главная функция запуска"""
    print("🚗 МАСТЕР-ПАРСЕР DROM.RU - ЗАПУСК")
    print("=" * 50)
    
    try:
        # Инициализация парсера
        print("🔧 Инициализация мастер-парсера...")
        parser = MasterDromParser(
            delay=1.0,
            cache_dir="data/cache",
            enable_cache=True
        )
        
        print("✅ Мастер-парсер инициализирован")
        
        # Запуск демо-парсинга
        print("\n🚀 Запуск демо-парсинга...")
        print("  • 3 бренда")
        print("  • до 3 длинных отзывов на бренд")
        print("  • до 10 коротких отзывов на бренд")
        print()
        
        results = parser.parse_limited_demo(
            max_brands=3,
            max_long_reviews=3,
            max_short_reviews=10
        )
        
        # Выводим результаты
        print("\n" + "=" * 50)
        print("🎉 РЕЗУЛЬТАТЫ ПАРСИНГА:")
        print("=" * 50)
        
        print(f"📊 Обработано брендов: {len(results['brands_processed'])}")
        print(f"📝 Всего отзывов: {results['total_reviews']}")
        print(f"📄 Длинных отзывов: {results['total_long_reviews']}")
        print(f"💭 Коротких отзывов: {results['total_short_reviews']}")
        print(f"⏱️  Время выполнения: {results.get('duration_seconds', 0):.1f} сек")
        
        if 'saved_to' in results:
            print(f"💾 Результаты сохранены: {results['saved_to']}")
        
        # Детали по брендам
        if results['brands_processed']:
            print("\n📋 Детали по брендам:")
            for brand_data in results['brands_processed']:
                print(f"  🏭 {brand_data['brand']} - {brand_data['model']}")
                print(f"     📄 Длинных: {brand_data['long_reviews_parsed']}/{brand_data['long_reviews_available']}")
                print(f"     💭 Коротких: {brand_data['short_reviews_parsed']}/{brand_data['short_reviews_available']}")
                print(f"     📊 Всего: {brand_data['total_parsed']}")
        
        # Статистика парсера
        stats = parser.get_statistics()
        print("\n📈 Статистика сети:")
        print(f"  🌐 Запросов выполнено: {stats['requests_made']}")
        print(f"  📦 Запросов из кэша: {stats['requests_cached']}")
        print(f"  ❌ Запросов с ошибкой: {stats['requests_failed']}")
        print(f"  📝 Отзывов распарсено: {stats['reviews_parsed']}")
        
        # Ошибки
        if results['errors']:
            print(f"\n⚠️  ОШИБКИ ({len(results['errors'])}):")
            for i, error in enumerate(results['errors'], 1):
                print(f"  {i}. {error}")
        
        print("\n✅ Парсинг завершен успешно!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Парсинг прерван пользователем")
        return 1
        
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
