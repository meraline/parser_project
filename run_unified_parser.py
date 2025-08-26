#!/usr/bin/env python3
"""
🚗 ЗАПУСК ЕДИНОГО ПАРСЕРА DROM.RU
===============================

Главный скрипт для запуска парсинга отзывов.
Больше не создаем новых парсеров - используем только этот!

Автор: AI Assistant  
Дата: 26.08.2025
"""

import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from auto_reviews_parser.parsers.unified_master_parser import UnifiedDromParser
import json
from datetime import datetime


def main():
    """Запуск парсинга 3 моделей с 3 длинными и 10 короткими отзывами"""
    
    print("🚗 ЕДИНЫЙ ПАРСЕР ОТЗЫВОВ DROM.RU - МАСТЕР-ВЕРСИЯ")
    print("=" * 60)
    print("📋 Задача: Парсинг первых 3 моделей")
    print("📝 Длинных отзывов на модель: 3")
    print("💬 Коротких отзывов на модель: 10")
    print("⏰ Время запуска:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("-" * 60)
    
    try:
        # Создаем единый парсер
        parser = UnifiedDromParser(delay=1.0)
        
        print("🚀 Инициализация парсера завершена")
        print("🎯 Начинаем парсинг...")
        print()
        
        # Запускаем парсинг
        results = parser.parse_multiple_models(
            models_limit=3,      # 3 модели  
            long_limit=3,        # 3 длинных отзыва на модель
            short_limit=10       # 10 коротких отзывов на модель
        )
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"unified_parsing_results_{timestamp}.json"
        filepath = parser.save_results(results, filename)
        
        print("\n" + "=" * 60)
        print("🎉 ПАРСИНГ УСПЕШНО ЗАВЕРШЕН!")
        print("=" * 60)
        
        summary = results['summary']
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"   🚗 Моделей обработано: {summary['models_processed']}")
        print(f"   📝 Длинных отзывов: {summary['total_long_reviews']}")
        print(f"   💬 Коротких отзывов: {summary['total_short_reviews']}")
        print(f"   📋 Всего отзывов: {summary['total_reviews']}")
        print(f"   💾 Файл результатов: {filepath}")
        
        # Показываем детали по моделям
        print(f"\n📋 ДЕТАЛИ ПО МОДЕЛЯМ:")
        for i, model_data in enumerate(results['models'], 1):
            brand = model_data['brand']
            model = model_data['model']
            long_count = model_data['stats']['long_reviews_count']
            short_count = model_data['stats']['short_reviews_count']
            total_count = model_data['stats']['total_reviews']
            
            print(f"   {i}. {brand.upper()}/{model.upper()}: "
                  f"{long_count} длинных + {short_count} коротких = {total_count} всего")
        
        # Показываем примеры данных
        print(f"\n📄 ПРИМЕРЫ ИЗВЛЕЧЕННЫХ ДАННЫХ:")
        
        for model_data in results['models'][:2]:  # Показываем первые 2 модели
            brand = model_data['brand']
            model = model_data['model']
            
            print(f"\n🚗 {brand.upper()}/{model.upper()}:")
            
            # Пример длинного отзыва
            if model_data['long_reviews']:
                long_review = model_data['long_reviews'][0]
                print(f"   📝 Длинный отзыв:")
                print(f"      - ID: {long_review['review_id']}")
                print(f"      - Автор: {long_review['author'] or 'Не указан'}")
                print(f"      - Год авто: {long_review['year'] or 'Не указан'}")
                print(f"      - Объем: {long_review['engine_volume'] or 'Не указан'}")
                print(f"      - Топливо: {long_review['fuel_type'] or 'Не указано'}")
                print(f"      - КПП: {long_review['transmission'] or 'Не указана'}")
                if long_review['positive']:
                    preview = long_review['positive'][:100] + "..." if len(long_review['positive']) > 100 else long_review['positive']
                    print(f"      - Плюсы: {preview}")
            
            # Пример короткого отзыва  
            if model_data['short_reviews']:
                short_review = model_data['short_reviews'][0]
                print(f"   💬 Короткий отзыв:")
                print(f"      - ID: {short_review['review_id']}")
                print(f"      - Автор: {short_review['author'] or 'Не указан'}")
                print(f"      - Год авто: {short_review['year'] or 'Не указан'}")
                print(f"      - Город: {short_review['city'] or 'Не указан'}")
                if short_review['positive']:
                    preview = short_review['positive'][:100] + "..." if len(short_review['positive']) > 100 else short_review['positive']
                    print(f"      - Отзыв: {preview}")
        
        # Статистика парсера
        stats = results['stats']
        print(f"\n📈 СТАТИСТИКА ПАРСЕРА:")
        print(f"   🌐 Всего запросов: {stats['total_requests']}")
        print(f"   ✅ Успешных запросов: {stats['successful_requests']}")
        print(f"   ❌ Неудачных запросов: {stats['failed_requests']}")
        print(f"   🏭 Брендов обработано: {stats['brands_processed']}")
        
        success_rate = (stats['successful_requests'] / stats['total_requests'] * 100) if stats['total_requests'] > 0 else 0
        print(f"   📊 Успешность: {success_rate:.1f}%")
        
        print(f"\n✅ ВСЕ ДАННЫЕ УСПЕШНО ИЗВЛЕЧЕНЫ И СОХРАНЕНЫ!")
        print(f"💡 Файл с результатами: {filepath}")
        print("🔄 Больше не создаем новых парсеров - развиваем этот!")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПАРСИНГА: {e}")
        print(f"📊 Проверьте логи в файле: logs/unified_parser.log")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
