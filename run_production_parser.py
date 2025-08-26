#!/usr/bin/env python3
"""
Запуск производственного парсера Drom.ru

Режимы работы:
1. all_brands - парсит все бренды
2. new_brands - проверяет и парсит только новые бренды

Согласно инструкции:
- Парсит все бренды из блока на главной странице
- Для каждого бренда получает модели
- Для каждой модели собирает длинные и короткие отзывы
- Структура URL: https://www.drom.ru/reviews/BRAND/MODEL/ID/
"""

import sys
import json
import logging
from datetime import datetime
from pathlib import Path

# Добавляем путь к модулям
sys.path.append(str(Path(__file__).parent / "src"))

try:
    from src.auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser
except ImportError:
    # Пробуем альтернативный путь
    try:
        from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser
    except ImportError:
        print("Ошибка импорта ProductionDromParser")
        print("Убедитесь, что модуль установлен: pip install -e .")
        sys.exit(1)

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/production_parser.log', encoding='utf-8')
        ]
    )

def save_results(results: dict, mode: str):
    """Сохранение результатов"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/production_parsing_{mode}_{timestamp}.json"
    
    # Создаем директорию если не существует
    Path("data").mkdir(exist_ok=True)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n=== РЕЗУЛЬТАТЫ СОХРАНЕНЫ ===")
    print(f"Файл: {filename}")
    print(f"Размер: {len(json.dumps(results, ensure_ascii=False)) / 1024:.1f} KB")
    
    return filename

def print_summary(results: dict):
    """Вывод сводки результатов"""
    print(f"\n=== СВОДКА РЕЗУЛЬТАТОВ ===")
    print(f"Режим: {results.get('mode', 'unknown')}")
    
    if results['mode'] == 'all_brands':
        print(f"Всего брендов: {results.get('total_brands', 0)}")
        print(f"Обработано брендов: {results.get('processed_brands', 0)}")
        print(f"Всего моделей: {results.get('total_models', 0)}")
        print(f"Длинных отзывов: {results.get('total_long_reviews', 0)}")
        print(f"Коротких отзывов: {results.get('total_short_reviews', 0)}")
        print(f"Всего отзывов: {results.get('total_long_reviews', 0) + results.get('total_short_reviews', 0)}")
        
        # Детали по брендам
        if 'brands_data' in results:
            print(f"\nДетали по брендам:")
            for brand_data in results['brands_data']:
                total_long = sum(m['long_reviews_count'] for m in brand_data['models'])
                total_short = sum(m['short_reviews_count'] for m in brand_data['models'])
                print(f"  {brand_data['brand']}: {brand_data['models_count']} моделей, {total_long}+{total_short} отзывов")
    
    elif results['mode'] == 'new_brands_check':
        print(f"Найдено новых брендов: {results.get('new_brands_found', 0)}")
        if results.get('new_brands_found', 0) > 0:
            print(f"Обработано брендов: {results.get('processed_brands', 0)}")
            print(f"Всего моделей: {results.get('total_models', 0)}")
            print(f"Длинных отзывов: {results.get('total_long_reviews', 0)}")
            print(f"Коротких отзывов: {results.get('total_short_reviews', 0)}")
        else:
            print(f"Сообщение: {results.get('message', 'Нет данных')}")

def run_all_brands_mode():
    """Режим 1: Парсинг всех брендов"""
    print("=== РЕЖИМ: ПАРСИНГ ВСЕХ БРЕНДОВ ===")
    print("Парсим все бренды согласно блока с главной страницы")
    print("Ограничение: первые 5 моделей на бренд для теста")
    print()
    
    parser = ProductionDromParser(delay=1.5)
    
    # Парсим первые несколько брендов для демонстрации
    results = parser.parse_all_brands(
        long_reviews_per_model=3,
        short_reviews_per_model=10
    )
    
    return results

def run_new_brands_mode():
    """Режим 2: Проверка новых брендов"""
    print("=== РЕЖИМ: ПРОВЕРКА НОВЫХ БРЕНДОВ ===")
    print("Проверяем новые бренды и парсим их")
    print()
    
    parser = ProductionDromParser(delay=1.5)
    
    # Файл с известными брендами
    known_brands_file = "data/brand_catalog.json"
    
    results = parser.check_and_parse_new_brands(
        known_brands_file=known_brands_file,
        long_reviews_per_model=3,
        short_reviews_per_model=10
    )
    
    return results

def run_demo_mode():
    """Демо режим: парсинг первых 3 брендов"""
    print("=== ДЕМО РЕЖИМ ===")
    print("Парсим первые 3 бренда для демонстрации")
    print()
    
    parser = ProductionDromParser(delay=1.0)
    
    # Получаем каталог брендов
    brands = parser.get_brands_catalog()
    
    # Берем первые 3 бренда
    demo_brands = brands[:3]
    
    results = {
        'mode': 'demo',
        'total_brands': len(demo_brands),
        'processed_brands': 0,
        'total_models': 0,
        'total_long_reviews': 0,
        'total_short_reviews': 0,
        'brands_data': []
    }
    
    for i, brand in enumerate(demo_brands):
        print(f"[{i+1}/{len(demo_brands)}] Обрабатываем бренд: {brand.name}")
        
        try:
            # Получаем модели бренда
            models = parser.get_models_for_brand(brand)
            
            brand_data = {
                'brand': brand.name,
                'brand_url_name': brand.url_name,
                'models_count': len(models),
                'models': []
            }
            
            # Берем только первые 2 модели для демо
            for model in models[:2]:
                print(f"  Модель: {model.name}")
                
                # Парсим отзывы
                long_reviews = parser.parse_long_reviews(model, 2)
                short_reviews = parser.parse_short_reviews(model, 5)
                
                model_data = {
                    'model': model.name,
                    'model_url_name': model.url_name,
                    'long_reviews_count': len(long_reviews),
                    'short_reviews_count': len(short_reviews),
                    'long_reviews': [review.__dict__ for review in long_reviews],
                    'short_reviews': [review.__dict__ for review in short_reviews]
                }
                
                brand_data['models'].append(model_data)
                results['total_long_reviews'] += len(long_reviews)
                results['total_short_reviews'] += len(short_reviews)
            
            results['brands_data'].append(brand_data)
            results['processed_brands'] += 1
            results['total_models'] += len(models)
            
        except Exception as e:
            print(f"Ошибка при обработке бренда {brand.name}: {e}")
            continue
    
    return results

def main():
    """Главная функция"""
    print("=== ПРОИЗВОДСТВЕННЫЙ ПАРСЕР DROM.RU ===")
    print("Архитектура согласно инструкции:")
    print("1. Парсит бренды из блока на главной странице")
    print("2. Для каждого бренда парсит модели")
    print("3. Для каждой модели собирает длинные и короткие отзывы")
    print("4. Структура URL: https://www.drom.ru/reviews/BRAND/MODEL/ID/")
    print()
    
    # Создаем необходимые директории
    Path("logs").mkdir(exist_ok=True)
    Path("data").mkdir(exist_ok=True)
    
    # Настраиваем логирование
    setup_logging()
    
    # Выбираем режим
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        print("Доступные режимы:")
        print("  demo       - Демо режим (первые 3 бренда)")
        print("  all        - Все бренды")
        print("  new        - Только новые бренды")
        print()
        mode = input("Выберите режим (demo/all/new): ").lower().strip()
    
    # Запускаем соответствующий режим
    try:
        if mode in ['demo', 'd']:
            results = run_demo_mode()
        elif mode in ['all', 'a', 'all_brands']:
            results = run_all_brands_mode()
        elif mode in ['new', 'n', 'new_brands']:
            results = run_new_brands_mode()
        else:
            print(f"Неизвестный режим: {mode}")
            print("Используйте: demo, all или new")
            return
        
        # Выводим сводку
        print_summary(results)
        
        # Сохраняем результаты
        filename = save_results(results, mode)
        
        print(f"\n=== ЗАВЕРШЕНО УСПЕШНО ===")
        print(f"Результаты сохранены в: {filename}")
        
    except KeyboardInterrupt:
        print("\n=== ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ ===")
    except Exception as e:
        print(f"\n=== ОШИБКА ===")
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
