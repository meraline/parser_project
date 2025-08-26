#!/usr/bin/env python3
"""
Тест исправленного production_drom_parser.py
Парсим первые 3 бренда по 3 длинных и 10 коротких отзывов
"""

import os
import sys
import logging
from pathlib import Path
import json
from datetime import datetime

# Добавляем src в путь
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from auto_reviews_parser.parsers.production_drom_parser import ProductionDromParser

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('test_production_parser.log'),
            logging.StreamHandler()
        ]
    )

def test_production_parser():
    """Тест production parser с ограничениями"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Создаем parser
    parser = ProductionDromParser()
    
    # Получаем список брендов
    logger.info("Получение списка брендов...")
    brands = parser.get_brands()
    
    if not brands:
        logger.error("Не удалось получить список брендов")
        return
        
    logger.info(f"Найдено {len(brands)} брендов")
    
    # Берем первые 3 бренда
    test_brands = brands[:3]
    
    results = []
    
    for brand in test_brands:
        logger.info(f"\n=== Обработка бренда: {brand.name} ===")
        
        # Получаем модели
        models = parser.get_models_for_brand(brand)
        
        if not models:
            logger.warning(f"Нет моделей для бренда {brand.name}")
            continue
            
        logger.info(f"Найдено {len(models)} моделей для {brand.name}")
        
        # Берем первую модель с отзывами
        target_model = None
        for model in models:
            if model.long_reviews_count > 0 or model.short_reviews_count > 0:
                target_model = model
                break
                
        if not target_model:
            logger.warning(f"Нет моделей с отзывами для бренда {brand.name}")
            continue
            
        logger.info(f"Выбранная модель: {target_model.name} (длинных: {target_model.long_reviews_count}, коротких: {target_model.short_reviews_count})")
        
        # Парсим отзывы с ограничениями
        brand_reviews = parser.parse_model_reviews(
            target_model, 
            max_long_reviews=3,
            max_short_reviews=10
        )
        
        # Добавляем результаты
        brand_result = {
            'brand': brand.name,
            'model': target_model.name,
            'long_reviews_available': target_model.long_reviews_count,
            'short_reviews_available': target_model.short_reviews_count,
            'long_reviews_parsed': len([r for r in brand_reviews if r.review_type == 'long']),
            'short_reviews_parsed': len([r for r in brand_reviews if r.review_type == 'short']),
            'total_parsed': len(brand_reviews)
        }
        
        results.append(brand_result)
        
        # Выводим примеры отзывов
        for i, review in enumerate(brand_reviews[:2]):  # Показываем первые 2
            logger.info(f"\nПример отзыва {i+1} ({review.review_type}):")
            logger.info(f"  ID: {review.review_id}")
            logger.info(f"  Автор: {review.author}")
            logger.info(f"  Город: {review.city}")
            if review.year:
                logger.info(f"  Год авто: {review.year}")
            if review.engine_volume:
                logger.info(f"  Объем двигателя: {review.engine_volume}")
            if review.positive:
                logger.info(f"  Плюсы: {review.positive[:100]}...")
            if review.negative:
                logger.info(f"  Минусы: {review.negative[:100]}...")
            
    # Сохраняем результаты
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/test_production_parser_{timestamp}.json"
    
    os.makedirs("data", exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Итоговая статистика
    logger.info(f"\n=== ИТОГОВАЯ СТАТИСТИКА ===")
    total_long = sum(r['long_reviews_parsed'] for r in results)
    total_short = sum(r['short_reviews_parsed'] for r in results)
    total_all = sum(r['total_parsed'] for r in results)
    
    logger.info(f"Обработано брендов: {len(results)}")
    logger.info(f"Длинных отзывов: {total_long}")
    logger.info(f"Коротких отзывов: {total_short}")
    logger.info(f"Всего отзывов: {total_all}")
    logger.info(f"Результаты сохранены в: {output_file}")

if __name__ == "__main__":
    test_production_parser()
