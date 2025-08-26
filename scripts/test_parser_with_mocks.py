#!/usr/bin/env python3
"""
Тестовый парсер с использованием мок-системы для демонстрации работы
"""

import os
import sys
from pathlib import Path

# Добавляем корневую папку проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Настраиваем моки ПЕРЕД импортом
import tests.mocks.mock_requests as mock_requests
import tests.mocks.mock_bs4 as mock_bs4
sys.modules['requests'] = mock_requests
sys.modules['bs4'] = mock_bs4

# Теперь импортируем модели и логику
from src.auto_reviews_parser.models.review_models import Review, ReviewAuthor


def create_sample_reviews():
    """Создаем образцы отзывов для демонстрации"""
    
    # Образец автора
    author = ReviewAuthor(
        user_id="test_user_123",
        username="АвтоЛюбитель2024",
        registration_date="2020-01-15",
        total_reviews=15,
        location="Москва"
    )
    
    # Образцы отзывов
    reviews = []
    
    # Длинный отзыв
    review1 = Review(
        review_id="1425079",
        url="https://www.drom.ru/reviews/toyota/4runner/1425079/",
        brand="Toyota",
        model="4Runner",
        year=2018,
        engine_volume=4.0,
        fuel_type="бензин",
        transmission="автомат",
        drive_type="полный",
        author=author,
        title="Отличный внедорожник для любых условий",
        content="Купил Toyota 4Runner год назад и очень доволен покупкой. "
               "Машина показала себя отлично как в городе, так и на бездорожье. "
               "Двигатель мощный, подвеска крепкая, салон просторный. "
               "Расход топлива в городе около 15 литров, но для такого размера "
               "это нормально. Надежность на высоте - никаких поломок за год.",
        pros="Надежность, мощность, проходимость, просторный салон",
        cons="Высокий расход топлива, дорогое обслуживание",
        useful_count=42,
        publication_date="2024-03-15",
        rating=5,
        type="длинный",
        source="drom.ru"
    )
    
    # Короткий отзыв
    review2 = Review(
        review_id="2192920",
        url="https://www.drom.ru/reviews/mazda/familia/short/2192920/",
        brand="Mazda",
        model="Familia",
        year=2001,
        engine_volume=1.5,
        fuel_type="бензин",
        transmission="автомат",
        drive_type="передний",
        author=ReviewAuthor(
            user_id="user_16138765",
            username="16138765",
            location="Минусинск"
        ),
        title="",
        content="",
        pros="Неубиваемая подвеска, живучий мотор, офигенно жаркая печка",
        cons="Слабая антикоррозионная стойкость кузова, деревянный салон",
        useful_count=5,
        publication_date="2023-08-26",
        rating=4,
        type="короткий",
        source="drom.ru"
    )
    
    reviews.extend([review1, review2])
    return reviews


def simulate_parsing():
    """Симулируем процесс парсинга"""
    
    print("🚀 ДЕМОНСТРАЦИЯ СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ")
    print("=" * 50)
    
    print("📡 Инициализация парсера...")
    print("✅ Парсер инициализирован с мок-системой")
    
    print("\n📋 Создание тестовых отзывов...")
    reviews = create_sample_reviews()
    print(f"✅ Создано {len(reviews)} тестовых отзыва")
    
    print(f"\n📊 РЕЗУЛЬТАТЫ ПАРСИНГА:")
    print("-" * 30)
    
    for i, review in enumerate(reviews, 1):
        print(f"\n{i}. Отзыв ID: {review.review_id}")
        print(f"   🚗 {review.brand} {review.model} ({review.year})")
        print(f"   🔧 {review.engine_volume}л {review.fuel_type} {review.transmission}")
        print(f"   👤 Автор: {review.author.username} ({review.author.location})")
        print(f"   📅 Дата: {review.publication_date}")
        print(f"   ⭐ Рейтинг: {review.rating}/5")
        print(f"   📝 Тип: {review.type}")
        print(f"   👍 Полезность: {review.useful_count}")
        
        if review.title:
            print(f"   📄 Заголовок: {review.title}")
        
        if review.content:
            content_preview = review.content[:100] + "..." if len(review.content) > 100 else review.content
            print(f"   📖 Содержание: {content_preview}")
        
        if review.pros:
            print(f"   ✅ Плюсы: {review.pros}")
        
        if review.cons:
            print(f"   ❌ Минусы: {review.cons}")
    
    print(f"\n🎉 ИТОГО ОБРАБОТАНО: {len(reviews)} отзывов")
    print("✅ Все отзывы успешно обработаны!")
    
    print(f"\n📈 СТАТИСТИКА:")
    print(f"• Длинных отзывов: {sum(1 for r in reviews if r.type == 'длинный')}")
    print(f"• Коротких отзывов: {sum(1 for r in reviews if r.type == 'короткий')}")
    print(f"• Средний рейтинг: {sum(r.rating for r in reviews) / len(reviews):.1f}")
    print(f"• Общая полезность: {sum(r.useful_count for r in reviews)}")
    
    print(f"\n💾 Данные готовы для сохранения в базу данных")
    print("🏁 Демонстрация парсинга завершена!")


if __name__ == "__main__":
    try:
        simulate_parsing()
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
