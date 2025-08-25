"""Тестовый скрипт для проверки парсера Drom.ru."""

from src.auto_reviews_parser.parsers.drom import DromParser


def main():
    """Основная функция для тестирования парсера."""
    # Имя файла базы данных
    db_file = "auto_reviews.db"

    # Инициализация парсера с базой данных
    parser = DromParser(db_path=db_file)

    # Тестовый URL
    test_url = "https://www.drom.ru/reviews/toyota/camry/1700000/"

    print("Первый запуск парсера - должны получить новые отзывы")
    # Запуск парсинга
    reviews = parser.parse_page(test_url)

    # Вывод результатов
    print(f"\nОбработано отзывов: {len(reviews)}\n")

    for review in reviews:
        print("-" * 80)
        print(f"Марка: {review.brand}")
        print(f"Модель: {review.model}")
        print(f"Автор: {review.author}")
        print(f"Дата: {review.publish_date}")
        print(f"Рейтинг: {review.rating}")
        print(f"URL: {review.url}")
        print("\nТекст отзыва:")
        if len(review.content) > 300:
            print(review.content[:300] + "...")
        else:
            print(review.content)
        print()

    print("\nВторой запуск парсера - не должно быть новых отзывов")
    # Повторный запуск для проверки дедупликации
    reviews = parser.parse_page(test_url)

    # Проверяем статистику в базе данных
    stats = parser.repository.stats()
    print("\nСтатистика базы данных:")
    print(f"Всего отзывов: {stats.get('total_reviews', 0)}")
    print(f"Уникальных марок: {stats.get('unique_brands', 0)}")
    print(f"Уникальных моделей: {stats.get('unique_models', 0)}")

    if stats.get("by_source"):
        print("\nРаспределение по источникам:")
        for source, count in stats["by_source"].items():
            print(f"- {source}: {count} отзывов")
    print()


if __name__ == "__main__":
    main()
