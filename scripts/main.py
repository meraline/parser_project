#!/usr/bin/env python3
"""
🚀 ГЛАВНЫЙ СКРИПТ ЗАПУСКА СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ
===============================================

Комплексная система для парсинга отзывов с drom.ru:
- Инициализация каталога брендов и моделей
- Парсинг длинных и коротких отзывов
- Сохранение в нормализованную базу данных
- Управление через командную строку

Автор: AI Assistant
Дата: 2024
"""

import sys
import argparse
import logging
from pathlib import Path

# Добавляем src в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# pylint: disable=wrong-import-position
from src.auto_reviews_parser.database.schema import DatabaseManager
from src.auto_reviews_parser.catalog.initializer import SimpleCatalogInitializer
from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


def setup_logging(log_level: str = "INFO"):
    """Настройка логирования"""
    level = getattr(logging, log_level.upper())

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("system_run.log", encoding="utf-8"),
        ],
    )


def init_database(db_path: str) -> bool:
    """Инициализация базы данных"""
    print("🗄️ Инициализация базы данных...")

    db_manager = DatabaseManager(db_path)
    if db_manager.create_database():
        print("✅ База данных создана успешно")
        return True
    print("❌ Ошибка создания базы данных")
    return False


def init_catalog(html_file: str, db_path: str) -> bool:
    """Инициализация каталога брендов"""
    print("🗂️ Инициализация каталога брендов...")

    if not Path(html_file).exists():
        print(f"❌ HTML файл не найден: {html_file}")
        return False

    initializer = SimpleCatalogInitializer()
    initializer.db_manager = DatabaseManager(db_path)

    if initializer.initialize_from_html_file(html_file):
        print("✅ Каталог брендов инициализирован")

        # Показываем статистику
        stats = initializer.db_manager.get_statistics()
        print(f"📊 Загружено брендов: {stats.get('brands_count', 0)}")
        return True
    print("❌ Ошибка инициализации каталога")
    return False


def parse_reviews(
    brand: str, model: str, db_path: str, max_long: int = 5, max_short: int = 10
) -> bool:
    """Парсинг отзывов для модели"""
    print(f"🚗 Парсинг отзывов для {brand}/{model}...")

    parser = DromReviewsParser(delay=1.0)
    parser.db_manager = DatabaseManager(db_path)

    success = parser.parse_model_reviews(
        brand_url_name=brand,
        model_url_name=model,
        max_pages_long=max_long,
        max_pages_short=max_short,
    )

    if success:
        print("✅ Парсинг отзывов завершен")

        # Показываем статистику
        stats = parser.db_manager.get_statistics()
        print(f"📊 Всего отзывов: {stats.get('total_reviews', 0)}")
        print(f"   📄 Длинных: {stats.get('long_reviews', 0)}")
        print(f"   📋 Коротких: {stats.get('short_reviews', 0)}")
        return True
    print("❌ Ошибка парсинга отзывов")
    return False


def show_statistics(db_path: str):
    """Показать статистику базы данных"""
    print("📊 Статистика базы данных:")
    print("-" * 30)

    db_manager = DatabaseManager(db_path)
    stats = db_manager.get_statistics()

    print(f"🏢 Брендов: {stats.get('brands_count', 0)}")
    print(f"🚗 Моделей: {stats.get('models_count', 0)}")
    print(f"📝 Отзывов всего: {stats.get('total_reviews', 0)}")
    print(f"   📄 Длинных отзывов: {stats.get('long_reviews', 0)}")
    print(f"   📋 Коротких отзывов: {stats.get('short_reviews', 0)}")


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="Система парсинга отзывов с drom.ru",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Инициализация системы
  python main.py init --db data/reviews.db --html brands_html.txt

  # Парсинг отзывов
  python main.py parse toyota camry --db data/reviews.db

  # Показать статистику
  python main.py stats --db data/reviews.db

  # Полный цикл
  python main.py full --db data/reviews.db --html brands_html.txt \\
                       --brand toyota --model camry
        """,
    )

    # Общие параметры
    parser.add_argument("--db", default="auto_reviews.db", help="Путь к базе данных")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования",
    )

    # Подкоманды
    subparsers = parser.add_subparsers(dest="command", help="Команды")

    # Команда инициализации
    init_parser = subparsers.add_parser("init", help="Инициализация системы")
    init_parser.add_argument(
        "--html", default="brands_html.txt", help="HTML файл с каталогом брендов"
    )

    # Команда парсинга
    parse_parser = subparsers.add_parser("parse", help="Парсинг отзывов")
    parse_parser.add_argument("brand", help="URL имя бренда")
    parse_parser.add_argument("model", help="URL имя модели")
    parse_parser.add_argument(
        "--max-long", type=int, default=5, help="Максимум страниц длинных отзывов"
    )
    parse_parser.add_argument(
        "--max-short", type=int, default=10, help="Максимум страниц коротких отзывов"
    )

    # Команда статистики
    subparsers.add_parser("stats", help="Показать статистику")

    # Команда полного цикла
    full_parser = subparsers.add_parser("full", help="Полный цикл")
    full_parser.add_argument(
        "--html", default="brands_html.txt", help="HTML файл с каталогом брендов"
    )
    full_parser.add_argument(
        "--brand", required=True, help="URL имя бренда для парсинга"
    )
    full_parser.add_argument(
        "--model", required=True, help="URL имя модели для парсинга"
    )
    full_parser.add_argument(
        "--max-long", type=int, default=5, help="Максимум страниц длинных отзывов"
    )
    full_parser.add_argument(
        "--max-short", type=int, default=10, help="Максимум страниц коротких отзывов"
    )

    args = parser.parse_args()

    # Настройка логирования
    setup_logging(args.log_level)

    if not args.command:
        parser.print_help()
        return

    print("🚀 СИСТЕМА ПАРСИНГА ОТЗЫВОВ DROM.RU")
    print("=" * 40)

    # Выполнение команд
    if args.command == "init":
        success = True
        success &= init_database(args.db)
        success &= init_catalog(args.html, args.db)

        if success:
            print("\n🎉 Инициализация завершена успешно!")
            show_statistics(args.db)
        else:
            print("\n❌ Ошибка инициализации!")
            sys.exit(1)

    elif args.command == "parse":
        if parse_reviews(
            args.brand, args.model, args.db, args.max_long, args.max_short
        ):
            print("\n🎉 Парсинг завершен успешно!")
        else:
            print("\n❌ Ошибка парсинга!")
            sys.exit(1)

    elif args.command == "stats":
        show_statistics(args.db)

    elif args.command == "full":
        success = True
        success &= init_database(args.db)
        success &= init_catalog(args.html, args.db)

        if success:
            success &= parse_reviews(
                args.brand, args.model, args.db, args.max_long, args.max_short
            )

        if success:
            print("\n🎉 Полный цикл завершен успешно!")
            show_statistics(args.db)
        else:
            print("\n❌ Ошибка выполнения!")
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        sys.exit(1)
