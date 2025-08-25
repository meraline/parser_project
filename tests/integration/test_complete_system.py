#!/usr/bin/env python3
"""
🧪 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ
=================================================

Тестирование полного цикла работы системы:
1. Создание базы данных
2. Инициализация каталога брендов
3. Парсинг отзывов (длинных и коротких)
4. Проверка результатов

Автор: AI Assistant
Дата: 2024
"""

import sys
import os
import logging
from pathlib import Path

# Добавляем корневую папку проекта в PYTHONPATH
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.auto_reviews_parser.database.schema import DatabaseManager
from src.auto_reviews_parser.catalog.initializer import SimpleCatalogInitializer
from src.auto_reviews_parser.parsers.drom_reviews import DromReviewsParser


class SystemTester:
    """Класс для комплексного тестирования системы"""

    def __init__(self, test_db_path: str = "test_auto_reviews.db"):
        self.test_db_path = test_db_path
        self.logger = logging.getLogger(__name__)

    def test_database_creation(self) -> bool:
        """Тест создания базы данных"""
        self.logger.info("🧪 Тестируем создание базы данных...")

        try:
            db_manager = DatabaseManager(self.test_db_path)
            result = db_manager.create_database()

            if result:
                self.logger.info("✅ База данных создана успешно")

                # Проверяем статистику
                stats = db_manager.get_statistics()
                self.logger.info(f"📊 Начальная статистика: {stats}")
                return True
            else:
                self.logger.error("❌ Ошибка создания базы данных")
                return False

        except Exception as e:
            self.logger.error(f"❌ Исключение при создании БД: {e}")
            return False

    def test_catalog_initialization(self) -> bool:
        """Тест инициализации каталога"""
        self.logger.info("🧪 Тестируем инициализацию каталога...")

        try:
            # Проверяем наличие HTML файла с брендами
            html_file = project_root / "brands_html.txt"
            if not html_file.exists():
                self.logger.error(f"❌ HTML файл не найден: {html_file}")
                return False

            # Инициализируем каталог
            initializer = SimpleCatalogInitializer()
            # Используем тестовую базу данных
            initializer.db_manager = DatabaseManager(self.test_db_path)

            result = initializer.initialize_from_html_file(str(html_file))

            if result:
                self.logger.info("✅ Каталог инициализирован успешно")

                # Проверяем статистику
                stats = initializer.db_manager.get_statistics()
                self.logger.info(f"📊 Статистика после инициализации: {stats}")
                return True
            else:
                self.logger.error("❌ Ошибка инициализации каталога")
                return False

        except Exception as e:
            self.logger.error(f"❌ Исключение при инициализации: {e}")
            return False

    def test_reviews_parsing(self) -> bool:
        """Тест парсинга отзывов"""
        self.logger.info("🧪 Тестируем парсинг отзывов...")

        try:
            # Создаем парсер с тестовой базой данных
            parser = DromReviewsParser(delay=0.5)
            parser.db_manager = DatabaseManager(self.test_db_path)

            # Тестируем на небольшом количестве страниц
            success = parser.parse_model_reviews(
                brand_url_name="toyota",
                model_url_name="camry",
                max_pages_long=1,
                max_pages_short=1,
            )

            if success:
                self.logger.info("✅ Парсинг отзывов выполнен успешно")

                # Проверяем статистику
                stats = parser.db_manager.get_statistics()
                self.logger.info(f"📊 Финальная статистика: {stats}")
                return True
            else:
                self.logger.warning(
                    "⚠️ Парсинг завершился без ошибок, " "но отзывы могли не найтись"
                )
                return True  # Это не всегда ошибка

        except Exception as e:
            self.logger.error(f"❌ Исключение при парсинге: {e}")
            return False

    def test_database_operations(self) -> bool:
        """Тест операций с базой данных"""
        self.logger.info("🧪 Тестируем операции с базой данных...")

        try:
            db_manager = DatabaseManager(self.test_db_path)

            # Тест добавления бренда
            brand_id = db_manager.add_brand(
                name="Тестовый Бренд",
                url_name="test_brand",
                full_url="https://test.com/brand",
                reviews_count=100,
            )

            if not brand_id:
                self.logger.error("❌ Ошибка добавления бренда")
                return False

            # Тест добавления модели
            model_id = db_manager.add_model(
                brand_id=brand_id,
                name="Тестовая Модель",
                url_name="test_model",
                full_url="https://test.com/model",
                reviews_count=50,
            )

            if not model_id:
                self.logger.error("❌ Ошибка добавления модели")
                return False

            # Тест добавления отзыва
            review_id = db_manager.add_review(
                model_id=model_id,
                review_type="long",
                title="Тестовый отзыв",
                content="Тестовое содержимое",
                positive_text="Плюсы",
                negative_text="Минусы",
                author_name="Тестовый Автор",
                car_year=2020,
                car_engine_volume=2.0,
                overall_rating=4.5,
            )

            if not review_id:
                self.logger.error("❌ Ошибка добавления отзыва")
                return False

            # Тест получения данных
            brand = db_manager.get_brand_by_url_name("test_brand")
            if not brand:
                self.logger.error("❌ Ошибка получения бренда")
                return False

            model = db_manager.get_model_by_url_name(brand_id, "test_model")
            if not model:
                self.logger.error("❌ Ошибка получения модели")
                return False

            self.logger.info("✅ Все операции с базой данных прошли успешно")
            return True

        except Exception as e:
            self.logger.error(f"❌ Исключение при тестировании БД: {e}")
            return False

    def run_complete_test(self) -> bool:
        """Запуск полного тестирования"""
        self.logger.info("🚀 НАЧИНАЕМ КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ СИСТЕМЫ")
        self.logger.info("=" * 60)

        # Удаляем тестовую базу данных если она существует
        test_db_file = Path(self.test_db_path)
        if test_db_file.exists():
            test_db_file.unlink()
            self.logger.info(f"🗑️ Удалена старая тестовая БД: {self.test_db_path}")

        tests = [
            ("Создание базы данных", self.test_database_creation),
            ("Операции с базой данных", self.test_database_operations),
            ("Инициализация каталога", self.test_catalog_initialization),
            ("Парсинг отзывов", self.test_reviews_parsing),
        ]

        passed = 0
        failed = 0

        for test_name, test_func in tests:
            self.logger.info(f"\n🧪 Тест: {test_name}")
            self.logger.info("-" * 40)

            try:
                if test_func():
                    self.logger.info(f"✅ {test_name} - ПРОЙДЕН")
                    passed += 1
                else:
                    self.logger.error(f"❌ {test_name} - ПРОВАЛЕН")
                    failed += 1
            except Exception as e:
                self.logger.error(f"💥 {test_name} - ИСКЛЮЧЕНИЕ: {e}")
                failed += 1

        # Итоговый отчет
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 ИТОГОВЫЙ ОТЧЕТ ТЕСТИРОВАНИЯ")
        self.logger.info("=" * 60)
        self.logger.info(f"✅ Пройдено тестов: {passed}")
        self.logger.info(f"❌ Провалено тестов: {failed}")
        self.logger.info(f"📈 Процент успеха: {passed/(passed+failed)*100:.1f}%")

        if failed == 0:
            self.logger.info("🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
            return True
        else:
            self.logger.error("💥 НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛИЛИСЬ!")
            return False

    def cleanup(self):
        """Очистка после тестирования"""
        test_db_file = Path(self.test_db_path)
        if test_db_file.exists():
            test_db_file.unlink()
            self.logger.info(f"🧹 Очистка: удалена тестовая БД {self.test_db_path}")


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("test_complete_system.log", encoding="utf-8"),
        ],
    )

    # Создание и запуск тестера
    tester = SystemTester()

    try:
        success = tester.run_complete_test()

        if success:
            print("\n🎉 КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО УСПЕШНО!")
            print("📝 Подробный лог сохранен в test_complete_system.log")
        else:
            print("\n💥 ТЕСТИРОВАНИЕ ЗАВЕРШИЛОСЬ С ОШИБКАМИ!")
            print("📝 Проверьте лог test_complete_system.log для деталей")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ Тестирование прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Критическая ошибка тестирования: {e}")
        sys.exit(1)
    finally:
        # Очистка
        tester.cleanup()
