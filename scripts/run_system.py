#!/usr/bin/env python3
"""
🎯 ГЛАВНЫЙ ЗАПУСКАТЕЛЬ СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ DROM.RU

Комплексное решение для создания, инициализации и работы с каталогом отзывов:
1. Создание нормализованной базы данных
2. Инициализация каталога брендов и моделей
3. Парсинг отзывов по каталогу
4. Мониторинг и статистика

Использование:
    python run_system.py --setup                 # Полная настройка системы
    python run_system.py --parse BRAND          # Парсинг конкретного бренда
    python run_system.py --stats                # Показать статистику
    python run_system.py --list-brands          # Список брендов
"""

import sys
import os
import subprocess
import argparse
import time
from pathlib import Path
from typing import Dict, List


class SystemManager:
    """Менеджер системы парсинга отзывов"""

    def __init__(self):
        self.project_root = Path(__file__).parent.absolute()
        self.db_path = self.project_root / "auto_reviews.db"

        # Файлы системы
        self.database_schema_path = self.project_root / "database_schema.py"
        self.catalog_initializer_path = self.project_root / "catalog_initializer.py"
        self.catalog_parser_path = self.project_root / "catalog_parser_v2.py"
        self.brands_html_path = self.project_root / "brands_html.txt"

    def check_system_files(self) -> Dict[str, bool]:
        """Проверка наличия файлов системы"""
        files_status = {}

        required_files = [
            ("database_schema.py", self.database_schema_path),
            ("catalog_initializer.py", self.catalog_initializer_path),
            ("catalog_parser_v2.py", self.catalog_parser_path),
            ("brands_html.txt", self.brands_html_path),
        ]

        print("🔍 ПРОВЕРКА ФАЙЛОВ СИСТЕМЫ:")
        for name, path in required_files:
            exists = path.exists()
            files_status[name] = exists
            status = "✅" if exists else "❌"
            print(f"{status} {name}")

        return files_status

    def create_database(self) -> bool:
        """Создание базы данных"""
        print("\n🗄️ СОЗДАНИЕ БАЗЫ ДАННЫХ...")

        if self.db_path.exists():
            print(f"⚠️ База данных уже существует: {self.db_path}")
            choice = input("Пересоздать? (y/N): ").lower().strip()
            if choice == "y":
                self.db_path.unlink()
                print("🗑️ Старая база данных удалена")
            else:
                print("✅ Используем существующую базу данных")
                return True

        try:
            result = subprocess.run(
                [sys.executable, str(self.database_schema_path)],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                print("✅ База данных создана успешно")
                if result.stdout:
                    print(result.stdout)
                return True
            else:
                print(f"❌ Ошибка создания базы данных: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Исключение при создании базы данных: {e}")
            return False

    def initialize_catalog(self) -> bool:
        """Инициализация каталога брендов и моделей"""
        print("\n📋 ИНИЦИАЛИЗАЦИЯ КАТАЛОГА...")

        if not self.db_path.exists():
            print("❌ База данных не найдена. Создайте ее сначала.")
            return False

        try:
            result = subprocess.run(
                [sys.executable, str(self.catalog_initializer_path)],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                print("✅ Каталог инициализирован успешно")
                if result.stdout:
                    print(result.stdout)
                return True
            else:
                print(f"❌ Ошибка инициализации каталога: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Исключение при инициализации каталога: {e}")
            return False

    def setup_system(self) -> bool:
        """Полная настройка системы"""
        print("🚀 НАСТРОЙКА СИСТЕМЫ ПАРСИНГА ОТЗЫВОВ")
        print("=" * 50)

        # Проверяем файлы
        files_status = self.check_system_files()
        missing_files = [name for name, exists in files_status.items() if not exists]

        if missing_files:
            print(f"\n❌ Отсутствуют файлы: {', '.join(missing_files)}")
            print("Убедитесь, что все файлы системы находятся в рабочей директории")
            return False

        # Создаем базу данных
        if not self.create_database():
            return False

        # Инициализируем каталог
        if not self.initialize_catalog():
            return False

        print("\n🎉 СИСТЕМА ГОТОВА К РАБОТЕ!")
        print("Используйте следующие команды:")
        print("  python run_system.py --list-brands    # Список брендов")
        print("  python run_system.py --parse toyota   # Парсинг отзывов Toyota")
        print("  python run_system.py --stats          # Статистика базы данных")

        return True

    def list_brands(self) -> bool:
        """Список доступных брендов"""
        print("\n📋 СПИСОК БРЕНДОВ")
        print("=" * 30)

        try:
            result = subprocess.run(
                [sys.executable, str(self.catalog_parser_path), "--list-brands"],
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )

            if result.returncode == 0:
                print(result.stdout)
                return True
            else:
                print(f"❌ Ошибка получения списка брендов: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Исключение при получении списка брендов: {e}")
            return False

    def parse_brand(
        self, brand_slug: str, max_models: int = 3, max_reviews: int = 5
    ) -> bool:
        """Парсинг отзывов бренда"""
        print(f"\n🚗 ПАРСИНГ БРЕНДА: {brand_slug.upper()}")
        print("=" * 40)

        if not self.db_path.exists():
            print("❌ База данных не найдена. Выполните настройку: --setup")
            return False

        try:
            cmd = [
                sys.executable,
                str(self.catalog_parser_path),
                "--brand",
                brand_slug,
                "--max-models",
                str(max_models),
                "--max-reviews",
                str(max_reviews),
            ]

            print(f"Команда: {' '.join(cmd)}")
            print(f"Максимум моделей: {max_models}")
            print(f"Максимум отзывов на модель: {max_reviews}")
            print()

            # Запускаем в интерактивном режиме для вывода в реальном времени
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                cwd=self.project_root,
            )

            # Выводим результат в реальном времени
            for line in process.stdout:
                print(line.rstrip())

            process.wait()

            if process.returncode == 0:
                print("\n✅ Парсинг завершен успешно")
                return True
            else:
                print(f"\n❌ Парсинг завершен с ошибкой (код: {process.returncode})")
                return False

        except Exception as e:
            print(f"❌ Исключение при парсинге: {e}")
            return False

    def show_stats(self) -> bool:
        """Показать статистику базы данных"""
        print("\n📊 СТАТИСТИКА БАЗЫ ДАННЫХ")
        print("=" * 30)

        if not self.db_path.exists():
            print("❌ База данных не найдена. Выполните настройку: --setup")
            return False

        try:
            # Импортируем DatabaseManager для получения статистики
            sys.path.append(str(self.project_root))
            from database_schema import DatabaseManager

            db_manager = DatabaseManager(str(self.db_path))
            stats = db_manager.get_database_stats()

            print(f"Брендов в каталоге: {stats.get('brands', 0)}")
            print(f"Моделей в каталоге: {stats.get('models', 0)}")
            print(f"Отзывов в базе: {stats.get('reviews', 0)}")
            print(f"Полных отзывов: {stats.get('complete_reviews', 0)}")
            print(f"Процент завершенности: {stats.get('completion_rate', 0)}%")

            # Топ брендов по количеству отзывов
            brands = db_manager.get_all_brands()
            if brands:
                print(f"\nТоп-10 брендов по количеству отзывов:")
                for i, brand in enumerate(
                    sorted(brands, key=lambda x: x["review_count"], reverse=True)[:10],
                    1,
                ):
                    print(f"{i:2d}. {brand['name']} - {brand['review_count']} отзывов")

            return True

        except Exception as e:
            print(f"❌ Ошибка получения статистики: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Главный запускатель системы парсинга отзывов Drom.ru",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python run_system.py --setup                 # Полная настройка системы
  python run_system.py --parse toyota          # Парсинг отзывов Toyota
  python run_system.py --parse bmw --models 5  # Парсинг BMW, максимум 5 моделей
  python run_system.py --stats                 # Показать статистику
  python run_system.py --list-brands           # Список доступных брендов
        """,
    )

    parser.add_argument(
        "--setup",
        action="store_true",
        help="Полная настройка системы (создание БД + инициализация каталога)",
    )
    parser.add_argument(
        "--parse",
        type=str,
        metavar="BRAND",
        help="Парсинг отзывов для указанного бренда (slug)",
    )
    parser.add_argument(
        "--models",
        type=int,
        default=3,
        metavar="N",
        help="Максимальное количество моделей для парсинга (по умолчанию: 3)",
    )
    parser.add_argument(
        "--reviews",
        type=int,
        default=5,
        metavar="N",
        help="Максимальное количество отзывов на модель (по умолчанию: 5)",
    )
    parser.add_argument(
        "--stats", action="store_true", help="Показать статистику базы данных"
    )
    parser.add_argument(
        "--list-brands", action="store_true", help="Показать список доступных брендов"
    )

    args = parser.parse_args()

    # Создаем менеджер системы
    system_manager = SystemManager()

    # Определяем действие
    if args.setup:
        success = system_manager.setup_system()
    elif args.parse:
        success = system_manager.parse_brand(
            brand_slug=args.parse, max_models=args.models, max_reviews=args.reviews
        )
    elif args.stats:
        success = system_manager.show_stats()
    elif args.list_brands:
        success = system_manager.list_brands()
    else:
        print("🚀 СИСТЕМА ПАРСИНГА ОТЗЫВОВ DROM.RU")
        print("=" * 40)
        print("Выберите действие:")
        print("  --setup           Настройка системы")
        print("  --parse BRAND     Парсинг отзывов")
        print("  --stats           Статистика")
        print("  --list-brands     Список брендов")
        print("\nИспользуйте --help для подробной справки")
        success = True

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
