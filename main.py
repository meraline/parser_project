#!/usr/bin/env python3
"""
Главное меню проекта Auto Reviews Parser.
Удобный интерфейс для запуска всех основных функций.
"""

import os
import sys
from pathlib import Path


def clear_screen():
    """Очищает экран."""
    os.system("cls" if os.name == "nt" else "clear")


def print_header():
    """Выводит заголовок приложения."""
    print("🚀 AUTO REVIEWS PARSER")
    print("=" * 60)
    print("Профессиональная система парсинга отзывов с drom.ru")
    print("=" * 60)


def print_main_menu():
    """Выводит главное меню."""
    print("\n📋 ГЛАВНОЕ МЕНЮ:")
    print()
    print("🚀 ПАРСИНГ:")
    print("  1. Оптимальный парсер (рекомендуется)")
    print("  2. Полная система с каталогом")
    print("  3. Быстрый парсер")
    print("  4. Тестирование производительности")
    print("  5. Извлечение каталога брендов")
    print()
    print("🧪 ТЕСТИРОВАНИЕ:")
    print("  6. Базовые тесты парсера")
    print("  7. Тесты парсинга комментариев")
    print("  8. Тесты характеристик")
    print("  9. Интеграционные тесты")
    print()
    print("📊 АНАЛИЗ ДАННЫХ:")
    print("  10. Анализ отзывов")
    print("  11. Проверка схемы БД")
    print("  12. Статистика базы данных")
    print("  13. Обновление главной БД")
    print()
    print("🐛 ОТЛАДКА:")
    print("  14. Отладка парсера")
    print("  15. Диагностика проблем")
    print("  16. Анализ страницы")
    print()
    print("📚 ДОКУМЕНТАЦИЯ:")
    print("  17. Руководство по парсингу")
    print("  18. Структура проекта")
    print("  19. Примеры использования")
    print()
    print("  0. Выход")
    print()


def execute_script(script_path: str, description: str):
    """Выполняет скрипт."""
    print(f"\n🚀 Запуск: {description}")
    print("=" * 50)

    if not os.path.exists(script_path):
        print(f"❌ Файл не найден: {script_path}")
        return

    try:
        # Меняем директорию и запускаем скрипт
        script_dir = os.path.dirname(script_path)
        script_name = os.path.basename(script_path)

        if script_dir:
            os.chdir(script_dir)

        os.system(f"python {script_name}")

    except Exception as e:
        print(f"❌ Ошибка выполнения: {e}")

    input("\n📎 Нажмите Enter для продолжения...")


def show_file_content(file_path: str, description: str):
    """Показывает содержимое файла."""
    print(f"\n📄 {description}")
    print("=" * 50)

    if not os.path.exists(file_path):
        print(f"❌ Файл не найден: {file_path}")
        return

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            print(content[:2000])  # Показываем первые 2000 символов
            if len(content) > 2000:
                print("\n... (содержимое обрезано)")
                print(f"\nПолный файл: {file_path}")
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")

    input("\n📎 Нажмите Enter для продолжения...")


def show_project_structure():
    """Показывает структуру проекта."""
    print("\n📁 СТРУКТУРА ПРОЕКТА:")
    print("=" * 50)

    structure = """
📦 parser_project/
├── 📁 src/                          # Исходный код парсера
│   └── auto_reviews_parser/         # Основные модули
│       ├── parsers/                 # DromParser и др.
│       ├── models/                  # Review, Comment
│       ├── database/                # Работа с БД
│       ├── services/                # Бизнес-логика
│       └── utils/                   # Утилиты
├── 📁 scripts/                      # Исполняемые скрипты
│   ├── 📁 parsing/                  # 🚀 Основные парсеры
│   │   ├── optimal_parser.py        # Оптимальный парсер
│   │   ├── full_parsing_system.py   # Полная система
│   │   ├── benchmark_parsing.py     # Тесты производительности
│   │   └── catalog_extractor.py     # Каталог брендов
│   ├── 📁 testing/                  # 🧪 Тестовые скрипты
│   ├── 📁 debugging/                # 🐛 Отладочные утилиты
│   ├── 📁 analysis/                 # 📊 Анализ данных
│   └── 📁 demo/                     # 🎭 Демонстрации
├── 📁 data/                         # Данные проекта
│   ├── 📁 databases/                # 💾 Базы данных SQLite
│   └── 📁 exports/                  # 📤 Экспорты
├── 📁 docs/                         # 📚 Документация
└── 📁 logs/                         # 📝 Логи работы

🎯 ГОТОВ К ПАРСИНГУ 1M+ ОТЗЫВОВ!
    """

    print(structure)
    input("\n📎 Нажмите Enter для продолжения...")


def main():
    """Главная функция меню."""
    project_root = Path(__file__).parent

    while True:
        clear_screen()
        print_header()
        print_main_menu()

        try:
            choice = input("👉 Выберите действие (0-19): ").strip()

            if choice == "0":
                print("\n👋 До свидания!")
                break

            elif choice == "1":
                execute_script(
                    "scripts/parsing/optimal_parser.py", "Оптимальный парсер"
                )

            elif choice == "2":
                execute_script(
                    "scripts/parsing/full_parsing_system.py", "Полная система парсинга"
                )

            elif choice == "3":
                execute_script("scripts/parsing/fast_parser.py", "Быстрый парсер")

            elif choice == "4":
                execute_script(
                    "scripts/parsing/benchmark_parsing.py",
                    "Тестирование производительности",
                )

            elif choice == "5":
                execute_script(
                    "scripts/parsing/catalog_extractor.py",
                    "Извлечение каталога брендов",
                )

            elif choice == "6":
                execute_script(
                    "scripts/testing/test_parser.py", "Базовые тесты парсера"
                )

            elif choice == "7":
                execute_script(
                    "scripts/testing/test_comments_parsing.py",
                    "Тесты парсинга комментариев",
                )

            elif choice == "8":
                execute_script(
                    "scripts/testing/test_characteristics.py", "Тесты характеристик"
                )

            elif choice == "9":
                execute_script(
                    "scripts/testing/test_multiple_reviews.py", "Интеграционные тесты"
                )

            elif choice == "10":
                execute_script("scripts/analysis/analyze_reviews.py", "Анализ отзывов")

            elif choice == "11":
                execute_script(
                    "scripts/analysis/check_db_schema.py", "Проверка схемы БД"
                )

            elif choice == "12":
                execute_script(
                    "scripts/analysis/check_results.py", "Статистика базы данных"
                )

            elif choice == "13":
                execute_script(
                    "scripts/analysis/update_main_database.py", "Обновление главной БД"
                )

            elif choice == "14":
                execute_script("scripts/debugging/run_debug.py", "Отладка парсера")

            elif choice == "15":
                execute_script(
                    "scripts/debugging/diagnose_save.py", "Диагностика проблем"
                )

            elif choice == "16":
                execute_script("scripts/analysis/analyze_page.py", "Анализ страницы")

            elif choice == "17":
                show_file_content(
                    "docs/FINAL_PARSING_GUIDE.md", "Руководство по парсингу"
                )

            elif choice == "18":
                show_project_structure()

            elif choice == "19":
                show_file_content("docs/README.md", "Примеры использования")

            else:
                print("❌ Неверный выбор. Попробуйте снова.")
                input("📎 Нажмите Enter для продолжения...")

        except KeyboardInterrupt:
            print("\n\n👋 Выход по Ctrl+C")
            break
        except Exception as e:
            print(f"\n❌ Ошибка: {e}")
            input("📎 Нажмите Enter для продолжения...")


if __name__ == "__main__":
    main()
