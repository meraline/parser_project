#!/usr/bin/env python3
"""
Скрипт для организации кодовой базы проекта парсинга отзывов.
Перемещает файлы в правильную структуру папок.
"""

import os
import shutil
from pathlib import Path

def organize_codebase():
    """Организует кодовую базу в логичную структуру."""
    
    project_root = Path("/home/analityk/Документы/projects/parser_project")
    
    # Определяем категории файлов
    file_moves = {
        # Основные парсеры -> scripts/parsing
        "scripts/parsing": [
            "optimal_parser.py",
            "full_parsing_system.py", 
            "fast_parser.py",
            "catalog_extractor.py",
            "benchmark_parsing.py",
            "parse_reviews.py",
            "parse_single_model.py",
            "parse_structured_reviews.py",
            "parse_10_reviews.py",
            "gentle_parse.py",
        ],
        
        # Тестовые скрипты -> scripts/testing
        "scripts/testing": [
            "test_parser.py",
            "test_parser_basic.py", 
            "test_parser_simple.py",
            "test_simple_parser.py",
            "test_drom_parser.py",
            "test_async.py",
            "test_sync.py",
            "test_run.py",
            "test_gentle.py",
            "test_gentle_parsing.py",
            "test_catalog.py",
            "test_catalog_parser.py",
            "test_multiple_reviews.py",
            "test_comments_parsing.py",
            "test_comments_parsing_updated.py",
            "test_real_comments.db",
            "test_characteristics.py",
            "test_full_characteristics.py",
            "test_all_chars.py",
            "test_ratings.py",
            "test_author_city.py",
            "test_author_city_extraction.py",
            "test_addition.py",
            "gentle_test.py",
        ],
        
        # Отладочные скрипты -> scripts/debugging  
        "scripts/debugging": [
            "debug_additions.py",
            "debug_characteristics.py",
            "debug_extraction.py",
            "debug_full_process.py",
            "debug_hidden_blocks.py",
            "debug_ratings.py",
            "debug_specific_addition.py",
            "debug_url_type.py",
            "run_debug.py",
            "diagnose_save.py",
        ],
        
        # Анализ данных -> scripts/analysis
        "scripts/analysis": [
            "analyze_page.py",
            "analyze_reviews.py", 
            "check_all_characteristics.py",
            "check_characteristics.py",
            "check_db_schema.py",
            "check_main_review.py",
            "check_results.py",
            "find_ratings.py",
            "find_reviews_with_ratings.py",
            "update_main_database.py",
            "cleanup.py",
        ],
        
        # Демо скрипты -> scripts/demo
        "scripts/demo": [
            "demo_comments_integration.py",
        ],
        
        # Базы данных -> data/databases
        "data/databases": [
            "auto_reviews.db",
            "auto_reviews_structured.db", 
            "benchmark_test.db",
            "debug_reviews.db",
            "demo_reviews_with_comments.db",
            "diagnose.db",
            "reviews.db",
            "test_20_reviews.db",
            "test_all_chars.db",
            "test_characteristics.db",
            "test_comments.db",
            "test_ratings.db",
            "test_reviews.db",
        ],
        
        # Экспорты -> data/exports
        "data/exports": [
            "toyota_camry_ml_features.csv",
            "toyota_camry_reviews.json",
        ],
        
        # Документация -> docs
        "docs": [
            "FINAL_PARSING_GUIDE.md",
            "README.md",
            "REPORT.md",
        ],
    }
    
    print("🗂️ ОРГАНИЗАЦИЯ КОДОВОЙ БАЗЫ")
    print("=" * 50)
    
    # Создаем недостающие директории
    for target_dir in file_moves.keys():
        target_path = project_root / target_dir
        target_path.mkdir(parents=True, exist_ok=True)
        print(f"📁 Создана папка: {target_dir}")
    
    # Создаем дополнительную папку для демо
    demo_dir = project_root / "scripts/demo"
    demo_dir.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    
    # Перемещаем файлы
    for target_dir, files in file_moves.items():
        target_path = project_root / target_dir
        
        for filename in files:
            source_path = project_root / filename
            target_file_path = target_path / filename
            
            if source_path.exists() and source_path.is_file():
                try:
                    shutil.move(str(source_path), str(target_file_path))
                    print(f"📄 {filename} -> {target_dir}/")
                    moved_count += 1
                except Exception as e:
                    print(f"❌ Ошибка перемещения {filename}: {e}")
    
    print(f"\n✅ Перемещено {moved_count} файлов")
    
    # Создаем README для каждой папки
    create_readme_files(project_root)
    
    print("\n📚 Созданы README файлы для документации")
    print("\n🎯 НОВАЯ СТРУКТУРА ПРОЕКТА:")
    print_new_structure()

def create_readme_files(project_root):
    """Создает README файлы для каждой папки."""
    
    readme_contents = {
        "scripts/parsing/README.md": """# 🚀 Скрипты парсинга

Основные скрипты для парсинга отзывов с drom.ru

## Главные файлы:
- `optimal_parser.py` - Оптимальный парсер (рекомендуется)
- `full_parsing_system.py` - Полная система с каталогом
- `catalog_extractor.py` - Извлечение каталога брендов
- `benchmark_parsing.py` - Тестирование производительности

## Использование:
```bash
python optimal_parser.py  # Быстрый и надежный парсинг
```
""",
        
        "scripts/testing/README.md": """# 🧪 Тестовые скрипты

Скрипты для тестирования различных компонентов парсера

## Основные тесты:
- `test_parser.py` - Базовые тесты парсера
- `test_comments_parsing.py` - Тесты парсинга комментариев
- `test_characteristics.py` - Тесты характеристик
""",
        
        "scripts/debugging/README.md": """# 🐛 Отладочные скрипты

Скрипты для отладки и диагностики проблем

## Файлы:
- `debug_*.py` - Различные отладочные утилиты
- `run_debug.py` - Основной отладчик
""",
        
        "scripts/analysis/README.md": """# 📊 Анализ данных

Скрипты для анализа собранных данных

## Файлы:
- `analyze_*.py` - Анализ отзывов и страниц
- `check_*.py` - Проверка данных и схемы БД
""",
        
        "data/databases/README.md": """# 💾 Базы данных

Файлы баз данных SQLite

## Основные БД:
- `auto_reviews.db` - Главная база отзывов
- `test_*.db` - Тестовые базы данных
""",
        
        "data/exports/README.md": """# 📤 Экспорты данных

Экспортированные файлы данных

## Форматы:
- `.csv` - Табличные данные
- `.json` - Структурированные данные
""",
    }
    
    for file_path, content in readme_contents.items():
        full_path = project_root / file_path
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)

def print_new_structure():
    """Выводит новую структуру проекта."""
    structure = """
📦 parser_project/
├── 📁 src/                          # Исходный код парсера
│   └── auto_reviews_parser/
├── 📁 scripts/                      # Исполняемые скрипты
│   ├── 📁 parsing/                  # Основные парсеры
│   ├── 📁 testing/                  # Тестовые скрипты  
│   ├── 📁 debugging/                # Отладочные утилиты
│   ├── 📁 analysis/                 # Анализ данных
│   └── 📁 demo/                     # Демонстрации
├── 📁 data/                         # Данные проекта
│   ├── 📁 databases/                # Базы данных SQLite
│   └── 📁 exports/                  # Экспортированные данные
├── 📁 docs/                         # Документация
├── 📁 tests/                        # Unit тесты
├── 📁 logs/                         # Логи работы
└── 📁 cli/                          # CLI интерфейс
"""
    print(structure)

if __name__ == "__main__":
    organize_codebase()
