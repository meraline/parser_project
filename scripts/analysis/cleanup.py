#!/usr/bin/env python3
"""Скрипт для очистки проекта от неиспользуемых файлов."""

import os
import shutil
from pathlib import Path

# Файлы для удаления
FILES_TO_REMOVE = [
    "analyze_page.py",
    "check_results.py",
    "debug_page.html",
    "debug_screenshot.png",
    "gentle_parse.py",
    "gentle_test.py",
    "test_async.py",
    "test_catalog.py",
    "test_catalog_parser.py",
    "test_gentle.py",
    "test_sync.py",
    "parser_debug.log",
]

# Папки для удаления
DIRS_TO_REMOVE = [
    "cache",
    "cli",
    "docker",
    "error_logs",
    "logs",
    "output",
]

# Файлы в src/auto_reviews_parser/parsers для удаления
PARSER_FILES_TO_REMOVE = [
    "drive2.py",
    "drive2_parser.py",
    "simple_drom.py",
    "drom_fixed.py",
    "drom_botasaurus.py",
    "drom_botasaurus_old.py",
    "drom_botasaurus_new.py",
    "drom_botasaurus_fixed.py",
    "drom_botasaurus_final.py",
    "drom_botasaurus_latest.py",
]


def cleanup_project():
    """Очистка проекта от неиспользуемых файлов."""
    root = Path(__file__).parent

    # Удаляем файлы из корня проекта
    for file in FILES_TO_REMOVE:
        path = root / file
        if path.exists():
            path.unlink()
            print(f"Удален файл: {file}")

    # Удаляем папки
    for directory in DIRS_TO_REMOVE:
        path = root / directory
        if path.exists():
            shutil.rmtree(path)
            print(f"Удалена папка: {directory}")

    # Удаляем файлы парсеров
    parsers_dir = root / "src" / "auto_reviews_parser" / "parsers"
    for file in PARSER_FILES_TO_REMOVE:
        path = parsers_dir / file
        if path.exists():
            path.unlink()
            print(f"Удален файл парсера: {file}")

    # Удаляем все __pycache__ directories
    for path in Path(root).rglob("__pycache__"):
        shutil.rmtree(path)
        print(f"Удален кэш Python: {path}")

    # Удаляем .pyc файлы
    for path in Path(root).rglob("*.pyc"):
        path.unlink()
        print(f"Удален файл: {path}")


if __name__ == "__main__":
    cleanup_project()
