#!/usr/bin/env python3
"""Скрипт инициализации базы данных."""

import sqlite3
import sys


def init_database(db_path: str = "auto_reviews.db") -> int:
    """Инициализация базы данных."""
    try:
        print("🔧 Инициализация базы данных...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Создание основной таблицы
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                type TEXT,
                brand TEXT,
                model TEXT,
                generation TEXT,
                year INTEGER,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                author TEXT,
                rating REAL,
                pros TEXT,
                cons TEXT,
                mileage INTEGER,
                engine_volume REAL,
                fuel_type TEXT,
                transmission TEXT,
                body_type TEXT,
                drive_type TEXT,
                publish_date TEXT,
                views_count INTEGER,
                content_hash TEXT,
                parsed_at TEXT
            )
        """
        )

        # Создание индексов
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_brand_model " "ON reviews(brand, model)",
            "CREATE INDEX IF NOT EXISTS idx_source_type " "ON reviews(source, type)",
            "CREATE INDEX IF NOT EXISTS idx_parsed_at " "ON reviews(parsed_at)",
            "CREATE INDEX IF NOT EXISTS idx_content_hash " "ON reviews(content_hash)",
        ]

        for index in indexes:
            cursor.execute(index)

        # Создание очереди парсинга
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS parse_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT,
                model TEXT,
                source TEXT,
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                parsed_pages INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        conn.commit()
        conn.close()
        print("✅ База данных инициализирована")
        return 0

    except Exception as e:
        print(f"\n❌ Ошибка: {str(e)}")
        return 1


def main() -> int:
    """Точка входа для инициализации БД."""
    return init_database()


if __name__ == "__main__":
    sys.exit(main())


if __name__ == "__main__":
    sys.exit(main())
