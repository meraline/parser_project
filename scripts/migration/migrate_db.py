#!/usr/bin/env python3
"""
Миграция схемы базы данных
"""
import sqlite3
from pathlib import Path
from datetime import datetime


def get_db_connection():
    """Подключение к базе данных"""
    db_path = (Path(__file__).parent.parent.parent /
               "data" / "databases" / "auto_reviews.db")
    return sqlite3.connect(db_path)


def create_migrations_table():
    """Создаём таблицу для отслеживания миграций"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT NOT NULL UNIQUE,
            description TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()


def apply_migration(version, description, sql):
    """Применяем миграцию"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Проверяем, была ли уже применена
    cursor.execute("SELECT id FROM migrations WHERE version = ?", (version,))
    if cursor.fetchone():
        print(f"Миграция {version} уже применена")
        conn.close()
        return
    
    try:
        # Применяем SQL
        cursor.executescript(sql)
        
        # Записываем в таблицу миграций
        cursor.execute("""
            INSERT INTO migrations (version, description)
            VALUES (?, ?)
        """, (version, description))
        
        conn.commit()
        print(f"✓ Применена миграция {version}: {description}")
    except Exception as e:
        conn.rollback()
        print(f"✗ Ошибка миграции {version}: {e}")
    finally:
        conn.close()


def run_migrations():
    """Запускаем все миграции"""
    create_migrations_table()
    
    # Миграция 001: Создание основных таблиц
    apply_migration("001", "Создание основных таблиц", """
        CREATE TABLE IF NOT EXISTS brands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS models (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            url TEXT NOT NULL,
            reviews_count INTEGER DEFAULT 0,
            short_reviews_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (brand_id) REFERENCES brands (id),
            UNIQUE(brand_id, slug)
        );
        
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id INTEGER NOT NULL,
            review_type TEXT DEFAULT 'long',
            title TEXT,
            content TEXT,
            author TEXT,
            rating REAL,
            year INTEGER,
            engine_volume REAL,
            fuel_type TEXT,
            transmission TEXT,
            drive_type TEXT,
            city TEXT,
            pros TEXT,
            cons TEXT,
            breakages TEXT,
            photos TEXT,
            source_url TEXT UNIQUE,
            review_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (model_id) REFERENCES models (id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_brands_slug ON brands(slug);
        CREATE INDEX IF NOT EXISTS idx_models_brand_slug
            ON models(brand_id, slug);
        CREATE INDEX IF NOT EXISTS idx_reviews_model ON reviews(model_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_type ON reviews(review_type);
    """)


if __name__ == "__main__":
    print("=== Миграция базы данных ===")
    run_migrations()
    print("Миграции завершены")
