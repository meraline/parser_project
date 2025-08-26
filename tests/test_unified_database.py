#!/usr/bin/env python3
"""
🧪 ТЕСТ УНИВЕРСАЛЬНОГО МЕНЕДЖЕРА БАЗЫ ДАННЫХ

Проверяет работу с PostgreSQL базой данных.
"""

import os
import sys
from pathlib import Path

# Добавляем путь к модулям проекта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from auto_reviews_parser.database.unified_manager import DatabaseManager


def test_postgres_connection():
    """Тест подключения к PostgreSQL"""
    print("🔌 Тестируем подключение к PostgreSQL...")
    
    # Устанавливаем переменные окружения
    os.environ['DATABASE_TYPE'] = 'postgresql'
    os.environ['DATABASE_HOST'] = 'localhost'
    os.environ['DATABASE_PORT'] = '5432'
    os.environ['DATABASE_NAME'] = 'auto_reviews'
    os.environ['DATABASE_USER'] = 'parser'
    os.environ['DATABASE_PASSWORD'] = 'parser'
    
    # Создаем менеджер с PostgreSQL подключением
    pg_connection_string = f"postgresql://{os.environ['DATABASE_USER']}:{os.environ['DATABASE_PASSWORD']}@{os.environ['DATABASE_HOST']}:{os.environ['DATABASE_PORT']}/{os.environ['DATABASE_NAME']}"
    
    manager = DatabaseManager(pg_connection_string)
    
    print(f"📊 Тип базы данных: {manager.db_type}")
    
    # Тестируем подключение
    try:
        conn = manager.get_connection()
        print("✅ Подключение к PostgreSQL успешно!")
        
        # Тестируем создание схемы
        if manager.create_database():
            print("✅ Схема базы данных создана!")
        else:
            print("❌ Ошибка создания схемы")
            return False
        
        # Тестируем статистику
        stats = manager.get_statistics()
        print(f"📊 Статистика базы: {stats}")
        
        # Тестируем добавление бренда
        brand_id = manager.add_brand(
            name="Тестовый Бренд",
            url_name="test-brand",
            full_url="https://test.com/test-brand",
            reviews_count=0
        )
        
        if brand_id:
            print(f"✅ Бренд добавлен с ID: {brand_id}")
            
            # Тестируем добавление модели
            model_id = manager.add_model(
                brand_id=brand_id,
                name="Тестовая Модель",
                url_name="test-model",
                full_url="https://test.com/test-brand/test-model",
                reviews_count=0
            )
            
            if model_id:
                print(f"✅ Модель добавлена с ID: {model_id}")
            else:
                print("❌ Ошибка добавления модели")
                return False
        else:
            print("❌ Ошибка добавления бренда")
            return False
        
        # Финальная статистика
        final_stats = manager.get_statistics()
        print(f"📊 Финальная статистика: {final_stats}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Запуск тестов базы данных...")
    
    if test_postgres_connection():
        print("🎉 Все тесты прошли успешно!")
        sys.exit(0)
    else:
        print("💥 Тесты провалены!")
        sys.exit(1)
