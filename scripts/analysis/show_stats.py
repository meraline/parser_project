#!/usr/bin/env python3
"""
Простой скрипт для показа статистики базы данных
"""

import asyncio
import sys
import os

async def show_database_stats():
    """Показать статистику базы данных"""
    try:
        import asyncpg
        
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            database='auto_reviews',
            user='parser',
            password='parser'
        )
        
        print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ")
        print("=" * 40)
        
        # Проверяем существование таблиц в схеме auto_reviews
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'auto_reviews' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        if not tables:
            print("⚠️  Схема auto_reviews пуста или не инициализирована")
            print("🔧 Выполните: python cli/init_db.py")
            await conn.close()
            return
        
        print(f"📋 Количество таблиц в схеме auto_reviews: {len(tables)}")
        print("\n📈 Статистика по данным:")
        
        # Статистика по основным таблицам (с указанием схемы)
        stats_queries = [
            ("бренды", "SELECT COUNT(*) FROM auto_reviews.бренды"),
            ("модели", "SELECT COUNT(*) FROM auto_reviews.модели"),
            ("отзывы", "SELECT COUNT(*) FROM auto_reviews.отзывы"), 
            ("короткие_отзывы", "SELECT COUNT(*) FROM auto_reviews.короткие_отзывы"),
            ("авторы", "SELECT COUNT(*) FROM auto_reviews.авторы"),
            ("сессии_парсинга", "SELECT COUNT(*) FROM auto_reviews.сессии_парсинга"),
            ("города", "SELECT COUNT(*) FROM auto_reviews.города"),
            ("комментарии", "SELECT COUNT(*) FROM auto_reviews.комментарии")
        ]
        
        for table_name, query in stats_queries:
            try:
                count = await conn.fetchval(query)
                print(f"  {table_name:15}: {count:>8,} записей")
            except Exception:
                print(f"  {table_name:15}: таблица не найдена")
        
        # Топ-5 брендов по отзывам
        try:
            top_brands = await conn.fetch("""
                SELECT название, количество_отзывов 
                FROM auto_reviews.бренды 
                WHERE количество_отзывов > 0
                ORDER BY количество_отзывов DESC 
                LIMIT 5
            """)
            
            if top_brands:
                print("\n🏆 ТОП-5 БРЕНДОВ ПО ОТЗЫВАМ:")
                for i, brand in enumerate(top_brands, 1):
                    name = brand['название']
                    count = brand['количество_отзывов']
                    print(f"  {i}. {name:20}: {count:>6,} отзывов")
        except Exception as e:
            print(f"\n⚠️  Данные по брендам недоступны: {e}")
        
        # Последние сессии парсинга
        try:
            last_sessions = await conn.fetch("""
                SELECT тип_парсинга, статус, начало_сессии, конец_сессии
                FROM auto_reviews.сессии_парсинга
                ORDER BY начало_сессии DESC
                LIMIT 3
            """)
            
            if last_sessions:
                print("\n🕒 ПОСЛЕДНИЕ СЕССИИ ПАРСИНГА:")
                for session in last_sessions:
                    session_type = session['тип_парсинга']
                    status = session['статус']
                    start_time = session['начало_сессии']
                    if start_time:
                        print(f"  {session_type:15}: {status:10} ({start_time.strftime('%Y-%m-%d %H:%M')})")
        except Exception as e:
            print(f"\n⚠️  Данные по сессиям недоступны: {e}")
        
        await conn.close()
        print("\n✅ Статистика загружена успешно")
        
    except ImportError:
        print("❌ Модуль asyncpg не установлен")
        print("🔧 Установите: pip install asyncpg")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        print("🔧 Проверьте, что PostgreSQL запущен и доступен")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(show_database_stats())
