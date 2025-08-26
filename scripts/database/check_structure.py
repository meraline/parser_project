#!/usr/bin/env python3
"""
Скрипт для проверки структуры базы данных PostgreSQL
"""
import asyncio
import asyncpg

async def check_database_structure():
    """Проверяет структуру базы данных"""
    try:
        # Подключение к базе данных
        conn = await asyncpg.connect(
            host='localhost',
            port=5432,
            user='parser',
            password='parser',
            database='auto_reviews'
        )
        
        print("✅ Подключение к базе данных успешно")
        print("=" * 60)
        
        # Получаем список всех таблиц в схеме auto_reviews
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'auto_reviews' 
        ORDER BY table_name;
        """
        
        tables = await conn.fetch(tables_query)
        print(f"📋 Найдено таблиц в схеме auto_reviews: {len(tables)}")
        print("-" * 40)
        
        for table in tables:
            table_name = table['table_name']
            print(f"📊 Таблица: {table_name}")
            
            # Получаем количество записей
            count_query = f"SELECT COUNT(*) FROM auto_reviews.{table_name};"
            try:
                count_result = await conn.fetchval(count_query)
                print(f"   📈 Записей: {count_result}")
            except Exception as e:
                print(f"   ❌ Ошибка подсчета: {e}")
            
            # Получаем структуру таблицы
            columns_query = """
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_schema = 'auto_reviews' AND table_name = $1
            ORDER BY ordinal_position;
            """
            
            try:
                columns = await conn.fetch(columns_query, table_name)
                print(f"   🏗️  Колонки ({len(columns)}):")
                for col in columns:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    print(f"      - {col['column_name']}: {col['data_type']} ({nullable})")
            except Exception as e:
                print(f"   ❌ Ошибка получения структуры: {e}")
            
            print()
        
        # Проверяем внешние ключи
        fk_query = """
        SELECT 
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'auto_reviews'
        ORDER BY tc.table_name, tc.constraint_name;
        """
        
        fks = await conn.fetch(fk_query)
        if fks:
            print("🔗 Внешние ключи:")
            print("-" * 40)
            for fk in fks:
                print(f"   {fk['table_name']}.{fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}")
        else:
            print("❌ Внешние ключи не найдены")
        
        await conn.close()
        print("\n✅ Проверка завершена")
        
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(check_database_structure())
