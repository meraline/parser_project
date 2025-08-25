#!/usr/bin/env python3
"""
Скрипт для проверки типов данных в базе данных.
"""

import sys
import os
import sqlite3

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from auto_reviews_parser.database.base import Database


def check_table_schema():
    """Проверяет схему таблицы reviews."""

    db = Database("auto_reviews.db")

    with db.connection() as conn:
        cursor = conn.cursor()

        # Получаем информацию о структуре таблицы
        cursor.execute("PRAGMA table_info(reviews)")
        columns = cursor.fetchall()

        print("Схема таблицы reviews:")
        print("=" * 60)
        for col in columns:
            cid, name, data_type, not_null, default_value, pk = col
            print(
                f"{name:25} | {data_type:15} | NOT NULL: {bool(not_null)} | PK: {bool(pk)}"
            )

        # Проверяем конкретные поля года
        print("\n" + "=" * 60)
        print("Проверка полей с годами:")
        print("=" * 60)

        year_fields = ["год_выпуска", "год_приобретения"]
        for field in year_fields:
            field_info = [col for col in columns if col[1] == field]
            if field_info:
                _, name, data_type, not_null, default_value, pk = field_info[0]
                print(f"{name}: {data_type}")
            else:
                print(f"{field}: НЕ НАЙДЕНО")


def check_actual_data():
    """Проверяет реальные данные в таблице."""

    db = Database("auto_reviews.db")

    with db.connection() as conn:
        cursor = conn.cursor()

        # Проверяем есть ли данные
        cursor.execute("SELECT COUNT(*) FROM reviews")
        count = cursor.fetchone()[0]
        print(f"\nВсего записей в reviews: {count}")

        if count > 0:
            # Получаем несколько записей для проверки
            cursor.execute(
                """
                SELECT марка, модель, год_выпуска, год_приобретения 
                FROM reviews 
                LIMIT 5
            """
            )

            rows = cursor.fetchall()
            print("\nПримеры данных:")
            print("=" * 80)
            print(
                f"{'Марка':<15} | {'Модель':<15} | {'Год выпуска':<12} | {'Год приобр.':<12}"
            )
            print("-" * 80)

            for row in rows:
                brand, model, year, year_purchased = row
                print(
                    f"{brand:<15} | {model:<15} | {str(year):<12} | {str(year_purchased):<12}"
                )
                print(f"Типы: {type(year).__name__}, {type(year_purchased).__name__}")
                print("-" * 40)


def check_sqlite_version():
    """Проверяет версию SQLite."""
    print(f"\nВерсия SQLite: {sqlite3.sqlite_version}")
    print(f"Версия модуля sqlite3: {sqlite3.version}")


if __name__ == "__main__":
    try:
        print("ПРОВЕРКА СХЕМЫ И ДАННЫХ В БД")
        print("=" * 60)

        check_sqlite_version()
        check_table_schema()
        check_actual_data()

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback

        traceback.print_exc()
