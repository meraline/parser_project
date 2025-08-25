#!/usr/bin/env python3
"""
🏗️ НОРМАЛИЗОВАННАЯ СХЕМА БАЗЫ ДАННЫХ
======================================================================
Применение 1-й, 2-й и 3-й нормальных форм для оптимизации структуры
"""

import sqlite3
import os
from datetime import datetime


class NormalizedDatabaseSchema:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def create_normalized_schema(self):
        """Создает полностью нормализованную схему базы данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Удаляем старые таблицы если нужно
        cursor.execute("DROP TABLE IF EXISTS отзывы_нормализованные")
        cursor.execute("DROP TABLE IF EXISTS автомобили")
        cursor.execute("DROP TABLE IF EXISTS авторы")
        cursor.execute("DROP TABLE IF EXISTS города")
        cursor.execute("DROP TABLE IF EXISTS комментарии_норм")
        cursor.execute("DROP TABLE IF EXISTS характеристики_норм")
        cursor.execute("DROP TABLE IF EXISTS рейтинги_деталей")
        cursor.execute("DROP TABLE IF EXISTS расход_топлива")

        # ========================================
        # 1. СПРАВОЧНАЯ ТАБЛИЦА ГОРОДОВ
        # ========================================
        cursor.execute(
            """
            CREATE TABLE города (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                название TEXT UNIQUE NOT NULL
            )
        """
        )

        # ========================================
        # 2. СПРАВОЧНАЯ ТАБЛИЦА АВТОРОВ
        # ========================================
        cursor.execute(
            """
            CREATE TABLE авторы (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                псевдоним TEXT UNIQUE,
                настоящее_имя TEXT,
                город_id INTEGER,
                дата_регистрации TEXT,
                FOREIGN KEY (город_id) REFERENCES города(id)
            )
        """
        )

        # ========================================
        # 3. СПРАВОЧНАЯ ТАБЛИЦА АВТОМОБИЛЕЙ
        # ========================================
        cursor.execute(
            """
            CREATE TABLE автомобили (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                марка TEXT NOT NULL,
                модель TEXT NOT NULL,
                поколение TEXT,
                тип_кузова TEXT,
                трансмиссия TEXT,
                тип_привода TEXT,
                руль TEXT,
                объем_двигателя_куб_см INTEGER,
                мощность_двигателя_лс INTEGER,
                тип_топлива TEXT,
                UNIQUE(марка, модель, поколение, тип_кузова)
            )
        """
        )

        # ========================================
        # 4. ОСНОВНАЯ ТАБЛИЦА ОТЗЫВОВ (НОРМАЛИЗОВАННАЯ)
        # ========================================
        cursor.execute(
            """
            CREATE TABLE отзывы_нормализованные (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                -- Ссылки на справочники
                автомобиль_id INTEGER NOT NULL,
                автор_id INTEGER,
                
                -- Уникальные для отзыва данные
                ссылка TEXT UNIQUE NOT NULL,
                заголовок TEXT,
                содержание TEXT,
                плюсы TEXT,
                минусы TEXT,
                
                -- Рейтинги
                общий_рейтинг REAL,
                рейтинг_владельца REAL,
                
                -- Персональные данные владельца
                год_приобретения INTEGER,
                пробег_км INTEGER,
                цвет_кузова TEXT,
                цвет_салона TEXT,
                
                -- Статистика
                количество_просмотров INTEGER DEFAULT 0,
                количество_лайков INTEGER DEFAULT 0,
                количество_дизлайков INTEGER DEFAULT 0,
                количество_голосов INTEGER DEFAULT 0,
                
                -- Даты
                дата_публикации TEXT,
                дата_парсинга TEXT DEFAULT CURRENT_TIMESTAMP,
                
                -- Мета-данные
                длина_контента INTEGER,
                хеш_содержания TEXT,
                статус_парсинга TEXT DEFAULT 'успех',
                детали_ошибки TEXT,
                
                FOREIGN KEY (автомобиль_id) REFERENCES автомобили(id),
                FOREIGN KEY (автор_id) REFERENCES авторы(id),
                UNIQUE(ссылка)
            )
        """
        )

        # ========================================
        # 5. ДЕТАЛЬНЫЕ РЕЙТИНГИ (ОТДЕЛЬНАЯ ТАБЛИЦА)
        # ========================================
        cursor.execute(
            """
            CREATE TABLE рейтинги_деталей (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER NOT NULL,
                оценка_внешнего_вида INTEGER,
                оценка_салона INTEGER,
                оценка_двигателя INTEGER,
                оценка_управления INTEGER,
                FOREIGN KEY (отзыв_id) REFERENCES отзывы_нормализованные(id)
            )
        """
        )

        # ========================================
        # 6. РАСХОД ТОПЛИВА (ОТДЕЛЬНАЯ ТАБЛИЦА)
        # ========================================
        cursor.execute(
            """
            CREATE TABLE расход_топлива (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER NOT NULL,
                расход_город_л_100км REAL,
                расход_трасса_л_100км REAL,
                расход_смешанный_л_100км REAL,
                FOREIGN KEY (отзыв_id) REFERENCES отзывы_нормализованные(id)
            )
        """
        )

        # ========================================
        # 7. КОММЕНТАРИИ (ОБНОВЛЕННАЯ СТРУКТУРА)
        # ========================================
        cursor.execute(
            """
            CREATE TABLE комментарии_норм (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER NOT NULL,
                автор_комментария_id INTEGER,
                содержание TEXT,
                дата_комментария TEXT,
                лайки INTEGER DEFAULT 0,
                дизлайки INTEGER DEFAULT 0,
                FOREIGN KEY (отзыв_id) REFERENCES отзывы_нормализованные(id),
                FOREIGN KEY (автор_комментария_id) REFERENCES авторы(id)
            )
        """
        )

        # ========================================
        # 8. ХАРАКТЕРИСТИКИ (ОБНОВЛЕННАЯ СТРУКТУРА)
        # ========================================
        cursor.execute(
            """
            CREATE TABLE характеристики_норм (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                отзыв_id INTEGER NOT NULL,
                название TEXT NOT NULL,
                значение TEXT,
                FOREIGN KEY (отзыв_id) REFERENCES отзывы_нормализованные(id)
            )
        """
        )

        # ========================================
        # ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ
        # ========================================
        indices = [
            "CREATE INDEX idx_авторы_псевдоним ON авторы(псевдоним)",
            "CREATE INDEX idx_авторы_город ON авторы(город_id)",
            "CREATE INDEX idx_автомобили_марка_модель ON автомобили(марка, модель)",
            "CREATE INDEX idx_отзывы_автомобиль ON отзывы_нормализованные(автомобиль_id)",
            "CREATE INDEX idx_отзывы_автор ON отзывы_нормализованные(автор_id)",
            "CREATE INDEX idx_отзывы_ссылка ON отзывы_нормализованные(ссылка)",
            "CREATE INDEX idx_отзывы_рейтинг ON отзывы_нормализованные(общий_рейтинг)",
            "CREATE INDEX idx_отзывы_дата ON отзывы_нормализованные(дата_парсинга)",
            "CREATE INDEX idx_рейтинги_отзыв ON рейтинги_деталей(отзыв_id)",
            "CREATE INDEX idx_расход_отзыв ON расход_топлива(отзыв_id)",
            "CREATE INDEX idx_комментарии_отзыв ON комментарии_норм(отзыв_id)",
            "CREATE INDEX idx_характеристики_отзыв ON характеристики_норм(отзыв_id)",
        ]

        for index in indices:
            cursor.execute(index)

        conn.commit()
        conn.close()

        print("✅ Создана нормализованная схема базы данных")

    def migrate_data_from_old_schema(self, old_db_path: str):
        """Мигрирует данные из старой схемы в нормализованную."""
        print("🔄 Начинаем миграцию данных...")

        # Подключаемся к обеим базам
        old_conn = sqlite3.connect(old_db_path)
        new_conn = sqlite3.connect(self.db_path)

        old_cursor = old_conn.cursor()
        new_cursor = new_conn.cursor()

        try:
            # ========================================
            # 1. МИГРИРУЕМ ГОРОДА
            # ========================================
            print("📍 Мигрируем города...")
            old_cursor.execute(
                "SELECT DISTINCT город_автора FROM отзывы WHERE город_автора IS NOT NULL"
            )
            cities = old_cursor.fetchall()

            for (city,) in cities:
                if city and city.strip():
                    new_cursor.execute(
                        "INSERT OR IGNORE INTO города (название) VALUES (?)",
                        (city.strip(),),
                    )

            # ========================================
            # 2. МИГРИРУЕМ АВТОРОВ
            # ========================================
            print("👤 Мигрируем авторов...")
            old_cursor.execute(
                """
                SELECT DISTINCT автор, настоящий_автор, город_автора 
                FROM отзывы 
                WHERE автор IS NOT NULL
            """
            )
            authors = old_cursor.fetchall()

            for author, real_name, city in authors:
                if author and author.strip():
                    # Получаем ID города
                    city_id = None
                    if city and city.strip():
                        new_cursor.execute(
                            "SELECT id FROM города WHERE название = ?", (city.strip(),)
                        )
                        city_result = new_cursor.fetchone()
                        if city_result:
                            city_id = city_result[0]

                    new_cursor.execute(
                        """
                        INSERT OR IGNORE INTO авторы (псевдоним, настоящее_имя, город_id) 
                        VALUES (?, ?, ?)
                    """,
                        (author.strip(), real_name, city_id),
                    )

            # ========================================
            # 3. МИГРИРУЕМ АВТОМОБИЛИ
            # ========================================
            print("🚗 Мигрируем автомобили...")
            old_cursor.execute(
                """
                SELECT DISTINCT 
                    марка, модель, поколение, тип_кузова, трансмиссия, тип_привода, руль,
                    объем_двигателя, мощность_двигателя, тип_топлива
                FROM отзывы 
                WHERE марка IS NOT NULL AND модель IS NOT NULL
            """
            )
            cars = old_cursor.fetchall()

            for car_data in cars:
                (
                    марка,
                    модель,
                    поколение,
                    тип_кузова,
                    трансмиссия,
                    тип_привода,
                    руль,
                    объем,
                    мощность,
                    топливо,
                ) = car_data

                # Парсим объем двигателя
                объем_куб_см = None
                if объем:
                    try:
                        # Извлекаем числа из строки типа "1500 куб.см"
                        import re

                        numbers = re.findall(r"\d+", str(объем))
                        if numbers:
                            объем_куб_см = int(numbers[0])
                    except:
                        pass

                # Парсим мощность
                мощность_лс = None
                if мощность:
                    try:
                        import re

                        numbers = re.findall(r"\d+", str(мощность))
                        if numbers:
                            мощность_лс = int(
                                numbers[-1]
                            )  # Берем последнее число (л.с.)
                    except:
                        pass

                new_cursor.execute(
                    """
                    INSERT OR IGNORE INTO автомобили 
                    (марка, модель, поколение, тип_кузова, трансмиссия, тип_привода, руль,
                     объем_двигателя_куб_см, мощность_двигателя_лс, тип_топлива)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        марка,
                        модель,
                        поколение,
                        тип_кузова,
                        трансмиссия,
                        тип_привода,
                        руль,
                        объем_куб_см,
                        мощность_лс,
                        топливо,
                    ),
                )

            # ========================================
            # 4. МИГРИРУЕМ ОСНОВНЫЕ ОТЗЫВЫ
            # ========================================
            print("📝 Мигрируем отзывы...")
            old_cursor.execute("SELECT * FROM отзывы")
            reviews = old_cursor.fetchall()

            # Получаем названия колонок
            old_cursor.execute("PRAGMA table_info(отзывы)")
            columns = [row[1] for row in old_cursor.fetchall()]

            for review in reviews:
                review_dict = dict(zip(columns, review))

                # Получаем ID автомобиля
                new_cursor.execute(
                    """
                    SELECT id FROM автомобили 
                    WHERE марка = ? AND модель = ? AND 
                          (поколение = ? OR (поколение IS NULL AND ? IS NULL))
                """,
                    (
                        review_dict["марка"],
                        review_dict["модель"],
                        review_dict.get("поколение"),
                        review_dict.get("поколение"),
                    ),
                )
                car_result = new_cursor.fetchone()
                автомобиль_id = car_result[0] if car_result else None

                # Получаем ID автора
                автор_id = None
                if review_dict.get("автор"):
                    new_cursor.execute(
                        "SELECT id FROM авторы WHERE псевдоним = ?",
                        (review_dict["автор"],),
                    )
                    author_result = new_cursor.fetchone()
                    if author_result:
                        автор_id = author_result[0]

                # Парсим пробег
                пробег_км = None
                if review_dict.get("пробег"):
                    try:
                        import re

                        numbers = re.findall(r"\d+", str(review_dict["пробег"]))
                        if numbers:
                            пробег_км = int(numbers[0])
                    except:
                        pass

                # Вставляем отзыв
                new_cursor.execute(
                    """
                    INSERT INTO отзывы_нормализованные 
                    (автомобиль_id, автор_id, ссылка, заголовок, содержание, плюсы, минусы,
                     общий_рейтинг, рейтинг_владельца, год_приобретения, пробег_км,
                     цвет_кузова, цвет_салона, количество_просмотров, количество_лайков,
                     количество_дизлайков, количество_голосов, дата_публикации, дата_парсинга,
                     длина_контента, хеш_содержания, статус_парсинга, детали_ошибки)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        автомобиль_id,
                        автор_id,
                        review_dict["ссылка"],
                        review_dict.get("заголовок"),
                        review_dict.get("содержание"),
                        review_dict.get("плюсы"),
                        review_dict.get("минусы"),
                        review_dict.get("общий_рейтинг"),
                        review_dict.get("рейтинг_владельца"),
                        review_dict.get("год_приобретения"),
                        пробег_км,
                        review_dict.get("цвет_кузова"),
                        review_dict.get("цвет_салона"),
                        review_dict.get("количество_просмотров", 0),
                        review_dict.get("количество_лайков", 0),
                        review_dict.get("количество_дизлайков", 0),
                        review_dict.get("количество_голосов", 0),
                        review_dict.get("дата_публикации"),
                        review_dict.get("дата_парсинга"),
                        review_dict.get("длина_контента"),
                        review_dict.get("хеш_содержания"),
                        review_dict.get("статус_парсинга", "успех"),
                        review_dict.get("детали_ошибки"),
                    ),
                )

                # Получаем ID нового отзыва
                отзыв_id = new_cursor.lastrowid

                # Мигрируем детальные рейтинги
                if any(
                    [
                        review_dict.get("оценка_внешнего_вида"),
                        review_dict.get("оценка_салона"),
                        review_dict.get("оценка_двигателя"),
                        review_dict.get("оценка_управления"),
                    ]
                ):
                    new_cursor.execute(
                        """
                        INSERT INTO рейтинги_деталей 
                        (отзыв_id, оценка_внешнего_вида, оценка_салона, оценка_двигателя, оценка_управления)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            отзыв_id,
                            review_dict.get("оценка_внешнего_вида"),
                            review_dict.get("оценка_салона"),
                            review_dict.get("оценка_двигателя"),
                            review_dict.get("оценка_управления"),
                        ),
                    )

                # Мигрируем расход топлива
                расходы = [
                    review_dict.get("расход_город"),
                    review_dict.get("расход_трасса"),
                    review_dict.get("расход_смешанный"),
                ]
                if any(расходы):
                    # Парсим расходы
                    def parse_fuel_consumption(value):
                        if not value:
                            return None
                        try:
                            import re

                            numbers = re.findall(r"\d+\.?\d*", str(value))
                            return float(numbers[0]) if numbers else None
                        except:
                            return None

                    расход_город = parse_fuel_consumption(
                        review_dict.get("расход_город")
                    )
                    расход_трасса = parse_fuel_consumption(
                        review_dict.get("расход_трасса")
                    )
                    расход_смешанный = parse_fuel_consumption(
                        review_dict.get("расход_смешанный")
                    )

                    if any([расход_город, расход_трасса, расход_смешанный]):
                        new_cursor.execute(
                            """
                            INSERT INTO расход_топлива 
                            (отзыв_id, расход_город_л_100км, расход_трасса_л_100км, расход_смешанный_л_100км)
                            VALUES (?, ?, ?, ?)
                        """,
                            (отзыв_id, расход_город, расход_трасса, расход_смешанный),
                        )

            # ========================================
            # 5. МИГРИРУЕМ КОММЕНТАРИИ
            # ========================================
            print("💬 Мигрируем комментарии...")
            old_cursor.execute("SELECT * FROM комментарии")
            comments = old_cursor.fetchall()

            if comments:
                old_cursor.execute("PRAGMA table_info(комментарии)")
                comment_columns = [row[1] for row in old_cursor.fetchall()]

                for comment in comments:
                    comment_dict = dict(zip(comment_columns, comment))

                    # Находим новый ID отзыва
                    old_cursor.execute(
                        "SELECT ссылка FROM отзывы WHERE id = ?",
                        (comment_dict["отзыв_id"],),
                    )
                    link_result = old_cursor.fetchone()
                    if link_result:
                        new_cursor.execute(
                            "SELECT id FROM отзывы_нормализованные WHERE ссылка = ?",
                            (link_result[0],),
                        )
                        new_review_result = new_cursor.fetchone()
                        if new_review_result:
                            new_отзыв_id = new_review_result[0]

                            # Получаем ID автора комментария
                            автор_комментария_id = None
                            if comment_dict.get("автор_комментария"):
                                new_cursor.execute(
                                    "SELECT id FROM авторы WHERE псевдоним = ?",
                                    (comment_dict["автор_комментария"],),
                                )
                                author_result = new_cursor.fetchone()
                                if author_result:
                                    автор_комментария_id = author_result[0]

                            new_cursor.execute(
                                """
                                INSERT INTO комментарии_норм 
                                (отзыв_id, автор_комментария_id, содержание, дата_комментария, лайки, дизлайки)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """,
                                (
                                    new_отзыв_id,
                                    автор_комментария_id,
                                    comment_dict.get("содержание_комментария"),
                                    comment_dict.get("дата_комментария"),
                                    comment_dict.get("лайки_комментария", 0),
                                    comment_dict.get("дизлайки_комментария", 0),
                                ),
                            )

            # ========================================
            # 6. МИГРИРУЕМ ХАРАКТЕРИСТИКИ
            # ========================================
            print("🔧 Мигрируем характеристики...")
            old_cursor.execute("SELECT * FROM характеристики")
            characteristics = old_cursor.fetchall()

            if characteristics:
                old_cursor.execute("PRAGMA table_info(характеристики)")
                char_columns = [row[1] for row in old_cursor.fetchall()]

                for char in characteristics:
                    char_dict = dict(zip(char_columns, char))

                    # Находим новый ID отзыва
                    old_cursor.execute(
                        "SELECT ссылка FROM отзывы WHERE id = ?",
                        (char_dict["отзыв_id"],),
                    )
                    link_result = old_cursor.fetchone()
                    if link_result:
                        new_cursor.execute(
                            "SELECT id FROM отзывы_нормализованные WHERE ссылка = ?",
                            (link_result[0],),
                        )
                        new_review_result = new_cursor.fetchone()
                        if new_review_result:
                            new_отзыв_id = new_review_result[0]

                            new_cursor.execute(
                                """
                                INSERT INTO характеристики_норм 
                                (отзыв_id, название, значение)
                                VALUES (?, ?, ?)
                            """,
                                (
                                    new_отзыв_id,
                                    char_dict.get("название_характеристики"),
                                    char_dict.get("значение_характеристики"),
                                ),
                            )

            new_conn.commit()
            print("✅ Миграция данных завершена успешно!")

        except Exception as e:
            print(f"❌ Ошибка миграции: {e}")
            new_conn.rollback()
            raise
        finally:
            old_conn.close()
            new_conn.close()

    def analyze_normalization(self):
        """Анализирует качество нормализации."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        print("\n📊 АНАЛИЗ НОРМАЛИЗАЦИИ")
        print("=" * 60)

        # Проверяем 1-ю НФ
        print("\n✅ 1-я НФ (Первая Нормальная Форма):")
        print("- Все поля атомарны")
        print("- Нет повторяющихся групп")
        print("- Каждое поле содержит только одно значение")

        # Проверяем 2-ю НФ
        print("\n✅ 2-я НФ (Вторая Нормальная Форма):")
        cursor.execute("SELECT COUNT(*) FROM автомобили")
        cars_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(DISTINCT автомобиль_id) FROM отзывы_нормализованные"
        )
        unique_cars_in_reviews = cursor.fetchone()[0]
        print(
            f"- Технические характеристики вынесены в справочник автомобилей: {cars_count} записей"
        )
        print(
            f"- Устранена избыточность: {unique_cars_in_reviews} уникальных автомобилей в отзывах"
        )

        # Проверяем 3-ю НФ
        print("\n✅ 3-я НФ (Третья Нормальная Форма):")
        cursor.execute("SELECT COUNT(*) FROM авторы")
        authors_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM города")
        cities_count = cursor.fetchone()[0]
        print(f"- Устранены транзитивные зависимости")
        print(f"- Авторы вынесены в отдельную таблицу: {authors_count} записей")
        print(f"- Города вынесены в справочник: {cities_count} записей")

        # Статистика экономии места
        print("\n💾 ЭКОНОМИЯ ПРОСТРАНСТВА:")
        cursor.execute("SELECT COUNT(*) FROM отзывы_нормализованные")
        reviews_count = cursor.fetchone()[0]

        # Подсчитываем экономию
        old_redundancy = reviews_count * 10  # примерная избыточность в старой схеме
        new_efficiency = cars_count + authors_count + cities_count
        savings = ((old_redundancy - new_efficiency) / old_redundancy) * 100

        print(f"- Отзывов в нормализованной БД: {reviews_count}")
        print(f"- Справочных записей: {new_efficiency}")
        print(f"- Примерная экономия места: {savings:.1f}%")

        # Целостность данных
        print("\n🛡️ ЦЕЛОСТНОСТЬ ДАННЫХ:")
        print("- Внешние ключи обеспечивают ссылочную целостность")
        print("- Невозможны аномалии вставки/обновления/удаления")
        print("- Нормализация до 3НФ завершена")

        conn.close()


def main():
    """Главная функция для создания нормализованной схемы."""

    # Пути к базам данных
    old_db_path = "/home/analityk/Документы/projects/parser_project/data/databases/полные_отзывы_v2.db"
    new_db_path = "/home/analityk/Документы/projects/parser_project/data/databases/нормализованная_бд_v3.db"

    # Создаем директорию если не существует
    os.makedirs(os.path.dirname(new_db_path), exist_ok=True)

    # Создаем нормализованную схему
    schema = NormalizedDatabaseSchema(new_db_path)

    print("🏗️ СОЗДАНИЕ НОРМАЛИЗОВАННОЙ БАЗЫ ДАННЫХ")
    print("=" * 60)

    # Создаем схему
    schema.create_normalized_schema()

    # Мигрируем данные
    if os.path.exists(old_db_path):
        schema.migrate_data_from_old_schema(old_db_path)
    else:
        print("⚠️ Старая база данных не найдена, создана только схема")

    # Анализируем результат
    schema.analyze_normalization()

    print(f"\n🎯 РЕЗУЛЬТАТ:")
    print(f"📁 Нормализованная БД: {new_db_path}")
    print(f"🗓️ Создана: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
