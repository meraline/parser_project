#!/usr/bin/env python3
"""
🚀 ПАРСЕР ДЛЯ НОРМАЛИЗОВАННОЙ БАЗЫ ДАННЫХ
======================================================================
Адаптированный парсер для работы с нормализованной схемой (3НФ)
Поддерживает два режима:
- Полный парсинг (--all)
- Случайное количество отзывов (--count N)
"""

import sys
import os
import sqlite3
import argparse
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import re
import random

# Настройка путей
project_root = Path.cwd()
sys.path.insert(0, str(project_root))

from src.auto_reviews_parser.parsers.drom import DromParser


class NormalizedDatabaseParser:
    """Парсер для работы с нормализованной базой данных."""

    def __init__(self, db_path="data/databases/нормализованная_бд_v3.db"):
        """Инициализация парсера."""
        self.db_path = Path(project_root) / db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Инициализируем DromParser с щадящим режимом
        self.drom_parser = DromParser(gentle_mode=True)

        # Проверяем существование нормализованной базы
        if not self.db_path.exists():
            print(f"❌ Нормализованная база данных не найдена: {self.db_path}")
            print("🔧 Создайте ее сначала с помощью normalized_database_schema.py")
            sys.exit(1)

        # Статистика
        self.stats = {
            "всего_url": 0,
            "успешно_спарсено": 0,
            "успешно_сохранено": 0,
            "пропущено_дубликатов": 0,
            "ошибок_парсинга": 0,
            "ошибок_сохранения": 0,
            "начало_работы": datetime.now(),
        }

        print(f"🚀 ПАРСЕР ДЛЯ НОРМАЛИЗОВАННОЙ БД")
        print("=" * 70)
        print(f"📊 База данных: {self.db_path}")
        self.show_database_info()

    def show_database_info(self):
        """Показывает информацию о базе данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Подсчитываем данные в каждой таблице
            tables = [
                ("отзывы_нормализованные", "Отзывы"),
                ("автомобили", "Автомобили"),
                ("авторы", "Авторы"),
                ("города", "Города"),
                ("характеристики_норм", "Характеристики"),
                ("комментарии_норм", "Комментарии"),
                ("рейтинги_деталей", "Рейтинги"),
                ("расход_топлива", "Расход топлива"),
            ]

            print(f"📋 Содержимое базы данных:")
            for table, description in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  • {description}: {count}")
        except Exception as e:
            print(f"⚠️ Ошибка чтения БД: {e}")
        finally:
            conn.close()

    def get_or_create_city(self, city_name: str) -> Optional[int]:
        """Получает или создает ID города."""
        if not city_name or not city_name.strip():
            return None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            city_name = city_name.strip()
            # Проверяем, есть ли город
            cursor.execute("SELECT id FROM города WHERE название = ?", (city_name,))
            result = cursor.fetchone()

            if result:
                return result[0]

            # Создаем новый город
            cursor.execute("INSERT INTO города (название) VALUES (?)", (city_name,))
            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            print(f"❌ Ошибка работы с городом '{city_name}': {e}")
            return None
        finally:
            conn.close()

    def get_or_create_author(
        self, author_name: str, real_name: str, city_name: str
    ) -> Optional[int]:
        """Получает или создает ID автора."""
        if not author_name or not author_name.strip():
            return None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            author_name = author_name.strip()

            # Проверяем, есть ли автор
            cursor.execute("SELECT id FROM авторы WHERE псевдоним = ?", (author_name,))
            result = cursor.fetchone()

            if result:
                return result[0]

            # Получаем ID города
            city_id = self.get_or_create_city(city_name) if city_name else None

            # Создаем нового автора
            cursor.execute(
                """
                INSERT INTO авторы (псевдоним, настоящее_имя, город_id) 
                VALUES (?, ?, ?)
            """,
                (author_name, real_name, city_id),
            )
            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            print(f"❌ Ошибка работы с автором '{author_name}': {e}")
            return None
        finally:
            conn.close()

    def parse_engine_specs(
        self, engine_text: str
    ) -> tuple[Optional[int], Optional[int]]:
        """Парсит характеристики двигателя."""
        if not engine_text:
            return None, None

        # Извлекаем объем в куб.см
        volume_match = re.search(r"(\d+)\s*куб\.см", engine_text)
        volume = int(volume_match.group(1)) if volume_match else None

        # Извлекаем мощность в л.с.
        power_match = re.search(r"(\d+)\s*л\.с\.", engine_text)
        power = int(power_match.group(1)) if power_match else None

        return volume, power

    def get_or_create_car(self, car_data: Dict) -> Optional[int]:
        """Получает или создает ID автомобиля."""
        if not car_data.get("марка") or not car_data.get("модель"):
            return None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            марка = car_data["марка"]
            модель = car_data["модель"]
            поколение = car_data.get("поколение")
            тип_кузова = car_data.get("тип_кузова")

            # Проверяем, есть ли автомобиль
            cursor.execute(
                """
                SELECT id FROM автомобили 
                WHERE марка = ? AND модель = ? AND 
                      (поколение = ? OR (поколение IS NULL AND ? IS NULL)) AND
                      (тип_кузова = ? OR (тип_кузова IS NULL AND ? IS NULL))
            """,
                (марка, модель, поколение, поколение, тип_кузова, тип_кузова),
            )
            result = cursor.fetchone()

            if result:
                return result[0]

            # Парсим характеристики двигателя
            engine_text = car_data.get("двигатель", "")
            объем_куб_см, мощность_лс = self.parse_engine_specs(engine_text)

            # Создаем новый автомобиль
            cursor.execute(
                """
                INSERT INTO автомобили 
                (марка, модель, поколение, тип_кузова, трансмиссия, тип_привода, руль,
                 объем_двигателя_куб_см, мощность_двигателя_лс, тип_топлива)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    марка,
                    модель,
                    поколение,
                    тип_кузова,
                    car_data.get("трансмиссия"),
                    car_data.get("тип_привода"),
                    car_data.get("руль"),
                    объем_куб_см,
                    мощность_лс,
                    car_data.get("тип_топлива"),
                ),
            )
            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            print(f"❌ Ошибка работы с автомобилем '{марка} {модель}': {e}")
            return None
        finally:
            conn.close()

    def parse_mileage(self, mileage_text: str) -> Optional[int]:
        """Парсит пробег в километрах."""
        if not mileage_text:
            return None
        try:
            # Извлекаем числа из строки типа "125 000 км"
            numbers = re.findall(r"\d+", str(mileage_text))
            if numbers:
                # Объединяем числа и конвертируем в километры
                mileage_str = "".join(numbers)
                return int(mileage_str)
        except:
            pass
        return None

    def parse_fuel_consumption(self, consumption_text: str) -> Optional[float]:
        """Парсит расход топлива."""
        if not consumption_text:
            return None
        try:
            # Извлекаем число из строки типа "10.0 л/100км"
            match = re.search(r"(\d+\.?\d*)", str(consumption_text))
            return float(match.group(1)) if match else None
        except:
            return None

    def save_review_to_normalized_db(self, review_data: Dict) -> bool:
        """Сохраняет отзыв в нормализованную базу данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 1. Получаем или создаем автомобиль
            car_data = {
                "марка": review_data.get("марка"),
                "модель": review_data.get("модель"),
                "поколение": review_data.get("поколение"),
                "тип_кузова": review_data.get("тип_кузова"),
                "трансмиссия": review_data.get("трансмиссия"),
                "тип_привода": review_data.get("тип_привода"),
                "руль": review_data.get("руль"),
                "двигатель": review_data.get("двигатель"),
                "тип_топлива": review_data.get("тип_топлива"),
            }
            автомобиль_id = self.get_or_create_car(car_data)

            if not автомобиль_id:
                print(f"❌ Не удалось создать автомобиль для отзыва")
                return False

            # 2. Получаем или создаем автора
            автор_id = self.get_or_create_author(
                review_data.get("автор", ""),
                review_data.get("настоящий_автор", ""),
                review_data.get("город_автора", ""),
            )

            # 3. Парсим пробег
            пробег_км = self.parse_mileage(review_data.get("пробег"))

            # 4. Сохраняем основной отзыв
            cursor.execute(
                """
                INSERT INTO отзывы_нормализованные
                (автомобиль_id, автор_id, ссылка, заголовок, содержание, плюсы, минусы,
                 общий_рейтинг, рейтинг_владельца, год_приобретения, пробег_км,
                 цвет_кузова, цвет_салона, количество_просмотров, количество_лайков,
                 количество_дизлайков, количество_голосов, дата_публикации, дата_парсинга,
                 длина_контента, хеш_содержания, статус_парсинга)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    автомобиль_id,
                    автор_id,
                    review_data.get("ссылка"),
                    review_data.get("заголовок"),
                    review_data.get("содержание"),
                    review_data.get("плюсы"),
                    review_data.get("минусы"),
                    review_data.get("общий_рейтинг"),
                    review_data.get("рейтинг_владельца"),
                    review_data.get("год_приобретения"),
                    пробег_км,
                    review_data.get("цвет_кузова"),
                    review_data.get("цвет_салона"),
                    review_data.get("количество_просмотров", 0),
                    review_data.get("количество_лайков", 0),
                    review_data.get("количество_дизлайков", 0),
                    review_data.get("количество_голосов", 0),
                    review_data.get("дата_публикации"),
                    datetime.now().isoformat(),
                    review_data.get("длина_контента"),
                    review_data.get("хеш_содержания"),
                    "успех",
                ),
            )

            отзыв_id = cursor.lastrowid

            # 5. Сохраняем детальные рейтинги
            if any(
                [
                    review_data.get("оценка_внешнего_вида"),
                    review_data.get("оценка_салона"),
                    review_data.get("оценка_двигателя"),
                    review_data.get("оценка_управления"),
                ]
            ):
                cursor.execute(
                    """
                    INSERT INTO рейтинги_деталей 
                    (отзыв_id, оценка_внешнего_вида, оценка_салона, оценка_двигателя, оценка_управления)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        отзыв_id,
                        review_data.get("оценка_внешнего_вида"),
                        review_data.get("оценка_салона"),
                        review_data.get("оценка_двигателя"),
                        review_data.get("оценка_управления"),
                    ),
                )

            # 6. Сохраняем расход топлива
            расход_город = self.parse_fuel_consumption(review_data.get("расход_город"))
            расход_трасса = self.parse_fuel_consumption(
                review_data.get("расход_трасса")
            )
            расход_смешанный = self.parse_fuel_consumption(
                review_data.get("расход_смешанный")
            )

            if any([расход_город, расход_трасса, расход_смешанный]):
                cursor.execute(
                    """
                    INSERT INTO расход_топлива 
                    (отзыв_id, расход_город_л_100км, расход_трасса_л_100км, расход_смешанный_л_100км)
                    VALUES (?, ?, ?, ?)
                """,
                    (отзыв_id, расход_город, расход_трасса, расход_смешанный),
                )

            # 7. Сохраняем характеристики
            for characteristic in review_data.get("характеристики", []):
                cursor.execute(
                    """
                    INSERT INTO характеристики_норм (отзыв_id, название, значение)
                    VALUES (?, ?, ?)
                """,
                    (
                        отзыв_id,
                        characteristic.get("название"),
                        characteristic.get("значение"),
                    ),
                )

            # 8. Сохраняем комментарии
            for comment in review_data.get("комментарии", []):
                автор_комментария_id = (
                    self.get_or_create_author(comment.get("автор", ""), "", "")
                    if comment.get("автор")
                    else None
                )

                cursor.execute(
                    """
                    INSERT INTO комментарии_норм 
                    (отзыв_id, автор_комментария_id, содержание, дата_комментария, лайки, дизлайки)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        отзыв_id,
                        автор_комментария_id,
                        comment.get("содержание"),
                        comment.get("дата"),
                        comment.get("лайки", 0),
                        comment.get("дизлайки", 0),
                    ),
                )

            conn.commit()
            return True

        except Exception as e:
            print(f"❌ Ошибка сохранения в нормализованную БД: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def generate_content_hash(self, content: str) -> str:
        """Генерирует хеш содержания отзыва."""
        if not content:
            return ""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def is_review_exists(self, url: str) -> tuple[bool, bool]:
        """
        Проверяет существование отзыва и полноту данных.

        Returns:
            tuple[bool, bool]: (существует, данные_полные)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT id, заголовок, содержание, плюсы, минусы, автомобиль_id, автор_id
                FROM отзывы_нормализованные 
                WHERE ссылка = ?
            """,
                (url,),
            )
            result = cursor.fetchone()

            if not result:
                return False, False

            # Проверяем полноту данных
            review_id, title, content, pros, cons, car_id, author_id = result

            # Считаем данные полными, если есть основные поля
            # Заголовок, плюсы, минусы не обязательны - не все отзывы их имеют
            is_complete = all(
                [
                    content and len(content.strip()) > 100,  # Основное содержание
                    car_id is not None,  # Связь с автомобилем
                    author_id is not None,  # Связь с автором
                ]
            )

            return True, is_complete

        except Exception as e:
            print(f"❌ Ошибка проверки отзыва: {e}")
            return False, False
        finally:
            conn.close()

    def delete_incomplete_review(self, url: str) -> bool:
        """Удаляет неполный отзыв для перезаписи."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                "DELETE FROM отзывы_нормализованные WHERE ссылка = ?", (url,)
            )
            conn.commit()
            print(f"🗑️ Удален неполный отзыв: {url}")
            return True
        except Exception as e:
            print(f"❌ Ошибка удаления отзыва: {e}")
            return False
        finally:
            conn.close()

    def parse_single_review(self, url: str) -> bool:
        """Парсит один отзыв и сохраняет в нормализованную БД с проверкой дубликатов."""
        try:
            print(f"🔍 Парсинг: {url}")

            # Проверяем существование и полноту данных
            exists, is_complete = self.is_review_exists(url)

            if exists and is_complete:
                print(f"⏭️ Пропускаем (уже есть полные данные): {url}")
                self.stats["пропущено_дубликатов"] += 1
                return True
            elif exists and not is_complete:
                print(f"🔄 Перезаписываем неполные данные: {url}")
                self.delete_incomplete_review(url)

            # Используем DromParser для извлечения данных
            reviews = self.drom_parser.parse_single_review(url)
            if not reviews:
                print(f"❌ Не удалось извлечь данные")
                return False

            review = reviews[0]

            # Преобразуем в формат для нормализованной БД
            review_data = {
                "ссылка": url,
                "марка": review.brand,
                "модель": review.model,
                "поколение": review.generation,
                "заголовок": review.title,
                "содержание": review.content,
                "плюсы": review.pros,
                "минусы": review.cons,
                "автор": review.author,
                "настоящий_автор": review.author,
                "город_автора": getattr(review, "author_city", ""),
                "общий_рейтинг": review.rating,
                "рейтинг_владельца": review.rating,  # Это одно и то же
                "количество_просмотров": getattr(
                    review, "views", getattr(review, "views_count", 0)
                ),
                "дата_публикации": review.publish_date,
                "длина_контента": len(review.content) if review.content else 0,
                "год_приобретения": getattr(
                    review, "year_acquired", review.year_purchased
                ),
                "пробег": getattr(review, "mileage", None),
                "тип_кузова": getattr(review, "body_type", None),
                "трансмиссия": getattr(review, "transmission", None),
                "тип_привода": getattr(review, "drive_type", None),
                "руль": getattr(review, "steering_wheel", None),
                "двигатель": getattr(review, "engine", None),
                "тип_топлива": getattr(review, "fuel_type", None),
                "хеш_содержания": self.generate_content_hash(review.content),
                "характеристики": [],
                "комментарии": [],
            }

            # Добавляем технические характеристики
            if hasattr(review, "technical_specs") and review.technical_specs:
                for key, value in review.technical_specs.items():
                    review_data["характеристики"].append(
                        {"название": key, "значение": str(value)}
                    )

            # Сохраняем в нормализованную БД
            success = self.save_review_to_normalized_db(review_data)

            if success:
                self.stats["успешно_сохранено"] += 1
                print(f"✅ Сохранено: {review.brand} {review.model}")
            else:
                self.stats["ошибок_сохранения"] += 1

            return success

        except Exception as e:
            print(f"❌ Ошибка парсинга: {e}")
            self.stats["ошибок_парсинга"] += 1
            return False

    def _get_review_urls_safe(self, brand: str, max_urls: int = 100):
        """
        Безопасно получает список URL отзывов без потери контекста Playwright.
        Использует отдельный браузер только для получения списка ссылок.
        """
        from playwright.sync_api import sync_playwright

        review_urls = []

        with sync_playwright() as p:
            # Используем локально установленный браузер
            chrome_path = (
                "/home/analityk/Документы/projects/parser_project/chrome-linux/chrome"
            )
            browser = p.chromium.launch(headless=True, executable_path=chrome_path)
            page = browser.new_page()

            try:
                # Формируем URL каталога
                catalog_url = f"https://www.drom.ru/reviews/{brand}/"
                print(f"    Открываем каталог: {catalog_url}")

                page.goto(catalog_url, wait_until="networkidle")
                page.wait_for_timeout(2000)

                # Разворачиваем дополнительные отзывы если есть
                expand_buttons = page.query_selector_all(
                    'button[data-ftid="component_reviews-list_show-more"]'
                )
                print(f"    Найдено кнопок для разворачивания: {len(expand_buttons)}")

                for i, button in enumerate(expand_buttons[:3]):  # Максимум 3 раза
                    try:
                        if button.is_visible():
                            print(f"    Кликнули на кнопку #{i+1}")
                            button.click()
                            page.wait_for_timeout(2000)
                    except Exception:
                        break

                # Получаем все ссылки на отзывы
                # Получаем все ссылки на отзывы - попробуем разные селекторы
                all_links = page.query_selector_all('a[href*="/reviews/"]')
                print(f"    Всего ссылок с reviews: {len(all_links)}")

                # Проверим первые 10 ссылок для отладки
                for i, link in enumerate(all_links[:10]):
                    href = link.get_attribute("href")
                    text = link.text_content()[:50] if link.text_content() else ""
                    print(f"    Ссылка {i+1}: {href} - '{text}'")

                # Шаг 1: Собираем ссылки на модели
                model_links = []
                for link in all_links:
                    href = link.get_attribute("href")
                    if not href:
                        continue

                    # Ищем ссылки на модели: /reviews/brand/model/
                    parts = href.strip("/").split("/")
                    if (
                        len(parts) == 3
                        and parts[0] == "reviews"
                        and parts[1] == brand.lower()
                    ):

                        if not href.startswith("http"):
                            href = f"https://www.drom.ru{href}"
                        model_links.append(href)

                print(f"    Найдено моделей: {len(model_links)}")

                # Шаг 2: Заходим на страницы моделей и ищем отзывы
                for model_url in model_links[:5]:  # Ограничиваем для начала
                    if len(review_urls) >= max_urls:
                        break

                    print(f"    Проверяем модель: {model_url}")
                    try:
                        page.goto(model_url, wait_until="networkidle")
                        page.wait_for_timeout(1000)

                        # Ищем отзывы на странице модели
                        model_reviews = page.query_selector_all('a[href*="/reviews/"]')

                        for review_link in model_reviews:
                            if len(review_urls) >= max_urls:
                                break

                            href = review_link.get_attribute("href")
                            if not href:
                                continue

                            # Проверяем что это конкретный отзыв с ID
                            parts = href.strip("/").split("/")
                            if (
                                len(parts) >= 4
                                and parts[0] == "reviews"
                                and parts[-1].isdigit()
                            ):

                                if not href.startswith("http"):
                                    href = f"https://www.drom.ru{href}"

                                if href not in review_urls:
                                    review_urls.append(href)
                                    print(f"    ✅ Найден отзыв: {href}")

                    except Exception as e:
                        print(f"    ⚠️ Ошибка на странице модели: {e}")
                        continue

                print(f"    Собрано URL отзывов: {len(review_urls)}")

            except Exception as e:
                print(f"    ⚠️ Ошибка получения списка: {e}")
            finally:
                browser.close()

        return review_urls

    def parse_brand_reviews(
        self, brand: str, mode: str = "count", max_reviews: int = 30
    ):
        """
        Парсит отзывы бренда в двух режимах:
        - mode="all": парсит все найденные отзывы последовательно
        - mode="count": парсит первые N отзывов по порядку (не случайно!)

        Пропускает дубликаты, перезаписывает неполные данные.
        """
        print(f"\n🚀 ПАРСИНГ БРЕНДА: {brand.upper()}")
        if mode == "all":
            print("📊 РЕЖИМ: Полный последовательный парсинг всех отзывов")
        else:
            print(f"📊 РЕЖИМ: Последовательная выборка ({max_reviews} отзывов)")
        print("=" * 70)

        try:
            # Получаем список URL отзывов более безопасным способом
            print(f"🔍 Получаем список отзывов для бренда {brand}...")

            review_urls = self._get_review_urls_safe(
                brand, max_reviews if mode != "all" else 10000
            )

            if mode == "all":
                print(f"📋 Найдено отзывов для полного парсинга: {len(review_urls)}")
                reviews_to_process = review_urls
            else:
                # Берем первые отзывы по порядку (НЕ случайно!)
                reviews_to_process = review_urls[:max_reviews]
                print(f"📋 Найдено отзывов: {len(review_urls)}")
                print(
                    f"📝 Будет обработано последовательно: {len(reviews_to_process)} отзывов"
                )

            # Последовательно обрабатываем отзывы
            processed_count = 0
            skipped_count = 0
            rewritten_count = 0

            for i, review_url in enumerate(reviews_to_process, 1):
                print(f"\n📝 [{i}/{len(reviews_to_process)}] {review_url}")
                self.stats["всего_url"] += 1

                # Проверяем существование и полноту данных
                exists, is_complete = self.is_review_exists(review_url)

                if exists and is_complete:
                    print(f"⏭️ Пропускаем (уже есть полные данные)")
                    skipped_count += 1
                    self.stats["пропущено_дубликатов"] += 1
                    continue
                elif exists and not is_complete:
                    print(f"🔄 Перезаписываем неполные данные")
                    self.delete_incomplete_review(review_url)
                    rewritten_count += 1

                # Парсим отзыв по URL
                success = self.parse_single_review(review_url)
                if success:
                    processed_count += 1

            # Показываем детальную статистику
            print(f"\n📊 РЕЗУЛЬТАТЫ ПОСЛЕДОВАТЕЛЬНОЙ ОБРАБОТКИ:")
            print(f"  • Обработано новых: {processed_count}")
            print(f"  • Пропущено дубликатов: {skipped_count}")
            print(f"  • Перезаписано неполных: {rewritten_count}")

            self.show_final_stats()

        except Exception as e:
            print(f"❌ Ошибка парсинга бренда: {e}")
            import traceback

            traceback.print_exc()

    def show_final_stats(self):
        """Показывает финальную статистику."""
        elapsed = datetime.now() - self.stats["начало_работы"]

        print("\n" + "=" * 70)
        print("📊 СТАТИСТИКА ПАРСИНГА В НОРМАЛИЗОВАННУЮ БД")
        print("=" * 70)
        print(f"🔗 Всего URL: {self.stats['всего_url']}")
        print(f"✅ Успешно спарсено: {self.stats['успешно_спарсено']}")
        print(f"💾 Сохранено: {self.stats['успешно_сохранено']}")
        print(f"❌ Ошибок парсинга: {self.stats['ошибок_парсинга']}")
        print(f"❌ Ошибок сохранения: {self.stats['ошибок_сохранения']}")
        print(f"⏱️ Время работы: {elapsed}")

        # Показываем обновленную статистику БД
        print(f"\n📊 СОДЕРЖИМОЕ БД ПОСЛЕ ПАРСИНГА:")
        self.show_database_info()


def parse_arguments():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="Парсер отзывов для нормализованной базы данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python normalized_parser.py --brand toyota --all          # Все отзывы Toyota последовательно
  python normalized_parser.py --brand mazda --count 50      # 50 первых отзывов Mazda по порядку
  python normalized_parser.py --url "https://..."           # Один отзыв по URL
  python normalized_parser.py --stats                       # Статистика БД
        """,
    )

    # Группа для выбора режима
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--brand", type=str, help="Название бренда для парсинга")
    mode_group.add_argument("--url", type=str, help="URL конкретного отзыва")
    mode_group.add_argument(
        "--stats", action="store_true", help="Показать статистику БД"
    )

    # Группа для режима парсинга бренда
    parse_group = parser.add_mutually_exclusive_group()
    parse_group.add_argument(
        "--all", action="store_true", help="Парсить все отзывы бренда"
    )
    parse_group.add_argument(
        "--count",
        type=int,
        default=30,
        help="Количество первых отзывов по порядку (по умолчанию: 30)",
    )

    return parser.parse_args()


def main():
    """Главная функция."""
    args = parse_arguments()
    parser_instance = NormalizedDatabaseParser()

    if args.stats:
        # Показать статистику БД
        parser_instance.show_database_info()

    elif args.url:
        # Парсить один отзыв
        print(f"🔍 Парсинг одного отзыва: {args.url}")
        parser_instance.parse_single_review(args.url)

    elif args.brand:
        # Парсить бренд
        brand = args.brand.lower().strip()

        if args.all:
            # Режим "все отзывы"
            print(f"🚀 ПОЛНЫЙ ПАРСИНГ БРЕНДА: {brand.upper()}")
            parser_instance.parse_brand_reviews(brand, mode="all")
        else:
            # Режим "случайное количество"
            count = args.count
            print(f"🎲 СЛУЧАЙНАЯ ВЫБОРКА: {brand.upper()} ({count} отзывов)")
            parser_instance.parse_brand_reviews(brand, mode="count", max_reviews=count)


def interactive_main():
    """Интерактивный режим (для обратной совместимости)."""
    parser_instance = NormalizedDatabaseParser()

    print("\nВыберите действие:")
    print("1. Парсить один отзыв")
    print("2. Парсить бренд (случайное количество)")
    print("3. Парсить бренд (все отзывы)")
    print("4. Показать статистику БД")

    choice = input("\nВведите номер (1-4): ").strip()

    if choice == "1":
        url = input("Введите URL отзыва: ").strip()
        parser_instance.parse_single_review(url)
    elif choice == "2":
        brand = input("Введите название бренда: ").strip().lower()
        max_reviews = int(input("Количество отзывов (по умолчанию 30): ") or "30")
        parser_instance.parse_brand_reviews(
            brand, mode="count", max_reviews=max_reviews
        )
    elif choice == "3":
        brand = input("Введите название бренда: ").strip().lower()
        parser_instance.parse_brand_reviews(brand, mode="all")
    elif choice == "4":
        parser_instance.show_database_info()
    else:
        print("❌ Неверный выбор")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Режим с аргументами командной строки
        main()
    else:
        # Интерактивный режим
        interactive_main()
