#!/usr/bin/env python3
"""
ВОССТАНОВЛЕННЫЙ ПОЛНЫЙ ПАРСЕР АВТО-ОТЗЫВОВ
==========================================

Полнофункциональная система парсинга отзывов с drom.ru по брендам
в алфавитном порядке с сохранением ВСЕХ параметров и данных.

Особенности:
- Парсинг в алфавитном порядке по брендам
- ВСЕ отзывы бренда → следующий бренд
- Полная информация: рейтинги, характеристики, комментарии
- Дубликаты проверяются через БД
- Автосохранение каждого успешного отзыва
- Подробная статистика и логирование

ВАЖНО: Этот парсер НЕ изменяется без явного указания!
"""

ВАЖНО: Этот парсер НЕ изменяется без явного указания!
"""

import time
import sqlite3
import traceback
from typing import Set, List, Dict, Any
from pathlib import Path
from datetime import datetime

# Добавляем системный путь
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.auto_reviews_parser.parsers.drom import DromParser
from src.auto_reviews_parser.database.base import Database


class ComprehensiveBrandParser:
    """Полнофункциональный парсер для извлечения всех отзывов по брендам."""

    def __init__(self, mode: str = "test"):
        """
        Инициализация парсера.

        Args:
            mode: Режим работы - "test" или "production"
        """
        # Алфавитный список всех поддерживаемых брендов
        self.brands = self._get_brands_alphabetical()

        # Инициализация парсера с щадящим режимом
        self.parser = DromParser(gentle_mode=True)

        # ПРАВИЛЬНЫЕ пути к базам данных (только 2 базы!)
        if mode == "production":
            self.db_path = "auto_reviews.db"  # Боевая база
        else:
            self.db_path = "data/databases/test_reviews.db"  # Тестовая база

        Path(self.db_path).parent.mkdir(
            parents=True, exist_ok=True
        )  # Подробная статистика
        self.stats = {
            "session_start": datetime.now().isoformat(),
            "start_time": time.time(),
            "total_parsed": 0,
            "successful_parsed": 0,
            "failed_parsed": 0,
            "comments_parsed": 0,
            "current_brand": "",
            "current_model": "",
            "brands_completed": 0,
            "last_save_time": time.time(),
            "save_count": 0,
            "duplicates_skipped": 0,
            "errors": [],
        }

        # Конфигурация парсинга
        self.config = {
            "save_interval": 1,  # Сохранять каждый отзыв
            "max_retries": 3,  # Попытки при ошибках
            "delay_between_requests": 2.0,  # Задержка между запросами (сек)
            "delay_between_models": 5.0,  # Задержка между моделями (сек)
            "delay_between_brands": 10.0,  # Задержка между брендами (сек)
            "enable_comments": True,  # Парсить комментарии
            "enable_characteristics": True,  # Парсить характеристики
            "enable_ratings": True,  # Парсить детальные рейтинги
        }

        # Инициализация БД
        self._init_comprehensive_database()

    def _get_brands_alphabetical(self) -> List[str]:
        """Полный алфавитный список брендов для парсинга согласно порядку с сайта."""
        brands = [
            # Латинские бренды (строго по алфавиту с сайта)
            "ac",
            "aito",
            "acura",
            "alfa_romeo",
            "alpina",
            "alpine",
            "arcfox",
            "aro",
            "asia",
            "aston_martin",
            "audi",
            "avatr",
            "baic",
            "baw",
            "bmw",
            "byd",
            "baojun",
            "belgee",
            "bentley",
            "brilliance",
            "bugatti",
            "buick",
            "cadillac",
            "changan",
            "changhe",
            "chery",
            "chevrolet",
            "chrysler",
            "ciimo",
            "citroen",
            "dw_hower",
            "dacia",
            "dadi",
            "daewoo",
            "daihatsu",
            "daimler",
            "datsun",
            "dayun",
            "denza",
            "derways",
            "dodge",
            "dongfeng",
            "cheryexeed",
            "eagle",
            "faw",
            "ferrari",
            "fiat",
            "ford",
            "forthing",
            "foton",
            "freightliner",
            "gac",
            "gmc",
            "geely",
            "genesis",
            "geo",
            "great_wall",
            "hafei",
            "haima",
            "haval",
            "hawtai",
            "hiphi",
            "higer",
            "hino",
            "honda",
            "hongqi",
            "howo",
            "hozon",
            "huanghai",
            "hummer",
            "hyundai",
            "im_motors",
            "iveco",
            "infiniti",
            "iran_khodro",
            "isuzu",
            "jac",
            "jmc",
            "jmev",
            "jaecoo",
            "jaguar",
            "jeep",
            "jetour",
            "jetta",
            "jidu",
            "kaiyi",
            "kia",
            "knewstar",
            "koenigsegg",
            "kuayue",
            "lamborghini",
            "lancia",
            "land_rover",
            "leapmotor",
            "lexus",
            "li",
            "lifan",
            "lincoln",
            "livan",
            "lotus",
            "luxeed",
            "luxgen",
            "lynk_and_co",
            "m-hero",
            "mg",
            "mini",
            "marussia",
            "maserati",
            "maxus",
            "maybach",
            "mazda",
            "mclaren",
            "mercedes-benz",
            "mercury",
            "mitsubishi",
            "mitsuoka",
            "nio",
            "nissan",
            "omoda",
            "ora",
            "oldsmobile",
            "opel",
            "oshan",
            "oting",
            "peugeot",
            "plymouth",
            "polar_stone",
            "polestar",
            "pontiac",
            "porsche",
            "proton",
            "ram",
            "radar",
            "ravon",
            "renault",
            "renault_samsung",
            "rising_auto",
            "rivian",
            "roewe",
            "rolls-royce",
            "rover",
            "rox",
            "seat",
            "swm",
            "saab",
            "saturn",
            "scania",
            "scion",
            "seres",
            "shuanghuan",
            "skoda",
            "skywell",
            "smart",
            "solaris-agr",
            "soueast",
            "ssang_yong",
            "subaru",
            "suzuki",
            "tata",
            "tank",
            "tesla",
            "tianma",
            "tianye",
            "toyota",
            "trabant",
            "vgv",
            "venucia",
            "volkswagen",
            "volvo",
            "vortex",
            "voyah",
            "wey",
            "wartburg",
            "weltmeister",
            "wuling",
            "xcite",
            "xiaomi",
            "xin_kai",
            "xpeng",
            "zx",
            "zeekr",
            "zotye",
            "icar",
            # Русские бренды (строго по алфавиту с сайта)
            "amber",
            "aurus",
            "bogdan",
            "gaz",
            "doninvest",
            "zaz",
            "zil",
            "zis",
            "izh",
            "kamaz",
            "lada",
            "luaz",
            "moskvitch",
            "other",
            "raf",
            "sollers",
            "tagaz",
            "uaz",
            "evolute",
        ]
        return brands

    def _init_comprehensive_database(self):
        """Создание полной схемы базы данных с ВСЕМИ полями."""
        with sqlite3.connect(self.db_path) as conn:
            # Основная таблица отзывов
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS comprehensive_reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    
                    -- Основные поля
                    url TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL DEFAULT 'drom.ru',
                    brand TEXT NOT NULL,
                    model TEXT NOT NULL,
                    
                    -- Контент отзыва
                    title TEXT,
                    content TEXT,
                    author TEXT,
                    author_city TEXT,
                    date_published TEXT,
                    date_parsed TEXT DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Рейтинги
                    overall_rating REAL,
                    owner_rating REAL,
                    views_count INTEGER DEFAULT 0,
                    
                    -- Детальные оценки (JSON или отдельные поля)
                    exterior_rating INTEGER,
                    interior_rating INTEGER,
                    engine_rating INTEGER,
                    handling_rating INTEGER,
                    comfort_rating INTEGER,
                    reliability_rating INTEGER,
                    
                    -- Технические характеристики
                    year INTEGER,
                    generation TEXT,
                    body_type TEXT,
                    transmission TEXT,
                    drive_type TEXT,
                    steering_wheel TEXT,
                    mileage INTEGER,
                    engine_volume REAL,
                    engine_power INTEGER,
                    fuel_type TEXT,
                    fuel_consumption_city REAL,
                    fuel_consumption_highway REAL,
                    fuel_consumption_mixed REAL,
                    
                    -- Дополнительные характеристики
                    purchase_year INTEGER,
                    ownership_duration INTEGER,
                    color_exterior TEXT,
                    color_interior TEXT,
                    price_purchase REAL,
                    price_current REAL,
                    
                    -- Метаданные
                    comments_count INTEGER DEFAULT 0,
                    photos_count INTEGER DEFAULT 0,
                    review_type TEXT DEFAULT 'review',
                    parsing_status TEXT DEFAULT 'success',
                    parsing_errors TEXT
                )
            """
            )

            # Создаем индексы отдельно
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_brand_model ON comprehensive_reviews(brand, model)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_date_parsed ON comprehensive_reviews(date_parsed)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rating ON comprehensive_reviews(overall_rating)"
            )

            # Таблица комментариев
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS review_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL,
                    review_url TEXT NOT NULL,
                    
                    author TEXT,
                    content TEXT,
                    date_published TEXT,
                    date_parsed TEXT DEFAULT CURRENT_TIMESTAMP,
                    
                    likes_count INTEGER DEFAULT 0,
                    
                    FOREIGN KEY (review_id) REFERENCES comprehensive_reviews (id)
                )
            """
            )

            # Создаем индексы для комментариев
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_review_id ON review_comments(review_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_comments_url ON review_comments(review_url)"
            )

            # Таблица статистики парсинга
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsing_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_start TEXT NOT NULL,
                    session_end TEXT,
                    target_reviews INTEGER,
                    total_parsed INTEGER DEFAULT 0,
                    successful_parsed INTEGER DEFAULT 0,
                    failed_parsed INTEGER DEFAULT 0,
                    brands_processed INTEGER DEFAULT 0,
                    avg_speed_per_hour REAL,
                    config_json TEXT,
                    status TEXT DEFAULT 'running'
                )
            """
            )

            conn.commit()

    def get_existing_urls(self) -> Set[str]:
        """Получение уже спарсенных URL из БД."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT url FROM comprehensive_reviews")
            return {row[0] for row in cursor.fetchall()}

    def get_brand_models(self, brand: str) -> List[str]:
        """Получение ВСЕХ моделей для бренда."""
        try:
            print(f"     🔍 Получение ВСЕХ моделей для {brand}...")

            # Используем новый метод парсера для получения моделей
            models = self.parser.get_all_models_for_brand(brand)

            if models:
                print(
                    f"     ✅ Найдено {len(models)} моделей с сайта: "
                    f"{models[:5]}..."
                )
                return models
            else:
                print(
                    f"     ⚠️ Модели для {brand} не найдены, "
                    f"используем резервный список"
                )
                return self._get_fallback_models(brand)

        except Exception as e:
            print(f"     ❌ Ошибка получения моделей для {brand}: {e}")
            print(f"     🔄 Используем резервный список моделей для {brand}")
            return self._get_fallback_models(brand)

    def _get_fallback_models(self, brand: str) -> List[str]:
        """Базовый список моделей как резерв на случай ошибок парсера."""
        # Расширенный список популярных моделей по брендам
        models_map = {
            "toyota": [
                "camry",
                "corolla",
                "rav4",
                "land-cruiser",
                "prius",
                "highlander",
                "4runner",
                "tacoma",
                "tundra",
                "sienna",
                "avalon",
                "yaris",
                "c-hr",
                "venza",
                "sequoia",
            ],
            "honda": [
                "civic",
                "accord",
                "cr-v",
                "pilot",
                "fit",
                "hr-v",
                "passport",
                "ridgeline",
                "odyssey",
                "insight",
                "clarity",
            ],
            "bmw": [
                "3-series",
                "5-series",
                "x3",
                "x5",
                "x1",
                "7-series",
                "x7",
                "4-series",
                "6-series",
                "8-series",
                "z4",
                "i3",
                "i8",
            ],
            "audi": [
                "a4",
                "a6",
                "q5",
                "q7",
                "a3",
                "a8",
                "q3",
                "q8",
                "a5",
                "a7",
                "tt",
                "r8",
                "e-tron",
            ],
            "mercedes-benz": [
                "c-class",
                "e-class",
                "glc",
                "gle",
                "a-class",
                "s-class",
                "gls",
                "glb",
                "gla",
                "cls",
                "g-class",
                "slc",
                "amg-gt",
            ],
            "volkswagen": [
                "golf",
                "passat",
                "tiguan",
                "polo",
                "jetta",
                "atlas",
                "arteon",
                "beetle",
                "touareg",
                "id4",
                "gti",
            ],
            "nissan": [
                "qashqai",
                "x-trail",
                "juke",
                "almera",
                "teana",
                "murano",
                "pathfinder",
                "armada",
                "sentra",
                "altima",
                "maxima",
                "350z",
                "370z",
            ],
            "hyundai": [
                "elantra",
                "tucson",
                "santa-fe",
                "accent",
                "sonata",
                "palisade",
                "kona",
                "veloster",
                "genesis",
                "ioniq",
            ],
            "kia": [
                "rio",
                "cerato",
                "sportage",
                "sorento",
                "optima",
                "soul",
                "stinger",
                "cadenza",
                "niro",
                "seltos",
                "telluride",
            ],
            "mazda": [
                "3",
                "6",
                "cx-5",
                "cx-9",
                "2",
                "cx-3",
                "cx-30",
                "mx-5",
                "cx-7",
                "tribute",
                "b-series",
            ],
            "ford": [
                "focus",
                "fiesta",
                "kuga",
                "mondeo",
                "ecosport",
                "explorer",
                "escape",
                "f-150",
                "mustang",
                "edge",
                "expedition",
                "ranger",
            ],
            "chevrolet": [
                "cruze",
                "captiva",
                "aveo",
                "malibu",
                "tahoe",
                "suburban",
                "silverado",
                "equinox",
                "traverse",
                "camaro",
                "corvette",
            ],
            "skoda": [
                "octavia",
                "rapid",
                "superb",
                "kodiaq",
                "fabia",
                "karoq",
                "scala",
                "kamiq",
                "enyaq",
            ],
            "renault": [
                "logan",
                "duster",
                "megane",
                "kaptur",
                "sandero",
                "clio",
                "kadjar",
                "koleos",
                "scenic",
                "talisman",
            ],
            "peugeot": [
                "308",
                "3008",
                "2008",
                "408",
                "5008",
                "208",
                "508",
                "1008",
                "4008",
                "expert",
            ],
            "opel": [
                "astra",
                "corsa",
                "insignia",
                "mokka",
                "zafira",
                "crossland",
                "grandland",
                "combo",
                "vivaro",
            ],
            "mitsubishi": [
                "outlander",
                "asx",
                "lancer",
                "pajero",
                "l200",
                "eclipse",
                "galant",
                "montero",
                "mirage",
            ],
            "subaru": [
                "forester",
                "outback",
                "impreza",
                "xv",
                "legacy",
                "wrx",
                "brz",
                "ascent",
                "tribeca",
            ],
            "suzuki": [
                "vitara",
                "swift",
                "sx4",
                "jimny",
                "baleno",
                "ignis",
                "grand-vitara",
                "liana",
                "wagon-r",
            ],
            "volvo": [
                "xc60",
                "xc90",
                "v60",
                "s60",
                "v40",
                "s90",
                "v90",
                "xc40",
                "c30",
                "s40",
            ],
            "lexus": ["rx", "es", "nx", "gx", "lx", "is", "ls", "ux", "gs", "lc", "rc"],
        }

        # Возвращаем модели для бренда или сам бренд как модель
        return models_map.get(brand, [brand])

    def get_all_brand_review_urls(self, brand: str) -> List[str]:
        """Получение ВСЕХ URL отзывов для бренда."""
        print(f"   🔍 Сбор ВСЕХ URL отзывов для бренда: {brand.upper()}")

        models = self.get_brand_models(brand)
        all_urls = []

        # Парсим ВСЕ модели бренда (без ограничений!)
        models_to_process = models  # Убрали ограничение!

        for i, model in enumerate(models_to_process, 1):
            try:
                print(f"     📄 Модель {i}/{len(models_to_process)}: {model}")

                # Используем реальный метод парсера для получения отзывов модели
                model_reviews = self.parser.parse_reviews(brand, model)
                model_urls = [review.url for review in model_reviews if review.url]

                all_urls.extend(model_urls)
                print(f"       ✅ Найдено: {len(model_urls)} URL")

                # Задержка между моделями
                time.sleep(self.config["delay_between_models"])

            except Exception as e:
                error_msg = f"Ошибка для модели {model}: {str(e)}"
                print(f"       ❌ {error_msg}")
                self.stats["errors"].append(
                    {
                        "type": "model_error",
                        "brand": brand,
                        "model": model,
                        "error": error_msg,
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        print(f"   📊 ИТОГО URL для {brand}: {len(all_urls)}")
        return all_urls

    def parse_single_review(self, url: str) -> Dict[str, Any]:
        """Полный парсинг одного отзыва со ВСЕМИ данными."""
        try:
            # Используем реальный метод парсера
            review_list = self.parser.parse_single_review(url)

            if not review_list:
                return {
                    "status": "error",
                    "error": "Пустой результат парсера",
                    "url": url,
                }

            review = review_list[0]  # Берем первый отзыв

            # Парсим комментарии если включено
            comments = []
            if self.config["enable_comments"]:
                try:
                    comments = self.parser.parse_comments(url)
                except Exception as e:
                    print(f"       ⚠️ Ошибка парсинга комментариев: {e}")

            return {
                "status": "success",
                "review": review,
                "comments": comments,
                "url": url,
                "parsed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            error_msg = str(e)
            print(f"       ❌ Ошибка парсинга {url}: {error_msg}")

            self.stats["errors"].append(
                {
                    "type": "parse_error",
                    "url": url,
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat(),
                }
            )

            return {"status": "error", "error": error_msg, "url": url}

    def save_comprehensive_batch(self, results_batch: List[Dict[str, Any]]):
        """Сохранение результатов со ВСЕМИ полями в БД."""
        if not results_batch:
            return

        if len(results_batch) == 1:
            print("       💾 Сохранение отзыва с полными данными...")
        else:
            print(f"   💾 Сохранение {len(results_batch)} отзывов с полными данными...")

        saved_reviews = 0
        saved_comments = 0

        with sqlite3.connect(self.db_path) as conn:
            for result in results_batch:
                if result["status"] == "success":
                    review = result["review"]

                    try:
                        # Сохраняем основной отзыв со ВСЕМИ полями
                        cursor = conn.execute(
                            """
                            INSERT OR IGNORE INTO comprehensive_reviews 
                            (url, source, brand, model, title, content, author, 
                             author_city, date_published, overall_rating, owner_rating, 
                             views_count, exterior_rating, interior_rating, engine_rating,
                             handling_rating, year, generation, body_type, transmission,
                             drive_type, steering_wheel, mileage, engine_volume, engine_power,
                             fuel_type, fuel_consumption_city, fuel_consumption_highway,
                             fuel_consumption_mixed, purchase_year, color_exterior, 
                             color_interior, comments_count, review_type, parsing_status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                result["url"],
                                review.source,
                                review.brand,
                                review.model,
                                review.title,
                                review.content,
                                review.author,
                                getattr(review, "author_city", ""),
                                review.date_published,
                                review.rating,
                                getattr(review, "owner_rating", None),
                                getattr(review, "views_count", 0),
                                getattr(review, "exterior_rating", None),
                                getattr(review, "interior_rating", None),
                                getattr(review, "engine_rating", None),
                                getattr(review, "handling_rating", None),
                                review.year,
                                review.generation,
                                review.body_type,
                                review.transmission,
                                review.drive_type,
                                review.steering_wheel,
                                review.mileage,
                                review.engine_volume,
                                review.engine_power,
                                review.fuel_type,
                                review.fuel_consumption_city,
                                review.fuel_consumption_highway,
                                getattr(review, "fuel_consumption_mixed", None),
                                getattr(review, "purchase_year", None),
                                getattr(review, "color_exterior", ""),
                                getattr(review, "color_interior", ""),
                                len(result.get("comments", [])),
                                review.type,
                                "success",
                            ),
                        )

                        if cursor.rowcount > 0:
                            saved_reviews += 1
                            review_id = cursor.lastrowid

                            # Сохраняем комментарии
                            for comment in result.get("comments", []):
                                conn.execute(
                                    """
                                    INSERT INTO review_comments 
                                    (review_id, review_url, author, content, date_published)
                                    VALUES (?, ?, ?, ?, ?)
                                """,
                                    (
                                        review_id,
                                        result["url"],
                                        comment.get("author", ""),
                                        comment.get("content", ""),
                                        comment.get("date", ""),
                                    ),
                                )
                                saved_comments += 1

                    except Exception as e:
                        print(f"       ❌ Ошибка сохранения отзыва: {e}")
                        # Сохраняем как неуспешный
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO comprehensive_reviews 
                            (url, brand, model, parsing_status, parsing_errors)
                            VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                result["url"],
                                result.get("brand", "unknown"),
                                result.get("model", "unknown"),
                                "error",
                                str(e),
                            ),
                        )

            conn.commit()

        self.stats["last_save_time"] = time.time()
        self.stats["save_count"] += 1

        print(
            f"   ✅ Сохранено: {saved_reviews} отзывов, {saved_comments} комментариев"
        )

    def start_comprehensive_parsing(self, target_reviews: int = 100):
        """Запуск полнофункционального парсинга по брендам в алфавитном порядке."""
        print("\n" + "=" * 70)
        print("🚀 ЗАПУСК ПОЛНОФУНКЦИОНАЛЬНОГО ПАРСИНГА ПО БРЕНДАМ")
        print("=" * 70)
        print(f"📊 Целевое количество отзывов: {target_reviews}")
        print(f"🏷️ Стратегия: ВСЕ отзывы бренда → следующий бренд (алфавитно)")
        print("💾 Автосохранение: каждый успешный отзыв")
        print("🔍 Проверка дубликатов: включена")
        print(
            f"💬 Парсинг комментариев: {'включен' if self.config['enable_comments'] else 'отключен'}"
        )
        print(
            f"📈 Детальные характеристики: {'включены' if self.config['enable_characteristics'] else 'отключены'}"
        )
        print(f"🌐 База данных: {self.db_path}")
        print("=" * 70)

        # Получаем уже существующие URL
        existing_urls = self.get_existing_urls()
        print(f"📋 Уже в базе данных: {len(existing_urls)} URL")

        # Записываем сессию в БД
        session_id = self._start_parsing_session(target_reviews)

        try:
            for brand_index, brand in enumerate(self.brands, 1):
                if self.stats["total_parsed"] >= target_reviews:
                    break

                self.stats["current_brand"] = brand
                print(f"\n🏷️ БРЕНД {brand_index}/{len(self.brands)}: {brand.upper()}")
                print("-" * 50)

                # Получаем ВСЕ URL отзывов для текущего бренда
                brand_urls = self.get_all_brand_review_urls(brand)

                # Фильтруем уже существующие URL (проверка дубликатов)
                new_urls = [url for url in brand_urls if url not in existing_urls]
                duplicates = len(brand_urls) - len(new_urls)

                self.stats["duplicates_skipped"] += duplicates

                print(f"   📋 Новых URL для парсинга: {len(new_urls)}")
                print(f"   ⚠️ Дубликатов пропущено: {duplicates}")

                brand_parsed = 0
                brand_successful = 0

                # Парсим каждый отзыв бренда
                for url_index, url in enumerate(new_urls, 1):
                    if self.stats["total_parsed"] >= target_reviews:
                        print(
                            f"   🎯 Достигнута целевая отметка: {target_reviews} отзывов"
                        )
                        break

                    # Показываем прогресс
                    if url_index % 5 == 0 or url_index <= 5:
                        progress = f"{self.stats['total_parsed'] + 1}/{target_reviews}"
                        print(f"   📄 Парсинг {progress}: {url}")

                    # Парсим отзыв со всеми данными
                    result = self.parse_single_review(url)

                    if result["status"] == "success":
                        existing_urls.add(url)
                        brand_parsed += 1
                        brand_successful += 1
                        self.stats["total_parsed"] += 1
                        self.stats["successful_parsed"] += 1
                        comments_count = len(result.get("comments", []))
                        self.stats["comments_parsed"] += comments_count

                        review = result["review"]
                        rating_text = f"⭐{review.rating}"
                        year_text = f"({review.year})"
                        print(
                            f"       ✅ {review.brand} {review.model} "
                            f"{year_text} - {rating_text}"
                        )

                        # Сохраняем успешный отзыв сразу
                        self.save_comprehensive_batch([result])
                        print("       💾 Отзыв сохранен в БД")
                    else:
                        self.stats["failed_parsed"] += 1
                        error = result.get("error", "Неизвестна")[:50]
                        print(f"       ❌ Ошибка: {error}")

                    # Задержка между запросами
                    time.sleep(self.config["delay_between_requests"])

                # Статистика по бренду
                print(
                    f"   📊 Бренд {brand} завершен: {brand_successful}/{brand_parsed} успешно"
                )
                self.stats["brands_completed"] += 1

                # Задержка между брендами
                if self.stats["total_parsed"] < target_reviews:
                    print(
                        f"   ⏸️ Пауза между брендами: {self.config['delay_between_brands']}с"
                    )
                    time.sleep(self.config["delay_between_brands"])

            print("\n✅ ПОЛНОФУНКЦИОНАЛЬНЫЙ ПАРСИНГ ЗАВЕРШЕН УСПЕШНО!")

        except KeyboardInterrupt:
            print("\n⏹️ Парсинг прерван пользователем")

        except Exception as e:
            print(f"\n💥 Критическая ошибка: {e}")
            traceback.print_exc()

        finally:
            self._finish_parsing_session(session_id)
            self.print_comprehensive_stats()

    def _start_parsing_session(self, target_reviews: int) -> int:
        """Начало сессии парсинга."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO parsing_sessions 
                (session_start, target_reviews, config_json, status)
                VALUES (?, ?, ?, ?)
            """,
                (
                    self.stats["session_start"],
                    target_reviews,
                    str(self.config),
                    "running",
                ),
            )
            conn.commit()
            return cursor.lastrowid

    def _finish_parsing_session(self, session_id: int):
        """Завершение сессии парсинга."""
        elapsed = time.time() - self.stats["start_time"]
        avg_speed = self.stats["total_parsed"] / elapsed * 3600 if elapsed > 0 else 0

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE parsing_sessions SET
                session_end = ?,
                total_parsed = ?,
                successful_parsed = ?,
                failed_parsed = ?,
                brands_processed = ?,
                avg_speed_per_hour = ?,
                status = ?
                WHERE id = ?
            """,
                (
                    datetime.now().isoformat(),
                    self.stats["total_parsed"],
                    self.stats["successful_parsed"],
                    self.stats["failed_parsed"],
                    self.stats["brands_completed"],
                    avg_speed,
                    "completed",
                    session_id,
                ),
            )
            conn.commit()

    def print_comprehensive_stats(self):
        """Печать подробной финальной статистики."""
        elapsed = time.time() - self.stats["start_time"]

        print("\n" + "=" * 70)
        print("📊 ПОДРОБНАЯ ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 70)

        # Основные метрики
        print(f"⏱️ Время работы: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        print(f"✅ Успешно спарсено: {self.stats['successful_parsed']}")
        print(f"❌ Ошибок парсинга: {self.stats['failed_parsed']}")
        print(f"📝 Всего обработано: {self.stats['total_parsed']}")
        print(f"💬 Комментариев: {self.stats['comments_parsed']}")
        print(f"🏷️ Брендов завершено: {self.stats['brands_completed']}")
        print(f"📄 Дубликатов пропущено: {self.stats['duplicates_skipped']}")
        print(f"💾 Операций сохранения: {self.stats['save_count']}")

        # Производительность
        if elapsed > 0:
            speed = self.stats["total_parsed"] / elapsed * 3600
            print(f"📈 Средняя скорость: {speed:.0f} отзывов/час")

        # Текущий прогресс
        if self.stats["current_brand"]:
            print(f"🏷️ Последний бренд: {self.stats['current_brand']}")
        if self.stats["current_model"]:
            print(f"🚗 Последняя модель: {self.stats['current_model']}")

        # Ошибки
        if self.stats["errors"]:
            print(f"⚠️ Всего ошибок: {len(self.stats['errors'])}")

        # Конфигурация
        print(f"\n🔧 КОНФИГУРАЦИЯ ПАРСИНГА:")
        for key, value in self.config.items():
            print(f"   {key}: {value}")

        # База данных
        print(f"\n💾 База данных: {self.db_path}")
        print(f"📅 Сессия: {self.stats['session_start']}")
        print("=" * 70)


def main():
    """Точка входа для запуска полнофункционального парсера."""
    print("🔥 ИСПРАВЛЕННЫЙ ПОЛНЫЙ ПАРСЕР АВТО-ОТЗЫВОВ")
    print("Парсинг в алфавитном порядке со ВСЕМИ моделями и данными")
    print("\n📋 РЕЖИМЫ РАБОТЫ:")
    print("1. Тестовый режим (30 отзывов)")
    print("2. Боевой режим (без ограничений)")

    choice = input("\nВыберите режим (1/2): ").strip()

    if choice == "2":
        # Боевой режим
        print("\n🚀 БОЕВОЙ РЕЖИМ")
        parser = ComprehensiveBrandParser(mode="production")
        target = int(input("Количество отзывов (0 = без ограничений): ") or "0")
        if target == 0:
            target = 1000000  # Практически без ограничений
        parser.start_comprehensive_parsing(target_reviews=target)
    else:
        # Тестовый режим
        print("\n🧪 ТЕСТОВЫЙ РЕЖИМ")
        parser = ComprehensiveBrandParser(mode="test")
        parser.start_comprehensive_parsing(target_reviews=30)


if __name__ == "__main__":
    main()
