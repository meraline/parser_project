class Config:
    """Конфигурация парсера"""

    # База данных
    DB_PATH = "auto_reviews.db"

    # Задержки (в секундах)
    MIN_DELAY = 5  # Минимальная задержка между запросами
    MAX_DELAY = 15  # Максимальная задержка между запросами
    ERROR_DELAY = 30  # Задержка при ошибке
    RATE_LIMIT_DELAY = 300  # Задержка при rate limit (5 минут)

    # Ограничения
    MAX_RETRIES = 3  # Максимальное количество повторов
    PAGES_PER_SESSION = 50  # Страниц за сессию
    MAX_REVIEWS_PER_MODEL = 1000  # Максимум отзывов на модель

    # User agents для ротации
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, как Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, как Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, как Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # Список популярных брендов и моделей для парсинга
    TARGET_BRANDS = {
        "toyota": ["camry", "corolla", "rav4", "highlander", "prius", "land-cruiser"],
        "volkswagen": ["polo", "golf", "passat", "tiguan", "touareg", "jetta"],
        "nissan": ["qashqai", "x-trail", "almera", "teana", "murano", "pathfinder"],
        "hyundai": ["solaris", "elantra", "tucson", "santa-fe", "creta", "sonata"],
        "kia": ["rio", "cerato", "sportage", "sorento", "soul", "optima"],
        "mazda": ["mazda3", "mazda6", "cx-5", "cx-3", "mx-5", "cx-9"],
        "ford": ["focus", "fiesta", "mondeo", "kuga", "explorer", "ecosport"],
        "chevrolet": ["cruze", "aveo", "captiva", "lacetti", "tahoe", "suburban"],
        "skoda": ["octavia", "rapid", "fabia", "superb", "kodiaq", "karoq"],
        "renault": ["logan", "sandero", "duster", "kaptur", "megane", "fluence"],
        "mitsubishi": ["lancer", "outlander", "asx", "pajero", "eclipse-cross", "l200"],
        "honda": ["civic", "accord", "cr-v", "pilot", "fit", "hr-v"],
        "bmw": ["3-series", "5-series", "x3", "x5", "x1", "1-series"],
        "mercedes-benz": ["c-class", "e-class", "s-class", "glc", "gle", "gla"],
        "audi": ["a3", "a4", "a6", "q3", "q5", "q7"],
        "lada": ["granta", "kalina", "priora", "vesta", "xray", "largus"],
    }
