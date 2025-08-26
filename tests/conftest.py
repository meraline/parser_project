"""
Конфигурация pytest и общие фикстуры
"""
import os
import sys
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock

# Добавляем src в путь
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Константы для тестов
TEST_DATA_DIR = Path(__file__).parent / "test_data"
MOCK_HTML_DIR = TEST_DATA_DIR / "html"


@pytest.fixture(scope="session")
def test_data_dir():
    """Директория с тестовыми данными"""
    return TEST_DATA_DIR


@pytest.fixture(scope="session")
def mock_html_dir():
    """Директория с mock HTML файлами"""
    return MOCK_HTML_DIR


@pytest.fixture
def temp_dir():
    """Временная директория для тестов"""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_review_data():
    """Образец данных отзыва для тестов"""
    return {
        "source": "drom",
        "type": "long",
        "brand": "Toyota",
        "model": "Camry", 
        "generation": None,
        "year": 2020,
        "engine_volume": 2.5,
        "fuel_type": "бензин",
        "transmission": "автомат",
        "drive_type": "передний",
        "body_type": "седан",
        "equipment": None,
        "url": "https://www.drom.ru/reviews/toyota/camry/123456/",
        "title": "Отзыв о Toyota Camry",
        "content": "Отличная машина, надежная. Дорогое обслуживание. Нет серьезных поломок",
        "author": "TestUser", 
        "date": "2024-01-15",
        "rating": 5,
        "id": "123456",
        "city": "Москва",
        "useful_count": 10,
        "not_useful_count": 2
    }


@pytest.fixture
def sample_brand_data():
    """Примерные данные бренда"""
    return {
        "id": "toyota",
        "name": "Toyota",
        "logo_url": "https://example.com/toyota.png",
        "reviews_count": 50000
    }


@pytest.fixture
def sample_model_data():
    """Примерные данные модели"""
    return {
        "id": "camry",
        "name": "Camry", 
        "brand_id": "toyota",
        "reviews_count": 5000,
        "short_reviews_count": 15000
    }


@pytest.fixture
def mock_response():
    """Mock HTTP ответ"""
    mock = Mock()
    mock.status_code = 200
    mock.text = "<html><body>Test content</body></html>"
    mock.json.return_value = {"status": "ok"}
    return mock


@pytest.fixture
def mock_database():
    """Mock база данных"""
    mock_db = MagicMock()
    mock_db.connect.return_value = True
    mock_db.execute.return_value = []
    mock_db.fetch_all.return_value = []
    mock_db.fetch_one.return_value = None
    return mock_db


@pytest.fixture
def mock_parser():
    """Mock парсер"""
    mock = MagicMock()
    mock.parse_reviews.return_value = []
    mock.parse_brand.return_value = {}
    mock.parse_model.return_value = {}
    return mock


@pytest.fixture(scope="session")
def test_database_config():
    """Конфигурация тестовой базы данных"""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_auto_reviews",
        "user": "postgres",
        "password": "postgres",
        "schema": "test_schema"
    }


@pytest.fixture
def env_vars():
    """Переменные окружения для тестов"""
    old_env = os.environ.copy()
    
    test_env = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432", 
        "POSTGRES_DB": "test_auto_reviews",
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres",
        "CHROME_PATH": "/usr/bin/chrome-test",
        "LOG_LEVEL": "DEBUG"
    }
    
    os.environ.update(test_env)
    yield test_env
    
    # Восстанавливаем окружение
    os.environ.clear()
    os.environ.update(old_env)


# Настройка логирования для тестов
@pytest.fixture(autouse=True)
def setup_logging():
    """Настройка логирования для тестов"""
    import logging
    logging.getLogger().setLevel(logging.WARNING)
    # Отключаем verbose логи в тестах
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)


# Создание тестовых директорий при необходимости
def pytest_configure(config):
    """Конфигурация pytest"""
    # Создаем директории для отчетов
    reports_dir = Path("tests/reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Создаем директорию для тестовых данных
    TEST_DATA_DIR.mkdir(exist_ok=True)
    MOCK_HTML_DIR.mkdir(exist_ok=True)


def pytest_collection_modifyitems(config, items):
    """Модификация собранных тестов"""
    for item in items:
        # Автоматически добавляем маркеры на основе расположения файлов
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
            item.add_marker(pytest.mark.slow)


# Функции для пропуска тестов при отсутствии зависимостей
def pytest_runtest_setup(item):
    """Настройка перед запуском каждого теста"""
    # Пропускаем DB тесты если нет подключения к БД
    if "db" in item.keywords:
        try:
            import psycopg2
            psycopg2.connect(
                host="localhost",
                database="postgres", 
                user="postgres",
                password="postgres"
            ).close()
        except:
            pytest.skip("PostgreSQL недоступен")
    
    # Пропускаем network тесты если нет интернета
    if "network" in item.keywords:
        import urllib.request
        try:
            urllib.request.urlopen('http://google.com', timeout=1)
        except:
            pytest.skip("Нет доступа к интернету")
