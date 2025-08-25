# 🚀 Система парсинга отзывов с Drom.ru

Комплексная система для автоматического сбора и анализа отзывов об автомобилях с сайта drom.ru.

## ✨ Возможности

- 🗂️ **Каталогизация брендов и моделей** - автоматическое создание структуры брендов
- 📄 **Парсинг длинных отзывов** - подробные обзоры автомобилей
- 📋 **Парсинг коротких отзывов** - краткие мнения владельцев
- 🗄️ **Нормализованная БД** - структурированное хранение данных
- 🎯 **Командная строка** - удобное управление через CLI
- 🐳 **Docker поддержка** - контейнеризованный запуск
- 🧪 **Тестирование** - полное покрытие тестами

## 📋 Структура проекта

```
parser_project/
├── src/auto_reviews_parser/     # Основной пакет
│   ├── database/               # Работа с БД
│   ├── catalog/               # Инициализация каталога
│   ├── parsers/               # Парсеры сайтов
│   └── ...
├── scripts/                   # Скрипты запуска
├── tests/                     # Тесты
├── docker/                    # Docker конфигурация
└── Makefile                   # Автоматизация задач
```

## 🚀 Быстрый старт

### 1. Установка

```bash
# Клонирование репозитория
git clone <repository_url>
cd parser_project

# Создание виртуального окружения
make setup

# Активация окружения
source parser_env/bin/activate

# Установка зависимостей
make install
```

### 2. Использование

```bash
# Просмотр всех команд
make help

# Инициализация системы
make run-init

# Парсинг отзывов для конкретной модели
make run-parse BRAND=toyota MODEL=camry

# Показать статистику
make run-stats

# Полный цикл (инициализация + парсинг)
make run-full BRAND=toyota MODEL=camry
```

### 3. Тестирование

```bash
# Запуск всех тестов
make test

# Проверка кода
make check-lint

# Форматирование кода
make format
```

## 🎮 Примеры использования

### Через Makefile (рекомендуется)

```bash
# Базовые команды
make setup install           # Настройка окружения
make run-init                # Инициализация каталога
make run-parse BRAND=toyota MODEL=camry  # Парсинг отзывов
make run-stats               # Статистика

# Работа с git
make git-status              # Статус репозитория
make git-commit              # Коммит изменений

# Docker
make docker-build            # Сборка образа
make docker-run              # Запуск контейнера
```

### Через CLI скрипт

```bash
cd scripts

# Инициализация
python main.py init --db ../auto_reviews.db --html ../brands_html.txt

# Парсинг отзывов
python main.py parse toyota camry --db ../auto_reviews.db --max-long 3 --max-short 5

# Статистика
python main.py stats --db ../auto_reviews.db

# Полный цикл
python main.py full --db ../auto_reviews.db --html ../brands_html.txt \\
               --brand toyota --model camry --max-long 3 --max-short 5
```

## 🗄️ База данных

Система использует SQLite базу данных с нормализованной структурой:

- **brands** - справочник брендов автомобилей
- **models** - модели автомобилей (привязаны к брендам)
- **reviews** - отзывы (длинные и короткие)

### Схема БД

```sql
-- Бренды
CREATE TABLE brands (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url_name TEXT UNIQUE NOT NULL
);

-- Модели
CREATE TABLE models (
    id INTEGER PRIMARY KEY,
    brand_id INTEGER REFERENCES brands(id),
    name TEXT NOT NULL,
    url_name TEXT NOT NULL
);

-- Отзывы
CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    model_id INTEGER REFERENCES models(id),
    review_type TEXT CHECK (review_type IN ('long', 'short')),
    author TEXT,
    title TEXT,
    content TEXT,
    rating INTEGER,
    pros TEXT,
    cons TEXT,
    -- автомобильные характеристики
    year INTEGER,
    engine_volume REAL,
    fuel_type TEXT,
    transmission TEXT,
    drive_type TEXT,
    mileage INTEGER,
    ownership_period TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🧪 Тестирование

```bash
# Все тесты
make test

# Конкретный тест
pytest tests/integration/test_complete_system.py -v

# Тесты с покрытием
pytest --cov=src tests/
```

## 🛠️ Разработка

### Структура кода

- `src/auto_reviews_parser/database/` - работа с базой данных
- `src/auto_reviews_parser/catalog/` - инициализация каталога брендов
- `src/auto_reviews_parser/parsers/` - парсеры сайтов
- `tests/` - тесты системы

### Добавление нового парсера

1. Создать класс в `src/auto_reviews_parser/parsers/`
2. Наследовать от базового парсера
3. Реализовать методы парсинга
4. Добавить тесты

### Проверка кода

```bash
make check-lint    # Проверка линтерами
make format        # Автоформатирование
```

## 🐳 Docker

### Сборка и запуск

```bash
# Сборка образа
make docker-build

# Запуск
make docker-run

# Или напрямую
docker-compose -f docker/docker-compose.yml up
```

### Переменные окружения

- `DB_PATH` - путь к базе данных
- `HTML_FILE` - файл с каталогом брендов
- `LOG_LEVEL` - уровень логирования

## 📊 Мониторинг

- Логи сохраняются в папку `logs/`
- Статистика доступна через `make run-stats`
- Prometheus метрики (в Docker режиме)

## 🤝 Участие в разработке

1. Форк репозитория
2. Создание ветки для фичи
3. Внесение изменений
4. Покрытие тестами
5. Pull Request

## 📝 Лицензия

MIT License

## 🔗 Связанные проекты

- [Drom.ru API](https://drom.ru) - источник данных
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) - парсинг HTML
- [SQLite](https://sqlite.org/) - база данных

---

**Автор**: AI Assistant  
**Дата**: 2024  
**Версия**: 1.0.0
