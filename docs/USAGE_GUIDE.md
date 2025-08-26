# 📖 Руководство пользователя: Система парсинга отзывов Drom.ru

## 🎯 Что умеет система

Наша система позволяет:
- ✅ **Парсить каталог брендов и моделей** с сайта Drom.ru
- ✅ **Собирать короткие отзывы** пользователей
- ✅ **Собирать длинные отзывы** с полной информацией
- ✅ **Хранить данные в PostgreSQL** с нормализованной структурой
- ✅ **Отслеживать сессии парсинга** и статистику
- ✅ **Экспортировать данные** в различные форматы

---

## 🚀 Быстрый старт

### 1. Активация окружения
```bash
# Активируем conda окружение
conda activate parser_project

# Переходим в директорию проекта
cd /home/analityk/Документы/projects/parser_project
```

### 2. Проверка состояния базы данных
```bash
# Проверяем подключение к PostgreSQL
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def check_db():
    manager = ExtendedPostgresManager()
    try:
        await manager.connect()
        stats = await manager.get_statistics()
        print('🎯 Статистика базы данных:')
        for key, value in stats.items():
            print(f'  📊 {key}: {value}')
        await manager.close()
    except Exception as e:
        print(f'❌ Ошибка подключения: {e}')

asyncio.run(check_db())
"
```

---

## 📋 Основные сценарии использования

### 🏭 Сценарий 1: Парсинг каталога брендов и моделей

```bash
# Запуск парсинга каталога (бренды + модели)
python scripts/parsing/catalog_parser.py

# Или через интеграционный скрипт
python -c "
import asyncio
from scripts.parsing.catalog_integration import CatalogIntegrator

async def parse_catalog():
    integrator = CatalogIntegrator()
    await integrator.update_catalog()
    
asyncio.run(parse_catalog())
"
```

**Что происходит:**
- 🔍 Загружается главная страница Drom.ru/reviews
- 📦 Извлекаются все бренды автомобилей
- 🚗 Для каждого бренда загружаются модели
- 💾 Данные сохраняются в PostgreSQL с нормализацией

### 🗣️ Сценарий 2: Парсинг коротких отзывов

```bash
# Парсинг коротких отзывов для конкретной модели
python scripts/parsing/drom_reviews.py --brand toyota --model camry --type short

# Парсинг коротких отзывов для всех моделей бренда
python scripts/parsing/drom_reviews.py --brand toyota --type short

# Парсинг с ограничением количества
python scripts/parsing/drom_reviews.py --brand mazda --model familia --type short --limit 100
```

### 📝 Сценарий 3: Парсинг длинных отзывов

```bash
# Парсинг длинных отзывов
python scripts/parsing/drom_reviews.py --brand toyota --model camry --type long

# Парсинг с детальным логированием
python scripts/parsing/drom_reviews.py --brand honda --model civic --type long --verbose
```

### 🔄 Сценарий 4: Полный цикл парсинга

```bash
# Скрипт для полного обновления данных
python -c "
import asyncio
from scripts.parsing.catalog_integration import CatalogIntegrator
from scripts.parsing.drom_reviews import DromReviewsParser

async def full_update():
    print('🚀 Начинаем полное обновление...')
    
    # 1. Обновляем каталог
    print('📦 Обновление каталога брендов и моделей...')
    integrator = CatalogIntegrator()
    await integrator.update_catalog()
    
    # 2. Парсим короткие отзывы для топ-брендов
    print('🗣️ Парсинг коротких отзывов...')
    top_brands = ['toyota', 'mazda', 'honda', 'nissan', 'subaru']
    parser = DromReviewsParser()
    
    for brand in top_brands:
        try:
            await parser.parse_short_reviews(brand_name=brand, limit=50)
            print(f'✅ Обработан бренд: {brand}')
        except Exception as e:
            print(f'❌ Ошибка для {brand}: {e}')
    
    print('🎯 Полное обновление завершено!')

asyncio.run(full_update())
"
```

---

## 🛠️ Работа с базой данных

### 📊 Просмотр статистики

```bash
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def show_stats():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    stats = await manager.get_statistics()
    print('📊 Общая статистика:')
    for key, value in stats.items():
        print(f'├─ {key}: {value}')
    
    # Топ брендов по количеству отзывов
    top_brands = await manager.get_top_brands_by_reviews(limit=5)
    print('\n🏆 Топ брендов по отзывам:')
    for i, brand in enumerate(top_brands, 1):
        print(f'{i}. {brand[\"название_бренда\"]} - {brand[\"количество_отзывов\"]} отзывов')
    
    await manager.close()

asyncio.run(show_stats())
"
```

### 🔍 Поиск данных

```bash
# Поиск брендов
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def search_brands():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    brands = await manager.get_all_brands()
    print('🚗 Доступные бренды:')
    for brand in brands:
        print(f'├─ {brand.название_бренда} (ID: {brand.id}, отзывов: {brand.количество_отзывов})')
    
    await manager.close()

asyncio.run(search_brands())
"
```

```bash
# Поиск моделей для бренда
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def search_models(brand_name='toyota'):
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    models = await manager.get_models_by_brand(brand_name)
    print(f'🚙 Модели бренда {brand_name}:')
    for model in models:
        print(f'├─ {model.название_модели} (отзывов: {model.количество_отзывов})')
    
    await manager.close()

# Замените 'toyota' на нужный бренд
asyncio.run(search_models('toyota'))
"
```

### 📤 Экспорт данных

```bash
# Экспорт в JSON
python -c "
import asyncio
import json
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def export_to_json():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    # Экспорт брендов
    brands = await manager.get_all_brands()
    brands_data = [brand.__dict__ for brand in brands]
    
    with open('data/exports/brands_export.json', 'w', encoding='utf-8') as f:
        json.dump(brands_data, f, ensure_ascii=False, indent=2, default=str)
    
    # Экспорт коротких отзывов
    reviews = await manager.get_all_short_reviews()
    reviews_data = [review.__dict__ for review in reviews]
    
    with open('data/exports/short_reviews_export.json', 'w', encoding='utf-8') as f:
        json.dump(reviews_data, f, ensure_ascii=False, indent=2, default=str)
    
    print('✅ Данные экспортированы в data/exports/')
    await manager.close()

asyncio.run(export_to_json())
"
```

---

## 🧪 Тестирование и отладка

### ✅ Запуск тестов

```bash
# Тестирование схемы базы данных
python tests/test_simple_schema.py

# Тестирование расширенного менеджера
python tests/test_extended_postgres_schema.py

# Полное интеграционное тестирование
python tests/test_parser_integration.py

# Все тесты сразу
python -m pytest tests/ -v
```

### 🐛 Отладка

```bash
# Включение детального логирования
export PYTHONPATH=/home/analityk/Документы/projects/parser_project
export DEBUG_MODE=1

# Запуск с отладкой
python scripts/parsing/drom_reviews.py --brand toyota --model camry --type short --debug
```

### 📋 Проверка состояния парсеров

```bash
# Проверка доступности сайта
python -c "
import requests
try:
    response = requests.get('https://www.drom.ru/reviews/', timeout=10)
    if response.status_code == 200:
        print('✅ Сайт Drom.ru доступен')
    else:
        print(f'⚠️ Получен код: {response.status_code}')
except Exception as e:
    print(f'❌ Ошибка подключения: {e}')
"
```

---

## ⚙️ Конфигурация

### 🗄️ Настройка базы данных

Файл: `src/auto_reviews_parser/database/postgres_config.py`

```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'auto_reviews',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 🌐 Настройка парсинга

Файл: `src/auto_reviews_parser/config.py`

```python
PARSING_CONFIG = {
    'delay_between_requests': 1,  # секунды
    'max_retries': 3,
    'timeout': 30,
    'user_agent': 'Mozilla/5.0...'
}
```

---

## 📁 Структура данных

### 🏭 Таблица брендов

```sql
бренды (
    id SERIAL PRIMARY KEY,
    название_бренда VARCHAR(100) UNIQUE,
    ссылка_на_бренд TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### 🚗 Таблица моделей

```sql
модели (
    id SERIAL PRIMARY KEY,
    бренд_id INTEGER REFERENCES бренды(id),
    название_модели VARCHAR(100),
    ссылка_на_модель TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### 🗣️ Таблица коротких отзывов

```sql
короткие_отзывы (
    id SERIAL PRIMARY KEY,
    модель_id INTEGER REFERENCES модели(id),
    автор_id VARCHAR(50),
    город VARCHAR(100),
    год_автомобиля INTEGER,
    объем_двигателя DECIMAL(3,1),
    тип_топлива VARCHAR(20),
    тип_коробки VARCHAR(20),
    тип_привода VARCHAR(20),
    плюсы TEXT,
    минусы TEXT,
    неисправности TEXT,
    дата_отзыва TIMESTAMP,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

---

## 🔧 Полезные команды

### 📊 Мониторинг

```bash
# Количество записей в каждой таблице
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def table_counts():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    tables = ['бренды', 'модели', 'короткие_отзывы', 'длинные_отзывы', 'сессии_парсинга']
    
    for table in tables:
        try:
            result = await manager.connection.fetchval(f'SELECT COUNT(*) FROM {table}')
            print(f'📊 {table}: {result} записей')
        except Exception as e:
            print(f'❌ {table}: ошибка - {e}')
    
    await manager.close()

asyncio.run(table_counts())
"
```

### 🧹 Очистка данных

```bash
# Очистка старых сессий парсинга (старше 30 дней)
python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def cleanup_old_sessions():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    result = await manager.connection.execute('''
        DELETE FROM сессии_парсинга 
        WHERE дата_создания < NOW() - INTERVAL '30 days'
    ''')
    
    print(f'🧹 Удалено старых сессий: {result}')
    await manager.close()

asyncio.run(cleanup_old_sessions())
"
```

---

## 🚨 Устранение неполадок

### ❌ Проблемы с подключением к БД

```bash
# Проверка подключения к PostgreSQL
psql -h localhost -U your_username -d auto_reviews -c "SELECT version();"

# Проверка существования таблиц
psql -h localhost -U your_username -d auto_reviews -c "\dt"
```

### ❌ Проблемы с парсингом

```bash
# Проверка доступности Chrome
ls -la chrome-linux/chrome

# Проверка прав доступа
chmod +x chrome-linux/chrome

# Тест простого запроса
python -c "
import requests
response = requests.get('https://www.drom.ru/reviews/')
print(f'Статус: {response.status_code}')
print(f'Размер ответа: {len(response.text)} символов')
"
```

### ❌ Проблемы с окружением

```bash
# Переустановка окружения
conda env remove -n parser_project
conda env create -f environment.yml

# Или установка зависимостей вручную
conda create -n parser_project python=3.12
conda activate parser_project
pip install -r requirements.txt
```

---

## 🎯 Следующие шаги

1. **📈 Масштабирование**: Добавьте параллельный парсинг для ускорения
2. **🔄 Автоматизация**: Настройте cron-задачи для регулярного обновления
3. **📱 API**: Создайте REST API для доступа к данным
4. **📊 Аналитика**: Добавьте дашборды для визуализации данных
5. **🛡️ Мониторинг**: Настройте alerting при ошибках парсинга

---

## 📞 Поддержка

При возникновении проблем:

1. Проверьте логи в `tests/logs/`
2. Запустите интеграционные тесты: `python tests/test_parser_integration.py`
3. Проверьте статус базы данных
4. Убедитесь, что виртуальное окружение активно

**Система готова к использованию! 🚀**
