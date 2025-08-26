# 🚀 РУКОВОДСТВО ПО ЗАПУСКУ ПАРСИНГА

## 📋 СПОСОБЫ ЗАПУСКА ПАРСИНГА

### 1. 🎯 БЫСТРЫЙ ЗАПУСК (рекомендуется)

```bash
# Активировать окружение
conda activate parser_project

# Обновить каталог брендов и моделей (первый запуск)
./quick_start.sh catalog

# Запустить парсинг коротких отзывов (с лимитом)
./quick_start.sh reviews

# Полная статистика
./quick_start.sh stats
```

### 2. 🎮 ИНТЕРАКТИВНОЕ МЕНЮ

```bash
python simple_menu.py
# Выбрать пункт 6 - "Запуск парсинга"
```

### 3. 🧪 ТЕСТОВЫЙ ПАРСИНГ КОНКРЕТНОЙ МОДЕЛИ

```bash
# Парсинг первых 10 отзывов Toyota Camry
python scripts/parsing/parse_10_reviews.py

# Парсинг конкретного бренда с лимитом
python scripts/parsing/parse_single_model.py
```

### 4. ⚙️ РУЧНАЯ НАСТРОЙКА ПАРСЕРА

```bash
# Запуск основного парсера с настройками
python src/auto_reviews_parser/services/auto_reviews_parser.py
```

## 🎛️ НАСТРОЙКА ЛИМИТОВ

### В файле конфигурации `Config`:

```python
class Config:
    # Ограничения парсинга
    MAX_REVIEWS_PER_MODEL = 1000      # Максимум отзывов на модель
    PAGES_PER_SESSION = 50            # Страниц за одну сессию
    MAX_RETRIES = 3                   # Количество повторов при ошибках
    
    # Задержки (безопасность)
    MIN_DELAY = 5                     # Минимальная пауза (сек)
    MAX_DELAY = 15                    # Максимальная пауза (сек)
    ERROR_DELAY = 30                  # Пауза при ошибке (сек)
    RATE_LIMIT_DELAY = 300            # Пауза при блокировке (5 мин)
```

### Быстрая настройка лимитов в quick_start.sh:

```bash
# Открыть файл для редактирования
nano quick_start.sh

# Найти строку:
await parser.parse_short_reviews(brand_name=brand, limit=20)

# Изменить limit на нужное значение:
await parser.parse_short_reviews(brand_name=brand, limit=100)  # 100 отзывов на бренд
```

## 💾 СОХРАНЕНИЕ ДАННЫХ

### ✅ АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ

**Каждый успешно спарсенный отзыв сохраняется НЕМЕДЛЕННО:**

1. **При парсинге отзыва** → сразу в PostgreSQL
2. **Проверка дублей** → по URL и content_hash
3. **Обработка ошибок** → логируется, парсинг продолжается
4. **Статистика сессий** → сохраняется в `сессии_парсинга`

### 📊 Структура сохранения:

```sql
-- Короткие отзывы сохраняются в:
INSERT INTO auto_reviews.короткие_отзывы (
    модель_id, автор, город, год_авто, объем_двигателя,
    тип_топлива, тип_коробки, тип_привода, 
    плюсы, минусы, поломки, фото_urls, дата_создания
) VALUES (...)

-- Длинные отзывы в:
INSERT INTO auto_reviews.отзывы (...)

-- Сессии парсинга в:
INSERT INTO auto_reviews.сессии_парсинга (
    тип_парсинга, дата_начала, дата_окончания,
    статус, найдено_элементов, обработано_элементов, ошибок
) VALUES (...)
```

## 🎯 ПРАКТИЧЕСКИЕ ПРИМЕРЫ

### 1. Парсинг топ-5 брендов с лимитом 50 отзывов:

```bash
# Редактировать quick_start.sh
nano quick_start.sh

# Изменить функцию parse_short_reviews():
parse_short_reviews() {
    log "🗣️ Запускаю парсинг коротких отзывов для популярных брендов..."
    python -c "
import asyncio
from scripts.parsing.comprehensive_brand_parser import parse_top_brands

async def main():
    # ТОП-5 брендов с лимитом 50 отзывов каждый
    await parse_top_brands(['toyota', 'honda', 'mazda', 'nissan', 'hyundai'], limit=50)

asyncio.run(main())
"
}

# Запустить
./quick_start.sh reviews
```

### 2. Целевой парсинг конкретной модели:

```python
# Создать скрипт target_parsing.py
import asyncio
from scripts.parsing.normalized_parser import EnhancedDromParser

async def parse_specific_model():
    parser = EnhancedDromParser()
    
    # Парсинг 200 отзывов Toyota Camry
    await parser.parse_model_reviews("toyota", "camry", limit=200)
    
    # Парсинг 100 отзывов Honda Civic  
    await parser.parse_model_reviews("honda", "civic", limit=100)

# Запуск
asyncio.run(parse_specific_model())
```

### 3. Массовый парсинг с контролем:

```python
# Скрипт mass_parsing.py
import asyncio
from time import sleep

async def controlled_mass_parsing():
    brands = ['toyota', 'honda', 'mazda', 'nissan', 'hyundai', 'kia', 'subaru']
    
    for brand in brands:
        print(f"🚗 Парсинг бренда: {brand}")
        
        # Парсим с лимитом 30 отзывов на бренд
        await parser.parse_brand_reviews(brand, limit=30)
        
        # Пауза между брендами (2 минуты)
        print("⏳ Пауза 2 минуты...")
        sleep(120)
    
    print("✅ Массовый парсинг завершен!")
```

## 🛡️ БЕЗОПАСНЫЙ РЕЖИМ ПАРСИНГА

### Встроенные защиты:

1. **Случайные паузы** между запросами (5-15 сек)
2. **Ротация User-Agent** для имитации разных браузеров  
3. **Обработка блокировок** с автоматическими паузами (5 мин)
4. **Ограничение страниц** за сессию
5. **Проверка дублей** перед сохранением

### Мониторинг парсинга:

```bash
# Статистика в реальном времени
python show_stats.py

# Детальный просмотр данных
python show_detailed_data.py

# Интерактивное меню для контроля
python simple_menu.py
```

## ⚡ БЫСТРЫЕ КОМАНДЫ

```bash
# 🎯 Стартовый набор для новичка (10 отзывов)
python scripts/parsing/parse_10_reviews.py

# 🚀 Стандартный запуск (100 отзывов)
./quick_start.sh reviews

# 🔥 Агрессивный парсинг (без лимитов, осторожно!)
python src/auto_reviews_parser/services/auto_reviews_parser.py

# 📊 Проверка результатов
python show_stats.py
```

## 🎛️ НАСТРОЙКА ЛИМИТОВ НА ЛЕТУ

### В интерактивном меню:

1. Запустить: `python simple_menu.py`
2. Выбрать: `6 - Запуск парсинга`
3. Выбрать тип парсинга
4. Система запросит параметры лимитов

### В quick_start.sh с параметрами:

```bash
# Парсинг с кастомным лимитом
./quick_start.sh reviews 50  # 50 отзывов на бренд

# Парсинг конкретного бренда
./quick_start.sh toyota 100  # 100 отзывов Toyota
```

## ⚠️ ВАЖНЫЕ МОМЕНТЫ

1. **Первый запуск**: Обязательно `./quick_start.sh catalog` для загрузки каталога
2. **Сохранение**: Каждый отзыв сохраняется сразу после парсинга
3. **Дубли**: Автоматически отфильтровываются по URL
4. **Ошибки**: Не прерывают парсинг, логируются отдельно
5. **Перезапуск**: Парсинг продолжается с того места, где остановился
6. **Мониторинг**: Используйте `python show_stats.py` для отслеживания прогресса

## 🎯 РЕКОМЕНДУЕМЫЙ ВОРКФЛОУ

```bash
# 1. Обновить каталог (раз в неделю)
./quick_start.sh catalog

# 2. Запустить парсинг отзывов
./quick_start.sh reviews

# 3. Проверить статистику
python show_stats.py

# 4. Экспортировать данные при необходимости
python simple_menu.py → пункт 5
```
