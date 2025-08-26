# ПЛАН РЕОРГАНИЗАЦИИ ПАРСЕРОВ

## Текущая проблема
Слишком много парсеров в разных папках:
- `src/auto_reviews_parser/parsers/` - 5+ активных парсеров
- `src/auto_reviews_parser/parsers/archive/` - 10+ старых парсеров  
- `scripts/parsing/` - 15+ скриптов парсинга

## Предложенная структура

### 1. Основные парсеры (src/auto_reviews_parser/parsers/)
```
├── drom_master_parser.py          # Единый мастер-парсер для Drom.ru
├── drive2_parser.py               # Парсер для Drive2 
├── base.py                        # Базовые классы
└── utils/                         # Вспомогательные модули
    ├── selectors.py               # CSS селекторы и data-ftid
    ├── html_extractors.py         # Методы извлечения данных
    └── validators.py              # Валидация данных
```

### 2. Объединенная логика для Drom
- Взять рабочую логику из `drom_reviews.py` (правильные селекторы)
- Исправить логику в `production_drom_parser.py` (правильные методы подсчета)
- Создать единый `drom_master_parser.py`

### 3. Архив (archive/)
- Переместить все старые/неиспользуемые парсеры
- Оставить только для истории

### 4. Скрипты (scripts/parsing/)
- Оставить только основные точки входа:
  - `run_full_parsing.py` - полный парсинг
  - `run_limited_parsing.py` - ограниченный парсинг  
  - `run_brand_parsing.py` - парсинг конкретного бренда

## Выявленные проблемы

### В production_drom_parser.py:
1. ✅ Правильное определение количества отзывов (исправлено)
2. ❌ Неполная реализация `_parse_long_review_block`
3. ❌ Неполная реализация `_parse_short_review_block`

### В drom_reviews.py:
1. ✅ Правильные селекторы: `data-ftid="review-item"`, `data-ftid="short-review-item"`
2. ✅ Полная реализация извлечения данных
3. ✅ Правильные data-ftid для полей отзывов

## Действия
1. Скопировать рабочую логику из `drom_reviews.py` в `production_drom_parser.py`
2. Создать единый мастер-парсер
3. Переместить неиспользуемые файлы в архив
4. Упростить структуру скриптов
