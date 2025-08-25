# 🚀 СИСТЕМА КАТАЛОЖНОГО ПАРСИНГА DROM.RU

## 📋 Обзор

Новая система состоит из двух основных компонентов:

1. **🔍 Brand Catalog Extractor** - извлекает полный каталог брендов и моделей
2. **🚗 Catalog Parser** - парсит отзывы на основе каталога

## 🏗️ Структура системы

```
📁 scripts/parsing/
├── 🔍 brand_catalog_extractor.py  # Экстрактор каталога брендов
├── 🚗 catalog_parser.py           # Парсер на основе каталога  
├── 📋 normalized_parser.py        # Базовый нормализованный парсер
└── 📊 CATALOG_PARSER_README.md    # Эта инструкция

📁 data/
└── 📄 brand_catalog.json          # Извлеченный каталог брендов

📁 logs/
├── 📝 brand_extractor.log         # Логи экстрактора
└── 📝 catalog_parser.log          # Логи каталожного парсера
```

## 🔧 Установка и настройка

### 1. Активация виртуальной среды
```bash
source parser_env/bin/activate
```

### 2. Создание каталога брендов
```bash
# Извлечение каталога брендов (выполнить один раз)
python scripts/parsing/brand_catalog_extractor.py --extract

# Извлечение моделей для всех брендов (опционально, для полноты)
python scripts/parsing/brand_catalog_extractor.py --extract --models --max-brands 10
```

### 3. Проверка каталога
```bash
# Статистика каталога
python scripts/parsing/brand_catalog_extractor.py --stats
```

## 🚀 Использование каталожного парсера

### 🎯 Парсинг конкретного бренда
```bash
# Парсить Toyota (3 модели, 5 отзывов на модель)
python scripts/parsing/catalog_parser.py --brand toyota --max-models 3 --max-reviews 5

# Парсить Mazda (все модели, 10 отзывов на модель)  
python scripts/parsing/catalog_parser.py --brand mazda --max-reviews 10

# Парсить BMW (5 моделей, 20 отзывов на модель)
python scripts/parsing/catalog_parser.py --brand bmw --max-models 5 --max-reviews 20
```

### 🌟 Парсинг всех брендов (боевой режим)
```bash
# Парсить топ-5 брендов (2 модели, 10 отзывов на модель)
python scripts/parsing/catalog_parser.py --all-brands --max-brands 5 --max-models 2 --max-reviews 10

# Полный боевой режим (топ-20 брендов, 10 моделей, 50 отзывов)
python scripts/parsing/catalog_parser.py --all-brands --max-brands 20 --max-models 10 --max-reviews 50

# Тестовый режим (топ-3 бренда, 1 модель, 3 отзыва)
python scripts/parsing/catalog_parser.py --all-brands --max-brands 3 --max-models 1 --max-reviews 3
```

### 📊 Статистика базы данных
```bash
# Показать статистику нормализованной БД
python scripts/parsing/catalog_parser.py --stats
```

## 🔄 Обновление каталога

### Проверка новых брендов
```bash
# Проверить изменения в каталоге брендов
python scripts/parsing/brand_catalog_extractor.py --update
```

### Добавление моделей для новых брендов
```bash
# Извлечь модели для конкретных брендов
python scripts/parsing/brand_catalog_extractor.py --extract --models --max-brands 5
```

## 📊 Структура каталога

### Пример brand_catalog.json:
```json
{
  "extraction_date": "2025-01-25T10:30:00",
  "total_brands": 150,
  "brands": {
    "toyota": {
      "name": "Toyota",
      "slug": "toyota",
      "url": "https://www.drom.ru/reviews/toyota/",
      "review_count": 282196,
      "models": ["4runner", "allion", "avensis", "camry", "corolla"]
    },
    "bmw": {
      "name": "BMW", 
      "slug": "bmw",
      "url": "https://www.drom.ru/reviews/bmw/",
      "review_count": 18001,
      "models": ["x5", "x3", "3-series", "5-series", "7-series"]
    }
  }
}
```

## 🎯 Логика работы

### 1. Последовательность парсинга:
- Бренды сортируются по количеству отзывов (от большего к меньшему)
- Модели обрабатываются в алфавитном порядке
- Отзывы парсятся последовательно (не рандомно)

### 2. Проверка дубликатов:
- ✅ Пропускает существующие полные отзывы
- 🔄 Перезаписывает неполные отзывы (< 100 символов)
- 📊 Детальная статистика обработки

### 3. Обработка ошибок:
- Логирование всех ошибок
- Продолжение работы при ошибках
- Итоговая статистика по ошибкам

## 📈 Мониторинг и логирование

### Файлы логов:
- `logs/brand_extractor.log` - извлечение каталога
- `logs/catalog_parser.log` - парсинг отзывов

### Примеры логов:
```
2025-01-25 10:30:15 - INFO - 🚗 Начинаем парсинг бренда Toyota (toyota)
2025-01-25 10:30:15 - INFO - 📋 Моделей к обработке: 20
2025-01-25 10:30:16 - INFO - [1/20] Обрабатываем модель: 4runner
2025-01-25 10:30:18 - INFO - Найдено 15 отзывов для toyota/4runner
2025-01-25 10:30:20 - INFO - ✅ Парсинг Toyota завершен: 5 новых отзывов
```

## 🎮 Примеры команд для разных сценариев

### 🧪 Тестирование:
```bash
# Быстрый тест одного бренда
python scripts/parsing/catalog_parser.py --brand toyota --max-models 1 --max-reviews 2

# Тест нескольких брендов
python scripts/parsing/catalog_parser.py --all-brands --max-brands 2 --max-models 1 --max-reviews 3
```

### 🔥 Боевое использование:
```bash
# Средняя нагрузка (рекомендуется)
python scripts/parsing/catalog_parser.py --all-brands --max-brands 10 --max-models 5 --max-reviews 20

# Высокая нагрузка (мощный сервер)
python scripts/parsing/catalog_parser.py --all-brands --max-brands 50 --max-models 10 --max-reviews 100

# Полный парсинг (очень долго!)
python scripts/parsing/catalog_parser.py --all-brands --max-reviews 1000
```

### 🎯 Целевой парсинг:
```bash
# Популярные бренды
python scripts/parsing/catalog_parser.py --brand toyota --max-reviews 100
python scripts/parsing/catalog_parser.py --brand bmw --max-reviews 50
python scripts/parsing/catalog_parser.py --brand mercedes-benz --max-reviews 50

# Китайские бренды
python scripts/parsing/catalog_parser.py --brand chery --max-reviews 30
python scripts/parsing/catalog_parser.py --brand geely --max-reviews 30
python scripts/parsing/catalog_parser.py --brand byd --max-reviews 30
```

## ⚡ Производительность

### Ориентировочные времена:
- **1 отзыв**: ~2-3 секунды
- **1 модель** (10 отзывов): ~30 секунд  
- **1 бренд** (5 моделей): ~3-5 минут
- **10 брендов**: ~30-60 минут

### Оптимизация:
- Используйте разумные лимиты (max-models, max-reviews)
- Мониторьте логи для выявления проблем
- Запускайте парсинг частями в случае больших объемов

## 🔧 Обслуживание

### Еженедельное обновление каталога:
```bash
# Проверка новых брендов
python scripts/parsing/brand_catalog_extractor.py --update

# Обновление статистики
python scripts/parsing/brand_catalog_extractor.py --stats
```

### Очистка логов:
```bash
# Архивирование старых логов
mv logs/catalog_parser.log logs/catalog_parser_$(date +%Y%m%d).log
mv logs/brand_extractor.log logs/brand_extractor_$(date +%Y%m%d).log
```

## 🎉 Заключение

Система каталожного парсинга обеспечивает:

✅ **Последовательную обработку** (не случайную)  
✅ **Проверку дубликатов** и перезапись неполных данных  
✅ **Масштабируемость** - от тестирования до production  
✅ **Мониторинг** и детальное логирование  
✅ **Гибкость** настроек под разные задачи  

**Система готова для production-использования! 🚀**
