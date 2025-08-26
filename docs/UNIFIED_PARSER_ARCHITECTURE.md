# 🚗 ЕДИНАЯ АРХИТЕКТУРА ПАРСЕРА DROM.RU

## 🎯 ГЛАВНАЯ ЦЕЛЬ
**БОЛЬШЕ НЕ СОЗДАЕМ НОВЫХ ПАРСЕРОВ!**  
Развиваем только `UnifiedDromParser` - единый мастер-парсер.

## 📁 СТРУКТУРА ПАРСЕРОВ

### ✅ ОСНОВНЫЕ ПАРСЕРЫ (ИСПОЛЬЗУЕМ)
```
src/auto_reviews_parser/parsers/
├── unified_master_parser.py  ← 🌟 ГЛАВНЫЙ ПАРСЕР - РАЗВИВАЕМ ТОЛЬКО ЕГО!
├── base.py                   ← Базовые классы и утилиты
├── drom_reviews.py          ← Старый парсер (оставляем для совместимости)
└── __init__.py              ← Экспорты
```

### ❌ УСТАРЕВШИЕ ПАРСЕРЫ (НЕ ИСПОЛЬЗУЕМ)
```
scripts/parsing/ - ВСЕ ФАЙЛЫ В ЭТОЙ ПАПКЕ УСТАРЕЛИ!
├── brand_based_parser.py     ❌ Заменен unified_master_parser.py
├── catalog_parser.py         ❌ Интегрирован в unified_master_parser.py  
├── normalized_parser.py      ❌ Заменен unified_master_parser.py
├── real_brand_parser.py      ❌ Заменен unified_master_parser.py
└── ... (все остальные)       ❌ НЕ ИСПОЛЬЗУЕМ!
```

## 🏗️ АРХИТЕКТУРА ЕДИНОГО ПАРСЕРА

### 📊 Структуры данных
```python
@dataclass 
class ReviewData:
    """Единая структура для всех типов отзывов"""
    review_id: str
    brand: str
    model: str  
    review_type: str  # 'long' или 'short'
    year: Optional[int]
    engine_volume: Optional[float]
    fuel_type: Optional[str]
    transmission: Optional[str]
    drive_type: Optional[str]
    author: Optional[str]
    city: Optional[str]
    date: Optional[str]
    rating: Optional[int]
    title: Optional[str]
    positive: Optional[str]
    negative: Optional[str]
    general: Optional[str]
    breakages: Optional[str]
    photos: List[str]
    url: Optional[str]

@dataclass
class ModelInfo:
    """Информация о модели"""
    brand: str
    model: str
    model_url: str
    long_reviews_count: int = 0
    short_reviews_count: int = 0
```

### 🔧 Основные методы

#### 🏭 Каталогизация
- `get_brand_catalog()` - Получение списка всех брендов
- `get_brand_models(brand_slug)` - Получение моделей бренда

#### 📝 Парсинг отзывов
- `parse_long_reviews(brand, model, limit)` - Длинные отзывы
- `parse_short_reviews(brand, model, limit)` - Короткие отзывы  
- `parse_model_reviews(brand, model, long_limit, short_limit)` - Все отзывы модели

#### 🎯 Массовый парсинг
- `parse_multiple_models(models_limit, long_limit, short_limit)` - Парсинг нескольких моделей

#### 💾 Сохранение
- `save_results(results, filename)` - Сохранение в JSON

### 🛠️ Встроенные возможности

#### 📂 Кэширование
- Автоматическое кэширование каталогов брендов и моделей
- Файлы кэша: `data/cache/brand_catalog.json`, `data/cache/models_{brand}.json`

#### 📊 Метрики и логирование
- Подробная статистика запросов
- Логирование в `logs/unified_parser.log`
- Метрики успешности парсинга

#### 🔄 Обработка ошибок
- Автоматические повторные попытки
- Обработка 404 ошибок
- Graceful degradation

#### ⏱️ Управление нагрузкой
- Настраиваемые задержки между запросами
- Безопасные заголовки HTTP

## 🚀 ИСПОЛЬЗОВАНИЕ

### Простой запуск
```bash
# Запуск единого парсера
python run_unified_parser.py
```

### Программное использование
```python
from auto_reviews_parser.parsers import UnifiedDromParser

# Создаем парсер
parser = UnifiedDromParser(delay=1.0)

# Парсим 3 модели: 3 длинных + 10 коротких отзывов каждая
results = parser.parse_multiple_models(
    models_limit=3,
    long_limit=3, 
    short_limit=10
)

# Сохраняем результаты
parser.save_results(results)
```

### Кастомный парсинг
```python
# Парсинг конкретной модели
model_results = parser.parse_model_reviews(
    brand="toyota",
    model="camry", 
    long_limit=5,
    short_limit=20
)

# Только короткие отзывы
short_reviews = parser.parse_short_reviews("toyota", "camry", limit=50)

# Только длинные отзывы  
long_reviews = parser.parse_long_reviews("toyota", "camry", limit=10)
```

## 📋 ПРИНЦИПЫ РАЗВИТИЯ

### ✅ ЧТО ДЕЛАЕМ
1. **Развиваем UnifiedDromParser** - добавляем новые функции только в него
2. **Улучшаем извлечение данных** - совершенствуем методы extract_*
3. **Добавляем новые сайты** - наследуем от UnifiedDromParser
4. **Оптимизируем производительность** - улучшаем кэширование и алгоритмы

### ❌ ЧТО НЕ ДЕЛАЕМ  
1. **НЕ создаем новых парсеров** в scripts/parsing/
2. **НЕ дублируем функциональность** - используем существующий код
3. **НЕ копируем код** между файлами - переиспользуем методы
4. **НЕ игнорируем архитектуру** - следуем принципу единого парсера

## 🔧 РАСШИРЕНИЕ ФУНКЦИОНАЛЬНОСТИ

### Добавление нового сайта
```python
from auto_reviews_parser.parsers.unified_master_parser import UnifiedDromParser

class AutoRuParser(UnifiedDromParser):
    """Парсер для auto.ru на основе единой архитектуры"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://auto.ru"
    
    # Переопределяем только специфичные методы
    def make_request(self, url: str) -> Optional[BeautifulSoup]:
        # Специфичная логика для auto.ru
        pass
```

### Добавление нового типа данных
```python
# В ReviewData добавляем новые поля
@dataclass 
class ReviewData:
    # ... существующие поля ...
    price: Optional[int] = None          # Цена автомобиля
    mileage: Optional[int] = None        # Пробег
    ownership_period: Optional[str] = None  # Период владения
```

### Улучшение извлечения данных
```python
def extract_price(self, text: str) -> Optional[int]:
    """Новый метод извлечения цены"""
    patterns = [
        r'(\d+(?:\s*\d{3})*)\s*(?:руб|₽)',
        r'(\d+)\s*тысяч', 
    ]
    # ... логика извлечения ...
```

## 📁 ФАЙЛОВАЯ СТРУКТУРА

```
parser_project/
├── src/auto_reviews_parser/parsers/
│   ├── unified_master_parser.py  ← 🌟 ГЛАВНЫЙ ФАЙЛ
│   ├── base.py                   ← Базовые классы
│   ├── drom_reviews.py          ← Совместимость
│   └── __init__.py              ← Экспорты
├── run_unified_parser.py         ← Скрипт запуска
├── data/
│   ├── cache/                    ← Кэш каталогов
│   └── *.json                    ← Результаты парсинга
└── logs/
    └── unified_parser.log        ← Логи парсера
```

## 🎯 ROADMAP

### Версия 1.1 (Текущая)
- ✅ Единый парсер для длинных и коротких отзывов
- ✅ Кэширование каталогов
- ✅ Метрики и логирование
- ✅ Обработка ошибок

### Версия 1.2 (Планируется)
- 🔄 Интеграция с базой данных
- 🔄 Параллельный парсинг
- 🔄 Веб-интерфейс для управления

### Версия 1.3 (Будущее)
- 📊 Аналитика отзывов
- 🤖 ML-обработка текстов
- 🔗 API для внешних приложений

---

## 🎉 ЗАКЛЮЧЕНИЕ

**Единый парсер - это наше будущее!**

- 🎯 Один файл вместо десятков
- 🔧 Легкое сопровождение
- 🚀 Быстрое развитие
- 📊 Централизованная статистика

**Запомните: UnifiedDromParser - единственный парсер, который мы развиваем!**
