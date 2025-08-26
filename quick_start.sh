#!/bin/bash

# 🚀 Быстрый запуск системы парсинга Drom.ru
# Использование: ./quick_start.sh [action]

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для красивого вывода
log() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Проверка активации окружения
check_environment() {
    if [[ "$CONDA_DEFAULT_ENV" != "parser_project" ]]; then
        warn "Активирую conda окружение parser_project..."
        eval "$(conda shell.bash hook)"
        conda activate parser_project
    fi
    log "✅ Окружение parser_project активно"
}

# Проверка подключения к базе данных
check_database() {
    log "🔍 Проверяю подключение к PostgreSQL..."
    python -c "
import asyncio
import sys
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def check_db():
    try:
        manager = ExtendedPostgresManager()
        await manager.connect()
        await manager.close()
        print('✅ Подключение к PostgreSQL успешно')
        return True
    except Exception as e:
        print(f'❌ Ошибка подключения к PostgreSQL: {e}')
        return False

result = asyncio.run(check_db())
sys.exit(0 if result else 1)
"
    if [ $? -eq 0 ]; then
        log "✅ База данных доступна"
    else
        error "❌ Проблема с базой данных. Проверьте настройки PostgreSQL"
        exit 1
    fi
}

# Показать статистику
show_stats() {
    log "📊 Загружаю статистику базы данных..."
    python -c "
import asyncio
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def show_stats():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    stats = await manager.get_statistics()
    print('\n📊 Текущая статистика:')
    print('=' * 40)
    for key, value in stats.items():
        print(f'├─ {key}: {value}')
    print('=' * 40)
    
    await manager.close()

asyncio.run(show_stats())
"
}

# Парсинг каталога
parse_catalog() {
    log "📦 Запускаю парсинг каталога брендов и моделей..."
    python -c "
import asyncio
import sys
from scripts.parsing.catalog_integration import CatalogIntegrator

async def parse_catalog():
    try:
        integrator = CatalogIntegrator()
        await integrator.update_catalog()
        print('✅ Каталог успешно обновлен')
        return True
    except Exception as e:
        print(f'❌ Ошибка парсинга каталога: {e}')
        return False

result = asyncio.run(parse_catalog())
sys.exit(0 if result else 1)
"
    if [ $? -eq 0 ]; then
        log "✅ Каталог успешно обновлен"
    else
        error "❌ Ошибка при парсинге каталога"
        exit 1
    fi
}

# Парсинг коротких отзывов для топ брендов
parse_short_reviews() {
    log "🗣️ Запускаю парсинг коротких отзывов для популярных брендов..."
    python -c "
import asyncio
from scripts.parsing.drom_reviews import DromReviewsParser

async def parse_top_brands():
    top_brands = ['toyota', 'mazda', 'honda', 'nissan', 'subaru']
    parser = DromReviewsParser()
    
    for brand in top_brands:
        try:
            print(f'🚗 Парсинг отзывов для {brand}...')
            await parser.parse_short_reviews(brand_name=brand, limit=20)
            print(f'✅ {brand} - готово')
        except Exception as e:
            print(f'❌ Ошибка для {brand}: {e}')
    
    print('🎯 Парсинг коротких отзывов завершен')

asyncio.run(parse_top_brands())
"
}

# Полное обновление
full_update() {
    log "🚀 Запускаю полное обновление системы..."
    
    check_environment
    check_database
    
    log "1️⃣ Обновление каталога..."
    parse_catalog
    
    log "2️⃣ Парсинг коротких отзывов..."
    parse_short_reviews
    
    log "3️⃣ Показываю итоговую статистику..."
    show_stats
    
    log "🎉 Полное обновление завершено успешно!"
}

# Экспорт данных
export_data() {
    log "📤 Запускаю экспорт данных..."
    
    # Создаем директорию для экспорта если её нет
    mkdir -p data/exports
    
    python -c "
import asyncio
import json
import csv
from datetime import datetime
from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager

async def export_all_data():
    manager = ExtendedPostgresManager()
    await manager.connect()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Экспорт брендов
    brands = await manager.get_all_brands()
    brands_data = []
    for brand in brands:
        brands_data.append({
            'id': brand.id,
            'название_бренда': brand.название_бренда,
            'ссылка_на_бренд': brand.ссылка_на_бренд,
            'количество_отзывов': brand.количество_отзывов,
            'дата_создания': str(brand.дата_создания),
            'дата_обновления': str(brand.дата_обновления)
        })
    
    with open(f'data/exports/brands_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(brands_data, f, ensure_ascii=False, indent=2)
    
    # Экспорт моделей
    models = await manager.get_all_models()
    models_data = []
    for model in models:
        models_data.append({
            'id': model.id,
            'бренд_id': model.бренд_id,
            'название_модели': model.название_модели,
            'ссылка_на_модель': model.ссылка_на_модель,
            'количество_отзывов': model.количество_отзывов,
            'дата_создания': str(model.дата_создания),
            'дата_обновления': str(model.дата_обновления)
        })
    
    with open(f'data/exports/models_{timestamp}.json', 'w', encoding='utf-8') as f:
        json.dump(models_data, f, ensure_ascii=False, indent=2)
    
    # Экспорт коротких отзывов в CSV
    reviews = await manager.get_all_short_reviews()
    with open(f'data/exports/short_reviews_{timestamp}.csv', 'w', encoding='utf-8', newline='') as f:
        if reviews:
            writer = csv.DictWriter(f, fieldnames=reviews[0].__dict__.keys())
            writer.writeheader()
            for review in reviews:
                writer.writerow(review.__dict__)
    
    print(f'✅ Данные экспортированы с меткой времени: {timestamp}')
    print(f'📁 Файлы сохранены в: data/exports/')
    
    await manager.close()

asyncio.run(export_all_data())
"
    log "✅ Экспорт данных завершен"
}

# Тестирование системы
run_tests() {
    log "🧪 Запускаю тестирование системы..."
    
    log "1️⃣ Тестирование схемы базы данных..."
    python tests/test_simple_schema.py
    
    log "2️⃣ Тестирование расширенного менеджера..."
    python tests/test_extended_postgres_schema.py
    
    log "3️⃣ Интеграционное тестирование..."
    python tests/test_parser_integration.py
    
    log "✅ Все тесты завершены"
}

# Помощь
show_help() {
    echo ""
    echo "🚀 Система парсинга отзывов Drom.ru"
    echo "=================================="
    echo ""
    echo "Использование: $0 [команда]"
    echo ""
    echo "Доступные команды:"
    echo "  stats           Показать статистику базы данных"
    echo "  catalog         Обновить каталог брендов и моделей"
    echo "  reviews         Парсить короткие отзывы для топ брендов"
    echo "  full            Полное обновление (каталог + отзывы)"
    echo "  export          Экспортировать все данные"
    echo "  test            Запустить тесты системы"
    echo "  check           Проверить состояние системы"
    echo "  help            Показать эту справку"
    echo ""
    echo "Примеры:"
    echo "  $0 full         # Полное обновление данных"
    echo "  $0 stats        # Показать текущую статистику"
    echo "  $0 export       # Экспортировать данные"
    echo ""
}

# Проверка системы
check_system() {
    log "🔍 Проверяю состояние системы..."
    
    check_environment
    check_database
    show_stats
    
    # Проверка доступности Drom.ru
    log "🌐 Проверяю доступность Drom.ru..."
    python -c "
import requests
try:
    response = requests.get('https://www.drom.ru/reviews/', timeout=10)
    if response.status_code == 200:
        print('✅ Сайт Drom.ru доступен')
    else:
        print(f'⚠️ Получен код ответа: {response.status_code}')
except Exception as e:
    print(f'❌ Ошибка подключения к Drom.ru: {e}')
"
    
    log "✅ Проверка системы завершена"
}

# Основная логика
case "${1:-help}" in
    "stats")
        check_environment
        show_stats
        ;;
    "catalog")
        check_environment
        check_database
        parse_catalog
        ;;
    "reviews")
        check_environment
        check_database
        parse_short_reviews
        ;;
    "full")
        full_update
        ;;
    "export")
        check_environment
        check_database
        export_data
        ;;
    "test")
        check_environment
        run_tests
        ;;
    "check")
        check_system
        ;;
    "help"|*)
        show_help
        ;;
esac
