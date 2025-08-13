# Примеры использования парсера отзывов

## Установка и настройка

### 1. Подготовка среды
```bash
# Создание виртуальной среды
python -m venv parser_env
source parser_env/bin/activate  # Linux/Mac

# Установка зависимостей
pip install botasaurus pandas matplotlib seaborn
```

### 2. Структура проекта
```bash
mkdir auto_reviews_project
cd auto_reviews_project

# Скачать файлы:
# - auto_reviews_parser.py
# - data_analyzer.py
```

## Сценарии использования

### 🚀 Быстрый старт (для тестирования)
```bash
# 1. Инициализация
python auto_reviews_parser.py init

# 2. Тестовая сессия (2-3 источника)
python auto_reviews_parser.py parse --sources 3

# 3. Проверка результатов
python auto_reviews_parser.py status
python data_analyzer.py stats
```

### 📊 Месячный сбор данных (рекомендуется)
```bash
# Щадящий режим: 2 сессии в день по 5 источников
nohup python auto_reviews_parser.py continuous --sessions 2 --sources 5 > parser.log 2>&1 &

# Мониторинг процесса
tail -f parser.log
python auto_reviews_parser.py status
```

### ⚡ Интенсивный сбор (для быстрого наполнения)
```bash
# 4 сессии в день по 10 источников
python auto_reviews_parser.py continuous --sessions 4 --sources 10
```

### 🔧 Настройка для конкретных брендов

Отредактируйте `Config.TARGET_BRANDS` в файле `auto_reviews_parser.py`:

```python
# Пример: только премиум бренды
TARGET_BRANDS = {
    'bmw': ['3-series', '5-series', 'x3', 'x5', 'x1'],
    'mercedes-benz': ['c-class', 'e-class', 's-class', 'glc', 'gle'],
    'audi': ['a3', 'a4', 'a6', 'q3', 'q5', 'q7'],
    'lexus': ['rx', 'es', 'is', 'nx', 'gx']
}

# Или массовый рынок
TARGET_BRANDS = {
    'toyota': ['camry', 'corolla', 'rav4', 'highlander'],
    'volkswagen': ['polo', 'golf', 'passat', 'tiguan'],
    'hyundai': ['solaris', 'elantra', 'tucson', 'santa-fe'],
    'kia': ['rio', 'cerato', 'sportage', 'sorento']
}
```

## Анализ собранных данных

### 📈 Базовая статистика
```bash
python data_analyzer.py stats
```

### 📋 Подробный отчет (HTML)
```bash
python data_analyzer.py report
# Создается файл analysis_report_YYYYMMDD_HHMMSS.html
```

### 🔍 Анализ конкретного бренда
```bash
python data_analyzer.py brand --brand toyota
python data_analyzer.py brand --brand bmw
```

### 💾 Экспорт данных
```bash
# JSON для API/веб-приложений
python data_analyzer.py export --output analytics_data.json

# Excel для отчетов
python auto_reviews_parser.py export --format xlsx
```

### 🎯 Рекомендации по покупке
```bash
python data_analyzer.py recommendations
```

## Автоматизация и мониторинг

### Systemd Service (Linux)

Создайте файл `/etc/systemd/system/auto-reviews-parser.service`:

```ini
[Unit]
Description=Auto Reviews Parser
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=parser
Group=parser
WorkingDirectory=/home/parser/auto_reviews_project
Environment=PATH=/home/parser/auto_reviews_project/parser_env/bin
ExecStart=/home/parser/auto_reviews_project/parser_env/bin/python auto_reviews_parser.py continuous --sessions 2 --sources 5
Restart=always
RestartSec=30
StandardOutput=append:/var/log/auto-parser.log
StandardError=append:/var/log/auto-parser-error.log

[Install]
WantedBy=multi-user.target
```

Активация:
```bash
sudo systemctl enable auto-reviews-parser
sudo systemctl start auto-reviews-parser
sudo systemctl status auto-reviews-parser
```

### Cron для периодических отчетов

```bash
# Открыть crontab
crontab -e

# Добавить задачи:
# Ежедневный отчет в 9:00
0 9 * * * cd /path/to/project && python data_analyzer.py report

# Еженедельный экспорт данных (воскресенье в 18:00)
0 18 * * 0 cd /path/to/project && python auto_reviews_parser.py export --format xlsx

# Проверка места на диске каждый час
0 * * * * df -h | awk '$5 > 85 {print "Disk usage critical: " $5 " on " $1}' | mail -s "Disk Alert" admin@example.com
```

### Мониторинг и алерты

```bash
#!/bin/bash
# Файл: check_parser_health.sh

LOG_FILE="/var/log/auto-parser.log"
RECENT_ENTRIES=$(tail -100 "$LOG_FILE" | grep "$(date '+%Y-%m-%d')" | wc -l)

if [ $RECENT_ENTRIES -lt 5 ]; then
    echo "WARNING: Parser seems inactive (only $RECENT_ENTRIES entries today)"
    exit 1
fi

# Проверка ошибок
ERROR_COUNT=$(tail -1000 "$LOG_FILE" | grep -i error | wc -l)
if [ $ERROR_COUNT -gt 10 ]; then
    echo "WARNING: High error count ($ERROR_COUNT errors in last 1000 entries)"
    exit 1
fi

echo "Parser is healthy: $RECENT_ENTRIES entries today, $ERROR_COUNT recent errors"
```

## Практические примеры

### Анализ конкретной модели
```python
# Пример: поиск всех отзывов Toyota Camry
import sqlite3
import pandas as pd

conn = sqlite3.connect('auto_reviews.db')

camry_reviews = pd.read_sql_query("""
    SELECT title, rating, content, author, year, mileage, publish_date, url
    FROM reviews 
    WHERE brand = 'toyota' AND model = 'camry'
    ORDER BY publish_date DESC
""", conn)

print(f"Найдено отзывов Toyota Camry: {len(camry_reviews)}")
print(f"Средний рейтинг: {camry_reviews['rating'].mean():.2f}")

# Отзывы по годам выпуска
year_stats = camry_reviews.groupby('year').agg({
    'rating': 'mean',
    'title': 'count'
}).round(2)

print("\nСтатистика по годам:")
print(year_stats)
```

### Сравнение брендов
```python
import matplotlib.pyplot as plt

# Средние рейтинги топ-10 брендов
brand_ratings = pd.read_sql_query("""
    SELECT brand, AVG(rating) as avg_rating, COUNT(*) as review_count
    FROM reviews 
    WHERE rating IS NOT NULL 
    GROUP BY brand 
    HAVING review_count >= 20
    ORDER BY avg_rating DESC 
    LIMIT 10
""", conn)

plt.figure(figsize=(12, 6))
plt.bar(brand_ratings['brand'], brand_ratings['avg_rating'])
plt.title('Средние рейтинги брендов (по отзывам владельцев)')
plt.ylabel('Рейтинг')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('brand_ratings.png', dpi=300)
plt.show()
```

### Поиск проблемных моделей
```python
# Модели с низкими рейтингами и частыми упоминаниями проблем
problematic_models = pd.read_sql_query("""
    SELECT brand, model, AVG(rating) as avg_rating, COUNT(*) as review_count,
           AVG(LENGTH(cons)) as avg_cons_length
    FROM reviews 
    WHERE rating IS NOT NULL AND rating < 3.0
    GROUP BY brand, model 
    HAVING review_count >= 5
    ORDER BY avg_rating ASC, avg_cons_length DESC
""", conn)

print("Модели с наибольшим количеством негативных отзывов:")
for _, row in problematic_models.head(10).iterrows():
    print(f"{row['brand']} {row['model']}: {row['avg_rating']:.1f}★ ({int(row['review_count'])} отзывов)")
```

## Оптимизация производительности

### Настройки для слабых серверов
```python
# В auto_reviews_parser.py измените Config:
class Config:
    MIN_DELAY = 10      # Увеличенные задержки
    MAX_DELAY = 25
    PAGES_PER_SESSION = 20  # Меньше страниц за раз
    MAX_REVIEWS_PER_MODEL = 500  # Меньший лимит
```

### Настройки для мощных серверов
```python
class Config:
    MIN_DELAY = 3       # Более агрессивные настройки
    MAX_DELAY = 8
    PAGES_PER_SESSION = 100
    MAX_REVIEWS_PER_MODEL = 2000
```

### Очистка и оптимизация БД
```bash
#!/bin/bash
# Файл: optimize_db.sh

echo "Оптимизация базы данных..."

# Удаление дублирующихся записей
sqlite3 auto_reviews.db "
DELETE FROM reviews 
WHERE id NOT IN (
    SELECT MIN(id) 
    FROM reviews 
    GROUP BY content_hash
);"

# Сжатие базы данных
sqlite3 auto_reviews.db "VACUUM;"

# Обновление статистики
sqlite3 auto_reviews.db "ANALYZE;"

echo "Оптимизация завершена"
```

## Интеграция с другими системами

### REST API для доступа к данным
```python
# Простой Flask API
from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

@app.route('/api/stats')
def get_stats():
    conn = sqlite3.connect('auto_reviews.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM reviews")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT brand, COUNT(*) FROM reviews GROUP BY brand")
    by_brand = dict(cursor.fetchall())
    
    conn.close()
    
    return jsonify({
        'total_reviews': total,
        'by_brand': by_brand
    })

@app.route('/api/brand/<brand>')
def get_brand_data(brand):
    conn = sqlite3.connect('auto_reviews.db')
    
    reviews = pd.read_sql_query(
        "SELECT * FROM reviews WHERE brand = ? LIMIT 100", 
        conn, params=[brand]
    )
    
    conn.close()
    
    return jsonify(reviews.to_dict('records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

### Webhook уведомления
```python
import requests

def send_webhook_notification(message):
    """Отправка уведомления в Slack/Discord/Telegram"""
    webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    
    payload = {
        "text": f"🚗 Auto Parser: {message}",
        "username": "Auto Reviews Bot"
    }
    
    try:
        requests.post(webhook_url, json=payload)
    except Exception as e:
        print(f"Failed to send webhook: {e}")

# Использование в парсере
def run_parsing_session(self, max_sources: int = 10):
    # ... код парсинга ...
    
    # Уведомление о завершении сессии
    send_webhook_notification(
        f"Сессия завершена: обработано {sources_processed} источников, "
        f"сохранено {total_reviews_saved} отзывов"
    )
```

## Типичные проблемы и решения

### 1. Парсер останавливается
```bash
# Проверить логи
tail -100 logs/parser_$(date +%Y%m%d).log

# Перезапустить с восстановлением
python auto_reviews_parser.py parse --sources 5
```

### 2. Слишком много ошибок 403/429
```python
# Увеличить задержки в Config
MIN_DELAY = 15
MAX_DELAY = 30
ERROR_DELAY = 60
```

### 3. База данных заблокирована
```bash
# Проверить процессы
lsof auto_reviews.db

# Принудительно разблокировать
sqlite3 auto_reviews.db ".timeout 30000; SELECT 1;"
```

### 4. Нехватка места на диске
```bash
# Очистка логов
find logs/ -name "*.log" -mtime +7 -delete

# Очистка кеша botasaurus
rm -rf cache/

# Сжатие базы данных
sqlite3 auto_reviews.db "VACUUM;"
```

Этот парсер создан для долгосрочной и стабильной работы. При правильной настройке он будет собирать тысячи отзывов ежедневно без вашего вмешательства.