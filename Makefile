#!/usr/bin/make -f
# Makefile для системы парсинга отзывов

# Переменные
PYTHON := python3
PIP := pip3
VENV := parser_env
DB_FILE := auto_reviews.db
HTML_FILE := brands_html.txt

# Цвета для вывода
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

.PHONY: help clean setup install test run-init run-parse run-stats \
        run-full check-lint format git-status git-commit docker-build \
        docker-run env-activate env-deactivate

# Помощь по командам
help:
	@echo "$(GREEN)🚀 СИСТЕМА ПАРСИНГА ОТЗЫВОВ DROM.RU$(NC)"
	@echo "$(BLUE)=======================================$(NC)"
	@echo ""
	@echo "$(YELLOW)📋 Доступные команды:$(NC)"
	@echo ""
	@echo "  $(GREEN)setup$(NC)        - Создание виртуального окружения"
	@echo "  $(GREEN)install$(NC)      - Установка зависимостей"
	@echo "  $(GREEN)test$(NC)         - Запуск тестов"
	@echo "  $(GREEN)check-lint$(NC)   - Проверка кода (pylint, flake8)"
	@echo "  $(GREEN)format$(NC)       - Форматирование кода (black, isort)"
	@echo ""
	@echo "  $(GREEN)run-init$(NC)     - Инициализация системы"
	@echo "  $(GREEN)run-parse$(NC)    - Парсинг отзывов (BRAND=toyota MODEL=camry)"
	@echo "  $(GREEN)run-stats$(NC)    - Показать статистику"
	@echo "  $(GREEN)run-full$(NC)     - Полный цикл"
	@echo ""
	@echo "  $(GREEN)git-status$(NC)   - Показать статус git"
	@echo "  $(GREEN)git-commit$(NC)   - Коммит изменений"
	@echo ""
	@echo "  $(GREEN)docker-build$(NC) - Сборка Docker образа"
	@echo "  $(GREEN)docker-run$(NC)   - Запуск в Docker"
	@echo ""
	@echo "  $(GREEN)clean$(NC)        - Очистка временных файлов"
	@echo ""
	@echo "$(YELLOW)📝 Примеры использования:$(NC)"
	@echo "  make setup install"
	@echo "  make run-init"
	@echo "  make run-parse BRAND=toyota MODEL=camry"
	@echo "  make test"

# Создание виртуального окружения
setup:
	@echo "$(BLUE)🔧 Создание виртуального окружения...$(NC)"
	$(PYTHON) -m venv $(VENV)
	@echo "$(GREEN)✅ Виртуальное окружение создано$(NC)"
	@echo "$(YELLOW)💡 Активируйте командой: source $(VENV)/bin/activate$(NC)"

# Установка зависимостей
install:
	@echo "$(BLUE)📦 Установка зависимостей...$(NC)"
	$(VENV)/bin/pip install -r requirements.txt
	$(VENV)/bin/pip install -e .
	@echo "$(GREEN)✅ Зависимости установлены$(NC)"

# Запуск тестов
test:
	@echo "$(BLUE)🧪 Запуск тестов...$(NC)"
	$(VENV)/bin/python -m pytest tests/ -v --tb=short
	@echo "$(GREEN)✅ Тесты завершены$(NC)"

# Проверка кода
check-lint:
	@echo "$(BLUE)🔍 Проверка кода линтерами...$(NC)"
	-$(VENV)/bin/pylint src/ scripts/ tests/ || true
	-$(VENV)/bin/flake8 src/ scripts/ tests/ || true
	@echo "$(YELLOW)⚠️ Проверка завершена (см. ошибки выше)$(NC)"

# Форматирование кода
format:
	@echo "$(BLUE)✨ Форматирование кода...$(NC)"
	$(VENV)/bin/black src/ scripts/ tests/
	$(VENV)/bin/isort src/ scripts/ tests/
	@echo "$(GREEN)✅ Код отформатирован$(NC)"

# Инициализация системы
run-init:
	@echo "$(BLUE)🚀 Инициализация системы...$(NC)"
	cd scripts && $(PYTHON) main.py --db ../$(DB_FILE) --html ../$(HTML_FILE) init

# Парсинг отзывов (с параметрами)
run-parse:
ifndef BRAND
	$(error BRAND не задан. Используйте: make run-parse BRAND=toyota MODEL=camry)
endif
ifndef MODEL
	$(error MODEL не задан. Используйте: make run-parse BRAND=toyota MODEL=camry)
endif
	@echo "$(BLUE)🚗 Парсинг отзывов $(BRAND)/$(MODEL)...$(NC)"
	cd scripts && $(PYTHON) main.py --db ../$(DB_FILE) parse $(BRAND) $(MODEL)

# Показать статистику
run-stats:
	@echo "$(BLUE)📊 Статистика базы данных...$(NC)"
	cd scripts && $(PYTHON) main.py --db ../$(DB_FILE) stats

# Полный цикл
run-full:
ifndef BRAND
	$(error BRAND не задан. Используйте: make run-full BRAND=toyota MODEL=camry)
endif
ifndef MODEL
	$(error MODEL не задан. Используйте: make run-full BRAND=toyota MODEL=camry)
endif
	@echo "$(BLUE)🎯 Полный цикл для $(BRAND)/$(MODEL)...$(NC)"
	cd scripts && $(PYTHON) main.py --db ../$(DB_FILE) --html ../$(HTML_FILE) \
		--brand $(BRAND) --model $(MODEL) full

# Git статус
git-status:
	@echo "$(BLUE)📋 Git статус:$(NC)"
	git status --porcelain
	@echo ""
	@echo "$(BLUE)📊 Git статистика:$(NC)"
	git log --oneline -5
	@echo ""
	@echo "$(BLUE)🌿 Ветка:$(NC) $(shell git branch --show-current)"

# Git коммит
git-commit:
	@echo "$(BLUE)💾 Подготовка к коммиту...$(NC)"
	git add .
	@echo "$(YELLOW)📝 Введите сообщение коммита:$(NC)"
	@read -p "Commit message: " msg; git commit -m "$$msg"
	@echo "$(GREEN)✅ Изменения закоммичены$(NC)"

# Сборка Docker образа
docker-build:
	@echo "$(BLUE)🐳 Сборка Docker образа...$(NC)"
	docker build -t auto-reviews-parser -f docker/Dockerfile .
	@echo "$(GREEN)✅ Docker образ собран$(NC)"

# Запуск в Docker
docker-run:
	@echo "$(BLUE)🐳 Запуск в Docker...$(NC)"
	docker-compose -f docker/docker-compose.yml up
	@echo "$(GREEN)✅ Docker контейнер запущен$(NC)"

# Очистка временных файлов
clean:
	@echo "$(BLUE)🧹 Очистка временных файлов...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -f parser_debug.log system_run.log
	@echo "$(GREEN)✅ Временные файлы удалены$(NC)"

# Активация окружения (только информация)
env-activate:
	@echo "$(YELLOW)💡 Для активации окружения выполните:$(NC)"
	@echo "   source $(VENV)/bin/activate"

# Деактивация окружения (только информация)
env-deactivate:
	@echo "$(YELLOW)💡 Для деактивации окружения выполните:$(NC)"
	@echo "   deactivate"

# Создание структуры проекта
init-project:
	@echo "$(BLUE)📁 Создание структуры проекта...$(NC)"
	mkdir -p data logs cache output
	touch data/.gitkeep logs/.gitkeep cache/.gitkeep output/.gitkeep
	@echo "$(GREEN)✅ Структура проекта создана$(NC)"
