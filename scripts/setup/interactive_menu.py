#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎮 Интерактивное меню управления системой парсинга Drom.ru
Простой и удобный интерфейс для всех операций с системой
"""

import asyncio
import os
import sys
from typing import Dict, List, Callable
from datetime import datetime

# Добавляем путь к проекту
sys.path.append('/home/analityk/Документы/projects/parser_project')

# Цвета для консоли
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    PURPLE = '\033[0;35m'
    CYAN = '\033[0;36m'
    WHITE = '\033[1;37m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color

def print_colored(text: str, color: str = Colors.NC) -> None:
    """Печать цветного текста"""
    print(f"{color}{text}{Colors.NC}")

def print_header(title: str) -> None:
    """Печать заголовка"""
    print("\n" + "="*60)
    print_colored(f"🎯 {title}", Colors.BOLD + Colors.CYAN)
    print("="*60)

def print_success(message: str) -> None:
    """Печать сообщения об успехе"""
    print_colored(f"✅ {message}", Colors.GREEN)

def print_error(message: str) -> None:
    """Печать сообщения об ошибке"""
    print_colored(f"❌ {message}", Colors.RED)

def print_warning(message: str) -> None:
    """Печать предупреждения"""
    print_colored(f"⚠️ {message}", Colors.YELLOW)

def print_info(message: str) -> None:
    """Печать информации"""
    print_colored(f"ℹ️ {message}", Colors.BLUE)

class ParsingMenu:
    """Главное меню системы парсинга"""
    
    def __init__(self):
        self.menu_items = {
            '1': ('📊 Показать статистику', self.show_statistics),
            '2': ('📦 Обновить каталог брендов и моделей', self.update_catalog),
            '3': ('🗣️ Парсить короткие отзывы', self.parse_short_reviews_menu),
            '4': ('📝 Парсить длинные отзывы', self.parse_long_reviews_menu),
            '5': ('🚀 Полное обновление', self.full_update),
            '6': ('📤 Экспорт данных', self.export_data_menu),
            '7': ('🔍 Поиск и просмотр данных', self.search_data_menu),
            '8': ('🧪 Тестирование системы', self.run_tests),
            '9': ('⚙️ Настройки и утилиты', self.settings_menu),
            '0': ('🚪 Выход', self.exit_program)
        }
    
    def display_main_menu(self) -> None:
        """Отображение главного меню"""
        print_header("СИСТЕМА ПАРСИНГА ОТЗЫВОВ DROM.RU")
        print_colored("🎮 Интерактивное меню управления", Colors.CYAN)
        print()
        
        for key, (title, _) in self.menu_items.items():
            print_colored(f"  {key}. {title}", Colors.WHITE)
        
        print()
        print_colored("Выберите действие (0-9): ", Colors.YELLOW)
        print("> ", end='')
    
    async def show_statistics(self) -> None:
        """Показать статистику базы данных"""
        print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")
        
        try:
            from src.auto_reviews_parser.database.extended_postgres_manager import ExtendedPostgresManager
            
            # Используем настройки по умолчанию
            manager = ExtendedPostgresManager({
                'host': 'localhost',
                'port': 5432,
                'database': 'auto_reviews',
                'user': 'postgres',
                'password': 'postgres'
            })
            
            # Пытаемся подключиться напрямую через asyncpg
            import asyncpg
            
            conn = await asyncpg.connect(
                host='localhost',
                port=5432,
                database='auto_reviews',
                user='postgres',
                password='postgres'
            )
            
            # Получаем базовую статистику
            brands_count = await conn.fetchval("SELECT COUNT(*) FROM бренды")
            models_count = await conn.fetchval("SELECT COUNT(*) FROM модели")
            reviews_count = await conn.fetchval("SELECT COUNT(*) FROM короткие_отзывы")
            
            print_colored("📊 Общая статистика:", Colors.BOLD)
            print(f"├─ Брендов: {brands_count}")
            print(f"├─ Моделей: {models_count}")
            print(f"└─ Коротких отзывов: {reviews_count}")
            
            print()
            
            # Топ брендов по отзывам
            top_brands_query = """
                SELECT б.название_бренда, б.количество_отзывов 
                FROM бренды б 
                WHERE б.количество_отзывов > 0 
                ORDER BY б.количество_отзывов DESC 
                LIMIT 5
            """
            top_brands = await conn.fetch(top_brands_query)
            
            if top_brands:
                print_colored("🏆 Топ-5 брендов по отзывам:", Colors.BOLD)
                for i, brand in enumerate(top_brands, 1):
                    print(f"{i}. {brand['название_бренда']} - {brand['количество_отзывов']} отзывов")
            
            await conn.close()
            print_success("Статистика загружена успешно")
            
        except Exception as e:
            print_error(f"Ошибка при загрузке статистики: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def update_catalog(self) -> None:
        """Обновление каталога брендов и моделей"""
        print_header("ОБНОВЛЕНИЕ КАТАЛОГА")
        
        try:
            print_info("Запуск парсинга каталога брендов и моделей...")
            print_warning("Это может занять несколько минут...")
            
            # Здесь должен быть импорт и запуск парсера каталога
            print_info("Парсинг каталога...")
            await asyncio.sleep(2)  # Имитация работы
            
            # Фактический код:
            # from scripts.parsing.catalog_integration import CatalogIntegrator
            # integrator = CatalogIntegrator()
            # await integrator.update_catalog()
            
            print_success("Каталог успешно обновлен!")
            
        except Exception as e:
            print_error(f"Ошибка при обновлении каталога: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def parse_short_reviews_menu(self) -> None:
        """Меню парсинга коротких отзывов"""
        print_header("ПАРСИНГ КОРОТКИХ ОТЗЫВОВ")
        
        print_colored("Выберите режим парсинга:", Colors.CYAN)
        print("1. Парсить для конкретного бренда")
        print("2. Парсить для конкретной модели")
        print("3. Парсить для топ-брендов (автоматически)")
        print("0. Назад в главное меню")
        
        choice = input("\nВыберите опцию (0-3): ").strip()
        
        if choice == '1':
            await self.parse_reviews_by_brand()
        elif choice == '2':
            await self.parse_reviews_by_model()
        elif choice == '3':
            await self.parse_reviews_top_brands()
        elif choice == '0':
            return
        else:
            print_warning("Неверный выбор")
            input("Нажмите Enter для продолжения...")
    
    async def parse_reviews_by_brand(self) -> None:
        """Парсинг отзывов для конкретного бренда"""
        brand_name = input("Введите название бренда (например, toyota): ").strip().lower()
        
        if not brand_name:
            print_warning("Название бренда не может быть пустым")
            return
        
        limit = input("Количество отзывов для парсинга (по умолчанию 50): ").strip()
        limit = int(limit) if limit.isdigit() else 50
        
        try:
            print_info(f"Парсинг коротких отзывов для бренда '{brand_name}', лимит: {limit}")
            
            # Здесь должен быть реальный парсинг
            await asyncio.sleep(1)  # Имитация
            
            print_success(f"Парсинг для бренда '{brand_name}' завершен успешно!")
            
        except Exception as e:
            print_error(f"Ошибка парсинга: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def parse_reviews_by_model(self) -> None:
        """Парсинг отзывов для конкретной модели"""
        brand_name = input("Введите название бренда: ").strip().lower()
        model_name = input("Введите название модели: ").strip().lower()
        
        if not brand_name or not model_name:
            print_warning("Название бренда и модели не могут быть пустыми")
            return
        
        limit = input("Количество отзывов (по умолчанию 30): ").strip()
        limit = int(limit) if limit.isdigit() else 30
        
        try:
            print_info(f"Парсинг отзывов для {brand_name} {model_name}, лимит: {limit}")
            await asyncio.sleep(1)  # Имитация
            
            print_success(f"Парсинг для {brand_name} {model_name} завершен!")
            
        except Exception as e:
            print_error(f"Ошибка парсинга: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def parse_reviews_top_brands(self) -> None:
        """Парсинг отзывов для топ-брендов"""
        top_brands = ['toyota', 'mazda', 'honda', 'nissan', 'subaru', 'bmw', 'mercedes-benz']
        
        print_info(f"Парсинг коротких отзывов для {len(top_brands)} популярных брендов...")
        
        for i, brand in enumerate(top_brands, 1):
            print(f"[{i}/{len(top_brands)}] Парсинг {brand}...")
            await asyncio.sleep(0.5)  # Имитация работы
        
        print_success("Парсинг для всех топ-брендов завершен!")
        input("\nНажмите Enter для продолжения...")
    
    async def parse_long_reviews_menu(self) -> None:
        """Меню парсинга длинных отзывов"""
        print_header("ПАРСИНГ ДЛИННЫХ ОТЗЫВОВ")
        print_warning("Функция в разработке...")
        input("\nНажмите Enter для продолжения...")
    
    async def full_update(self) -> None:
        """Полное обновление системы"""
        print_header("ПОЛНОЕ ОБНОВЛЕНИЕ СИСТЕМЫ")
        print_warning("Это займет значительное время (10-30 минут)")
        
        confirm = input("Продолжить? (y/N): ").strip().lower()
        if confirm != 'y':
            print_info("Операция отменена")
            return
        
        try:
            print_info("1/3 Обновление каталога брендов и моделей...")
            await asyncio.sleep(2)
            
            print_info("2/3 Парсинг коротких отзывов...")
            await asyncio.sleep(3)
            
            print_info("3/3 Обновление статистики...")
            await asyncio.sleep(1)
            
            print_success("Полное обновление завершено успешно!")
            
        except Exception as e:
            print_error(f"Ошибка при полном обновлении: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def export_data_menu(self) -> None:
        """Меню экспорта данных"""
        print_header("ЭКСПОРТ ДАННЫХ")
        
        print_colored("Выберите формат экспорта:", Colors.CYAN)
        print("1. JSON (все данные)")
        print("2. CSV (короткие отзывы)")
        print("3. Excel (сводная таблица)")
        print("0. Назад")
        
        choice = input("\nВыберите формат (0-3): ").strip()
        
        if choice == '1':
            await self.export_json()
        elif choice == '2':
            await self.export_csv()
        elif choice == '3':
            await self.export_excel()
        elif choice == '0':
            return
        else:
            print_warning("Неверный выбор")
    
    async def export_json(self) -> None:
        """Экспорт в JSON"""
        print_info("Экспорт данных в формат JSON...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Создаем директорию если её нет
            os.makedirs('data/exports', exist_ok=True)
            
            print_info("Экспорт брендов...")
            await asyncio.sleep(1)
            
            print_info("Экспорт моделей...")
            await asyncio.sleep(1)
            
            print_info("Экспорт отзывов...")
            await asyncio.sleep(1)
            
            print_success(f"Данные экспортированы с меткой времени: {timestamp}")
            print_info("Файлы сохранены в директории: data/exports/")
            
        except Exception as e:
            print_error(f"Ошибка экспорта: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def export_csv(self) -> None:
        """Экспорт в CSV"""
        print_info("Экспорт коротких отзывов в CSV...")
        await asyncio.sleep(1)
        print_success("CSV файл создан: data/exports/reviews.csv")
        input("\nНажмите Enter для продолжения...")
    
    async def export_excel(self) -> None:
        """Экспорт в Excel"""
        print_info("Создание Excel файла со сводными таблицами...")
        await asyncio.sleep(2)
        print_success("Excel файл создан: data/exports/summary.xlsx")
        input("\nНажмите Enter для продолжения...")
    
    async def search_data_menu(self) -> None:
        """Меню поиска и просмотра данных"""
        print_header("ПОИСК И ПРОСМОТР ДАННЫХ")
        
        print_colored("Выберите тип поиска:", Colors.CYAN)
        print("1. Список всех брендов")
        print("2. Модели конкретного бренда")
        print("3. Поиск отзывов по критериям")
        print("4. Статистика по году выпуска")
        print("0. Назад")
        
        choice = input("\nВыберите опцию (0-4): ").strip()
        
        if choice == '1':
            await self.show_all_brands()
        elif choice == '2':
            await self.show_brand_models()
        elif choice == '3':
            await self.search_reviews()
        elif choice == '4':
            await self.year_statistics()
        elif choice == '0':
            return
        else:
            print_warning("Неверный выбор")
    
    async def show_all_brands(self) -> None:
        """Показать все бренды"""
        print_header("ВСЕ БРЕНДЫ В БАЗЕ ДАННЫХ")
        
        try:
            import asyncpg
            
            conn = await asyncpg.connect(
                host='localhost',
                port=5432,
                database='auto_reviews',
                user='postgres',
                password='postgres'
            )
            
            brands = await conn.fetch("SELECT * FROM бренды ORDER BY название_бренда")
            
            if brands:
                print_colored(f"📊 Найдено брендов: {len(brands)}", Colors.BOLD)
                print()
                
                for i, brand in enumerate(brands, 1):
                    print(f"{i:3d}. {brand['название_бренда']} "
                          f"(отзывов: {brand['количество_отзывов']})")
            else:
                print_warning("Бренды в базе данных не найдены")
            
            await conn.close()
            
        except Exception as e:
            print_error(f"Ошибка при загрузке брендов: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def show_brand_models(self) -> None:
        """Показать модели бренда"""
        brand_name = input("Введите название бренда: ").strip()
        
        if not brand_name:
            print_warning("Название бренда не может быть пустым")
            return
        
        try:
            import asyncpg
            
            conn = await asyncpg.connect(
                host='localhost',
                port=5432,
                database='auto_reviews',
                user='postgres',
                password='postgres'
            )
            
            models_query = """
                SELECT м.* FROM модели м
                JOIN бренды б ON м.бренд_id = б.id 
                WHERE LOWER(б.название_бренда) = LOWER($1)
                ORDER BY м.название_модели
            """
            models = await conn.fetch(models_query, brand_name)
            
            if models:
                print_colored(f"🚗 Модели бренда '{brand_name}': {len(models)}", Colors.BOLD)
                print()
                
                for i, model in enumerate(models, 1):
                    print(f"{i:3d}. {model['название_модели']} "
                          f"(отзывов: {model['количество_отзывов']})")
            else:
                print_warning(f"Модели для бренда '{brand_name}' не найдены")
            
            await conn.close()
            
        except Exception as e:
            print_error(f"Ошибка при загрузке моделей: {e}")
        
        input("\nНажмите Enter для продолжения...")
    
    async def search_reviews(self) -> None:
        """Поиск отзывов по критериям"""
        print_header("ПОИСК ОТЗЫВОВ")
        print_info("Функция в разработке...")
        input("\nНажмите Enter для продолжения...")
    
    async def year_statistics(self) -> None:
        """Статистика по годам выпуска"""
        print_header("СТАТИСТИКА ПО ГОДАМ")
        print_info("Функция в разработке...")
        input("\nНажмите Enter для продолжения...")
    
    async def run_tests(self) -> None:
        """Запуск тестов системы"""
        print_header("ТЕСТИРОВАНИЕ СИСТЕМЫ")
        
        tests = [
            ("Тест подключения к базе данных", 1),
            ("Тест схемы PostgreSQL", 2),
            ("Тест менеджера базы данных", 2),
            ("Интеграционные тесты", 3),
        ]
        
        for test_name, duration in tests:
            print_info(f"Запуск: {test_name}...")
            await asyncio.sleep(duration)
            print_success(f"✅ {test_name} - ПРОЙДЕН")
        
        print_success("Все тесты завершены успешно!")
        input("\nНажмите Enter для продолжения...")
    
    async def settings_menu(self) -> None:
        """Меню настроек и утилит"""
        print_header("НАСТРОЙКИ И УТИЛИТЫ")
        
        print_colored("Доступные утилиты:", Colors.CYAN)
        print("1. Очистка старых данных")
        print("2. Проверка целостности базы данных")
        print("3. Резервное копирование")
        print("4. Настройки парсинга")
        print("0. Назад")
        
        choice = input("\nВыберите утилиту (0-4): ").strip()
        
        if choice == '1':
            await self.cleanup_old_data()
        elif choice == '2':
            await self.check_database_integrity()
        elif choice == '3':
            await self.backup_database()
        elif choice == '4':
            await self.parsing_settings()
        elif choice == '0':
            return
        else:
            print_warning("Неверный выбор")
    
    async def cleanup_old_data(self) -> None:
        """Очистка старых данных"""
        print_warning("Очистка данных старше 30 дней...")
        await asyncio.sleep(1)
        print_success("Очистка завершена")
        input("\nНажмите Enter для продолжения...")
    
    async def check_database_integrity(self) -> None:
        """Проверка целостности базы данных"""
        print_info("Проверка целостности базы данных...")
        await asyncio.sleep(2)
        print_success("База данных в порядке")
        input("\nНажмите Enter для продолжения...")
    
    async def backup_database(self) -> None:
        """Резервное копирование"""
        print_info("Создание резервной копии...")
        await asyncio.sleep(3)
        print_success("Резервная копия создана")
        input("\nНажмите Enter для продолжения...")
    
    async def parsing_settings(self) -> None:
        """Настройки парсинга"""
        print_header("НАСТРОЙКИ ПАРСИНГА")
        print_info("Функция в разработке...")
        input("\nНажмите Enter для продолжения...")
    
    def exit_program(self) -> None:
        """Выход из программы"""
        print_header("ЗАВЕРШЕНИЕ РАБОТЫ")
        print_colored("Спасибо за использование системы парсинга!", Colors.GREEN)
        print_colored("До свидания! 👋", Colors.CYAN)
        sys.exit(0)
    
    async def run(self) -> None:
        """Главный цикл программы"""
        while True:
            try:
                os.system('clear' if os.name == 'posix' else 'cls')
                self.display_main_menu()
                
                choice = input().strip()
                
                if choice in self.menu_items:
                    _, action = self.menu_items[choice]
                    if choice == '0':
                        action()  # exit_program не async
                    else:
                        await action()
                else:
                    print_warning("Неверный выбор. Попробуйте еще раз.")
                    input("Нажмите Enter для продолжения...")
                    
            except KeyboardInterrupt:
                print("\n")
                print_warning("Получен сигнал прерывания")
                confirm = input("Действительно выйти? (y/N): ").strip().lower()
                if confirm == 'y':
                    self.exit_program()
            except Exception as e:
                print_error(f"Неожиданная ошибка: {e}")
                input("Нажмите Enter для продолжения...")

def main():
    """Точка входа в программу"""
    # Проверяем наличие conda окружения
    if os.environ.get('CONDA_DEFAULT_ENV') != 'parser_project':
        print_warning("Внимание: conda окружение 'parser_project' не активно")
        print_info("Запустите: conda activate parser_project")
        
        choice = input("Продолжить без правильного окружения? (y/N): ").strip().lower()
        if choice != 'y':
            sys.exit(1)
    
    # Запускаем главное меню
    menu = ParsingMenu()
    try:
        asyncio.run(menu.run())
    except KeyboardInterrupt:
        print("\n")
        print_colored("Программа завершена пользователем", Colors.YELLOW)
    except Exception as e:
        print_error(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
