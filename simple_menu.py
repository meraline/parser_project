#!/usr/bin/env python3
"""
Простое интерактивное меню для управления системой парсинга автомобильных отзывов
Использует прямые подключения к PostgreSQL через asyncpg
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

# Цвета для терминала
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

def print_colored(text: str, color: str = Colors.WHITE) -> None:
    """Печать цветного текста"""
    print(f"{color}{text}{Colors.RESET}")

def print_header(text: str) -> None:
    """Печать заголовка"""
    print("\n" + "=" * 60)
    print_colored(f"  {text}  ", Colors.BOLD + Colors.CYAN)
    print("=" * 60)

def print_success(text: str) -> None:
    """Печать сообщения об успехе"""
    print_colored(f"✅ {text}", Colors.GREEN)

def print_warning(text: str) -> None:
    """Печать предупреждения"""
    print_colored(f"⚠️  {text}", Colors.YELLOW)

def print_error(text: str) -> None:
    """Печать ошибки"""
    print_colored(f"❌ {text}", Colors.RED)

def print_info(text: str) -> None:
    """Печать информации"""
    print_colored(f"ℹ️  {text}", Colors.BLUE)

class SimpleMenu:
    """Простое меню для работы с базой данных"""
    
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'auto_reviews',
            'user': 'parser',
            'password': 'parser'
        }
    
    async def get_connection(self):
        """Получить подключение к базе данных"""
        try:
            import asyncpg
            return await asyncpg.connect(**self.db_config)
        except ImportError:
            print_error("Модуль asyncpg не установлен. Установите: pip install asyncpg")
            return None
        except Exception as e:
            print_error(f"Ошибка подключения к базе данных: {e}")
            return None
    
    async def run(self) -> None:
        """Запуск главного меню"""
        while True:
            self.show_main_menu()
            choice = input("\nВыберите пункт меню: ").strip()
            
            if choice == '0':
                print_info("До свидания!")
                break
            elif choice == '1':
                await self.show_statistics()
            elif choice == '2':
                await self.show_all_brands()
            elif choice == '3':
                await self.show_brand_models()
            elif choice == '4':
                await self.search_reviews()
            elif choice == '5':
                await self.export_menu()
            elif choice == '6':
                await self.run_parsing()
            elif choice == '7':
                await self.database_tools()
            else:
                print_warning("Неверный выбор. Попробуйте снова.")
    
    def show_main_menu(self) -> None:
        """Показать главное меню"""
        print_header("СИСТЕМА УПРАВЛЕНИЯ АВТОМОБИЛЬНЫМИ ОТЗЫВАМИ")
        
        menu_items = [
            ("1", "📊", "Статистика базы данных"),
            ("2", "🏢", "Просмотр всех брендов"),
            ("3", "🚗", "Просмотр моделей бренда"),
            ("4", "🔍", "Поиск отзывов"),
            ("5", "📤", "Экспорт данных"),
            ("6", "⚙️", "Запуск парсинга"),
            ("7", "🛠️", "Инструменты базы данных"),
            ("0", "🚪", "Выход")
        ]
        
        for num, icon, desc in menu_items:
            print(f"  {num}. {icon} {desc}")
    
    async def show_statistics(self) -> None:
        """Показать статистику базы данных"""
        print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            # Статистика по брендам
            brands_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.бренды")
            print_info(f"Всего брендов: {brands_count}")
            
            # Статистика по моделям
            models_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.модели")
            print_info(f"Всего моделей: {models_count}")
            
            # Статистика по отзывам
            reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.отзывы")
            print_info(f"Длинных отзывов: {reviews_count}")
            
            # Статистика по коротким отзывам
            short_reviews_count = await conn.fetchval("SELECT COUNT(*) FROM auto_reviews.короткие_отзывы")
            print_info(f"Коротких отзывов: {short_reviews_count}")
            
            # Топ-5 брендов по количеству отзывов
            top_brands = await conn.fetch("""
                SELECT название, количество_отзывов 
                FROM auto_reviews.бренды 
                WHERE количество_отзывов > 0
                ORDER BY количество_отзывов DESC 
                LIMIT 5
            """)
            
            if top_brands:
                print("\n" + "🏆 ТОП-5 БРЕНДОВ ПО ОТЗЫВАМ:")
                for i, brand in enumerate(top_brands, 1):
                    print(f"  {i}. {brand['название']}: {brand['количество_отзывов']} отзывов")
            
        except Exception as e:
            print_error(f"Ошибка при получении статистики: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def show_all_brands(self) -> None:
        """Показать все бренды"""
        print_header("ВСЕ БРЕНДЫ В БАЗЕ ДАННЫХ")
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            brands = await conn.fetch("SELECT * FROM auto_reviews.бренды ORDER BY название")
            
            if brands:
                print_colored(f"📊 Найдено брендов: {len(brands)}", Colors.BOLD)
                print()
                
                for i, brand in enumerate(brands, 1):
                    print(f"{i:3d}. {brand['название']} "
                          f"(отзывов: {brand['количество_отзывов']})")
            else:
                print_warning("Бренды в базе данных не найдены")
                
        except Exception as e:
            print_error(f"Ошибка при загрузке брендов: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def show_brand_models(self) -> None:
        """Показать модели бренда"""
        brand_name = input("Введите название бренда: ").strip()
        
        if not brand_name:
            print_warning("Название бренда не может быть пустым")
            return
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            models_query = """
                SELECT м.* FROM auto_reviews.модели м
                JOIN auto_reviews.бренды б ON м.бренд_id = б.id 
                WHERE LOWER(б.название) = LOWER($1)
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
                
        except Exception as e:
            print_error(f"Ошибка при загрузке моделей: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def search_reviews(self) -> None:
        """Поиск отзывов"""
        search_term = input("Введите поисковый запрос: ").strip()
        
        if not search_term:
            print_warning("Поисковый запрос не может быть пустым")
            return
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            # Поиск в длинных отзывах
            long_query = """
                SELECT о.id, о.заголовок, б.название, м.название_модели
                FROM auto_reviews.отзывы о
                JOIN auto_reviews.модели м ON о.модель_id = м.id
                JOIN auto_reviews.бренды б ON м.бренд_id = б.id
                WHERE о.текст_отзыва ILIKE $1 OR о.заголовок ILIKE $1
                ORDER BY о.дата_публикации DESC
                LIMIT 10
            """
            long_reviews = await conn.fetch(long_query, f'%{search_term}%')
            
            # Поиск в коротких отзывах
            short_query = """
                SELECT ко.id, б.название, м.название_модели, ко.год_выпуска
                FROM auto_reviews.короткие_отзывы ко
                JOIN auto_reviews.модели м ON ко.модель_id = м.id
                JOIN auto_reviews.бренды б ON м.бренд_id = б.id
                WHERE ко.плюсы ILIKE $1 OR ко.минусы ILIKE $1 OR ко.поломки ILIKE $1
                ORDER BY ко.дата_публикации DESC
                LIMIT 10
            """
            short_reviews = await conn.fetch(short_query, f'%{search_term}%')
            
            print_header(f"РЕЗУЛЬТАТЫ ПОИСКА: '{search_term}'")
            
            if long_reviews:
                print_colored(f"📄 Длинные отзывы: {len(long_reviews)}", Colors.BOLD)
                for review in long_reviews:
                    title = review['заголовок'] or 'Без заголовка'
                    print(f"- {review['название']} {review['название_модели']}: {title[:50]}...")
            
            if short_reviews:
                print_colored(f"💬 Короткие отзывы: {len(short_reviews)}", Colors.BOLD)
                for review in short_reviews:
                    year = review['год_выпуска'] or 'н/д'
                    print(f"- {review['название']} {review['название_модели']} ({year})")
            
            if not long_reviews and not short_reviews:
                print_warning("Отзывы по вашему запросу не найдены")
                
        except Exception as e:
            print_error(f"Ошибка при поиске: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def export_menu(self) -> None:
        """Меню экспорта данных"""
        print_header("ЭКСПОРТ ДАННЫХ")
        print("1. Экспорт всех брендов")
        print("2. Экспорт моделей бренда")
        print("3. Экспорт отзывов модели")
        print("0. Назад")
        
        choice = input("\nВыберите тип экспорта: ").strip()
        
        if choice == "1":
            await self.export_brands()
        elif choice == "2":
            await self.export_brand_models()
        elif choice == "3":
            await self.export_model_reviews()
        elif choice == "0":
            return
        else:
            print_warning("Неверный выбор")
    
    async def export_brands(self) -> None:
        """Экспорт брендов в JSON"""
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            brands = await conn.fetch("SELECT * FROM auto_reviews.бренды ORDER BY название")
            
            brands_data = []
            for brand in brands:
                brands_data.append({
                    'id': brand['id'],
                    'название': brand['название'],
                    'ссылка': brand['ссылка'],
                    'количество_отзывов': brand['количество_отзывов'],
                    'дата_добавления': brand['дата_добавления'].isoformat() if brand['дата_добавления'] else None
                })
            
            # Создаем директорию если не существует
            os.makedirs('data/exports', exist_ok=True)
            
            filename = f"data/exports/brands_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(brands_data, f, ensure_ascii=False, indent=2)
            
            print_success(f"Экспорт завершен: {filename}")
            print_colored(f"Экспортировано брендов: {len(brands_data)}", Colors.BOLD)
            
        except Exception as e:
            print_error(f"Ошибка при экспорте: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def export_brand_models(self) -> None:
        """Экспорт моделей бренда"""
        brand_name = input("Введите название бренда: ").strip()
        if not brand_name:
            print_warning("Название бренда не может быть пустым")
            return
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            models_query = """
                SELECT м.*, б.название FROM auto_reviews.модели м
                JOIN auto_reviews.бренды б ON м.бренд_id = б.id 
                WHERE LOWER(б.название) = LOWER($1)
                ORDER BY м.название_модели
            """
            models = await conn.fetch(models_query, brand_name)
            
            if not models:
                print_warning(f"Модели для бренда '{brand_name}' не найдены")
                return
            
            models_data = []
            for model in models:
                models_data.append({
                    'id': model['id'],
                    'название_модели': model['название_модели'],
                    'бренд': model['название'],
                    'ссылка': model['ссылка'],
                    'количество_отзывов': model['количество_отзывов'],
                    'дата_добавления': model['дата_добавления'].isoformat() if model['дата_добавления'] else None
                })
            
            os.makedirs('data/exports', exist_ok=True)
            
            safe_brand_name = "".join(c for c in brand_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"data/exports/models_{safe_brand_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(models_data, f, ensure_ascii=False, indent=2)
            
            print_success(f"Экспорт завершен: {filename}")
            print_colored(f"Экспортировано моделей: {len(models_data)}", Colors.BOLD)
            
        except Exception as e:
            print_error(f"Ошибка при экспорте: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def export_model_reviews(self) -> None:
        """Экспорт отзывов модели"""
        print_info("Функция экспорта отзывов в разработке...")
        input("\nНажмите Enter для продолжения...")
    
    async def run_parsing(self) -> None:
        """Запуск парсинга"""
        print_header("ЗАПУСК ПАРСИНГА")
        print("1. Парсинг каталога брендов")
        print("2. Парсинг коротких отзывов")
        print("3. Полный цикл парсинга")
        print("0. Назад")
        
        choice = input("\nВыберите тип парсинга: ").strip()
        
        if choice == "1":
            print_info("Запуск парсинга каталога...")
            print_info("Используйте: ./quick_start.sh для автоматического парсинга")
        elif choice == "2":
            print_info("Запуск парсинга коротких отзывов...")
            print_info("Используйте: ./quick_start.sh для автоматического парсинга")
        elif choice == "3":
            print_info("Запуск полного цикла...")
            print_info("Используйте: ./quick_start.sh для автоматического парсинга")
        elif choice == "0":
            return
        else:
            print_warning("Неверный выбор")
        
        input("\nНажмите Enter для продолжения...")
    
    async def database_tools(self) -> None:
        """Инструменты базы данных"""
        print_header("ИНСТРУМЕНТЫ БАЗЫ ДАННЫХ")
        print("1. Проверка подключения")
        print("2. Проверка схемы")
        print("3. Очистка тестовых данных")
        print("0. Назад")
        
        choice = input("\nВыберите инструмент: ").strip()
        
        if choice == "1":
            await self.check_connection()
        elif choice == "2":
            await self.check_schema()
        elif choice == "3":
            await self.cleanup_test_data()
        elif choice == "0":
            return
        else:
            print_warning("Неверный выбор")
    
    async def check_connection(self) -> None:
        """Проверка подключения к базе данных"""
        print_info("Проверка подключения к PostgreSQL...")
        
        conn = await self.get_connection()
        if conn:
            try:
                result = await conn.fetchval("SELECT version()")
                print_success("Подключение к базе данных успешно!")
                print_info(f"Версия PostgreSQL: {result}")
            except Exception as e:
                print_error(f"Ошибка при проверке версии: {e}")
            finally:
                await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def check_schema(self) -> None:
        """Проверка схемы базы данных"""
        print_info("Проверка схемы базы данных...")
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            
            expected_tables = [
                'бренды', 'модели', 'отзывы', 'короткие_отзывы',
                'авторы_отзывов', 'сессии_парсинга', 'логи_парсинга'
            ]
            
            existing_tables = [table['table_name'] for table in tables]
            
            print_colored("Существующие таблицы:", Colors.BOLD)
            for table in existing_tables:
                if table in expected_tables:
                    print_success(f"✅ {table}")
                else:
                    print_info(f"   {table}")
            
            print_colored("\nОтсутствующие таблицы:", Colors.BOLD)
            for table in expected_tables:
                if table not in existing_tables:
                    print_error(f"❌ {table}")
            
        except Exception as e:
            print_error(f"Ошибка при проверке схемы: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")
    
    async def cleanup_test_data(self) -> None:
        """Очистка тестовых данных"""
        confirm = input("Вы уверены, что хотите очистить тестовые данные? (да/нет): ").strip().lower()
        
        if confirm not in ['да', 'yes', 'y']:
            print_info("Операция отменена")
            return
        
        conn = await self.get_connection()
        if not conn:
            return
        
        try:
            # Очистка данных в порядке зависимостей
            tables_to_clean = [
                'короткие_отзывы',
                'отзывы', 
                'модели',
                'бренды',
                'авторы_отзывов',
                'логи_парсинга',
                'сессии_парсинга'
            ]
            
            async with conn.transaction():
                for table in tables_to_clean:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    if count > 0:
                        await conn.execute(f"DELETE FROM {table}")
                        print_success(f"Очищена таблица {table}: {count} записей")
                    else:
                        print_info(f"Таблица {table} уже пуста")
            
            print_success("Очистка тестовых данных завершена!")
            
        except Exception as e:
            print_error(f"Ошибка при очистке данных: {e}")
        finally:
            await conn.close()
        
        input("\nНажмите Enter для продолжения...")

async def main():
    """Основная функция"""
    try:
        menu = SimpleMenu()
        await menu.run()
    except KeyboardInterrupt:
        print_info("\n\nПрограмма прервана пользователем")
    except Exception as e:
        print_error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(main())
