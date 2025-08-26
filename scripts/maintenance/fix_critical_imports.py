#!/usr/bin/env python3
"""
Скрипт для исправления критических импортов в основных рабочих файлах.
Не трогает файлы в archive папках, которые предназначены для архивирования.
"""

import os
import sys

# Добавляем src в путь для импорта
sys.path.insert(0, '/home/analityk/Документы/projects/parser_project/src')

def fix_imports():
    """Исправляет основные проблемы с импортами"""
    
    # Основные файлы для исправления импортов
    fixes = {
        # CLI файлы
        'cli/main.py': [
            ('auto_reviews_parser.database.base', 'src.auto_reviews_parser.database.base'),
            ('auto_reviews_parser.database.repositories.review_repository', 'src.auto_reviews_parser.database.repositories.review_repository'),
            ('auto_reviews_parser.parsers.drom', 'src.auto_reviews_parser.parsers.drom_reviews'),
            ('auto_reviews_parser.parsers.drive2', 'src.auto_reviews_parser.parsers.drive2'),
            ('auto_reviews_parser.services.parser_service', 'src.auto_reviews_parser.services.queue_service'),
            ('auto_reviews_parser.services.parallel_parser', 'src.auto_reviews_parser.services.queue_service'),
            ('cli.init_db', 'init_db'),
        ],
        
        # Основные скрипты
        'scripts/main.py': [
            ('src.auto_reviews_parser.database.schema', 'src.auto_reviews_parser.database.schema'),
            ('src.auto_reviews_parser.catalog.initializer', 'src.auto_reviews_parser.catalog.initializer'),
            ('src.auto_reviews_parser.parsers.drom_reviews', 'src.auto_reviews_parser.parsers.drom_reviews'),
        ],
        
        # Основные src файлы
        'src/auto_reviews_parser/__init__.py': [
            ('database.base', '.database.base'),
        ],
        
        'src/auto_reviews_parser/utils/retry_decorator.py': [
            ('delay_manager', '.delay_manager'),
        ],
        
        'src/auto_reviews_parser/utils/health.py': [
            ('services.auto_reviews_parser', '..services.queue_service'),
        ],
        
        'src/auto_reviews_parser/database/repositories/review_repository.py': [
            ('base', '..base'),
            ('models', '...models'),
            ('utils.logger', '...utils.logger'),
        ],
        
        'src/auto_reviews_parser/database/repositories/comment_repository.py': [
            ('base', '..base'),
            ('models.comment', '...models.comment'),
            ('utils.logger', '...utils.logger'),
        ],
        
        'src/auto_reviews_parser/database/repositories/queue_repository.py': [
            ('base', '..base'),
        ],
        
        'src/auto_reviews_parser/models/__init__.py': [
            ('review', '.review'),
        ],
        
        'src/auto_reviews_parser/models/extended_review.py': [
            ('review', '.review'),
        ],
        
        'src/auto_reviews_parser/catalog/initializer.py': [
            ('database.schema', '..database.schema'),
        ],
        
        'src/auto_reviews_parser/analyzers/__init__.py': [
            ('data_analyzer', '.data_analyzer'),
        ],
        
        'src/auto_reviews_parser/parsers/__init__.py': [
            ('base', '.base'),
            ('drom_reviews', '.drom_reviews'),
            ('models.review', '..models.review'),
        ],
        
        'src/auto_reviews_parser/parsers/drom_reviews.py': [
            ('database.schema', '..database.schema'),
        ],
        
        'src/auto_reviews_parser/parsers/base.py': [
            ('models.review', '..models.review'),
            ('utils.delay_manager', '..utils.delay_manager'),
            ('utils.metrics', '..utils.metrics'),
        ],
        
        'src/auto_reviews_parser/parsers/sync_base.py': [
            ('models.review', '..models.review'),
            ('utils.delay_manager', '..utils.delay_manager'),
            ('utils.retry_decorator', '..utils.retry_decorator'),
            ('utils.metrics', '..utils.metrics'),
        ],
        
        'src/auto_reviews_parser/parsers/drive2.py': [
            ('base', '.base'),
            ('models', '..models'),
            ('utils.logger', '..utils.logger'),
        ],
    }
    
    base_path = '/home/analityk/Документы/projects/parser_project'
    
    for file_path, imports_to_fix in fixes.items():
        full_path = os.path.join(base_path, file_path)
        
        if not os.path.exists(full_path):
            print(f"⚠️  Файл не найден: {full_path}")
            continue
            
        print(f"🔧 Исправляем импорты в {file_path}")
        
        # Читаем файл
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"❌ Ошибка чтения файла {full_path}: {e}")
            continue
        
        # Исправляем импорты
        modified = False
        for old_import, new_import in imports_to_fix:
            old_line = f"from {old_import} import"
            new_line = f"from {new_import} import"
            
            if old_line in content:
                content = content.replace(old_line, new_line)
                modified = True
                print(f"  ✅ {old_import} → {new_import}")
        
        # Сохраняем файл если были изменения
        if modified:
            try:
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"  💾 Сохранено")
            except Exception as e:
                print(f"  ❌ Ошибка записи: {e}")
        else:
            print(f"  ℹ️  Импорты уже корректны")

if __name__ == "__main__":
    print("🔧 Исправление критических импортов...")
    fix_imports()
    print("✅ Завершено")
