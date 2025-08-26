#!/usr/bin/env python3
"""
Скрипт для проверки всех импортов в проекте
Проверяет синтаксис и импорты во всех Python файлах
"""

import os
import sys
import ast
import importlib.util
from pathlib import Path
from typing import List, Tuple, Dict

def find_python_files(directory: str) -> List[Path]:
    """Найти все Python файлы в директории"""
    python_files = []
    for root, dirs, files in os.walk(directory):
        # Пропускаем папки с виртуальными окружениями и кэшем
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'parser_env', 'chrome-linux']]
        
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)
    
    return python_files

def check_syntax(file_path: Path) -> Tuple[bool, str]:
    """Проверить синтаксис Python файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        ast.parse(content)
        return True, ""
    except SyntaxError as e:
        return False, f"Syntax error: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"

def check_imports(file_path: Path) -> Tuple[bool, List[str]]:
    """Проверить импорты в файле"""
    errors = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    try:
                        importlib.import_module(alias.name)
                    except ImportError as e:
                        errors.append(f"Import error: {alias.name} - {e}")
            
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    try:
                        importlib.import_module(node.module)
                    except ImportError as e:
                        errors.append(f"Import error: {node.module} - {e}")
        
        return len(errors) == 0, errors
    
    except Exception as e:
        return False, [f"Error checking imports: {e}"]

def main():
    """Основная функция проверки"""
    print("🔍 Проверка всех импортов в проекте...")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent.parent
    python_files = find_python_files(str(project_root))
    
    total_files = len(python_files)
    syntax_errors = []
    import_errors = []
    
    print(f"📁 Найдено {total_files} Python файлов")
    print()
    
    for i, file_path in enumerate(python_files, 1):
        relative_path = file_path.relative_to(project_root)
        print(f"[{i}/{total_files}] Проверяю {relative_path}")
        
        # Проверка синтаксиса
        syntax_ok, syntax_error = check_syntax(file_path)
        if not syntax_ok:
            syntax_errors.append((relative_path, syntax_error))
            print(f"  ❌ Синтаксическая ошибка: {syntax_error}")
            continue
        
        # Проверка импортов
        imports_ok, import_error_list = check_imports(file_path)
        if not imports_ok:
            import_errors.append((relative_path, import_error_list))
            print(f"  ⚠️  Ошибки импортов: {len(import_error_list)}")
            for error in import_error_list[:3]:  # Показываем только первые 3
                print(f"      {error}")
            if len(import_error_list) > 3:
                print(f"      ... и ещё {len(import_error_list) - 3}")
        else:
            print(f"  ✅ OK")
    
    print()
    print("=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ:")
    print(f"  Всего файлов: {total_files}")
    print(f"  Синтаксических ошибок: {len(syntax_errors)}")
    print(f"  Файлов с ошибками импортов: {len(import_errors)}")
    
    if syntax_errors:
        print("\n❌ СИНТАКСИЧЕСКИЕ ОШИБКИ:")
        for file_path, error in syntax_errors:
            print(f"  {file_path}: {error}")
    
    if import_errors:
        print("\n⚠️  ОШИБКИ ИМПОРТОВ:")
        for file_path, errors in import_errors:
            print(f"  {file_path}:")
            for error in errors:
                print(f"    - {error}")
    
    if not syntax_errors and not import_errors:
        print("\n🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        return True
    else:
        print(f"\n⚠️  Найдены проблемы в {len(syntax_errors + import_errors)} файлах")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
