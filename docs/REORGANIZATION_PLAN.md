# 🔄 ПЛАН РЕОРГАНИЗАЦИИ ФАЙЛОВ

## 📁 Текущие файлы в корне и их правильное размещение:

### ✅ ОСТАЮТСЯ В КОРНЕ:
- `setup.py` - конфигурация установки пакета
- `quick_start.sh` - основной скрипт запуска (главный интерфейс)
- `simple_menu.py` - главное интерактивное меню

### 🔄 ПЕРЕМЕСТИТЬ:

#### В `scripts/database/`:
- `check_db_structure.py` → `scripts/database/check_structure.py`

#### В `scripts/analysis/`:
- `show_detailed_data.py` → `scripts/analysis/show_detailed_data.py`
- `show_stats.py` → `scripts/analysis/show_stats.py`

#### В `scripts/maintenance/`:
- `update_simple_menu.py` → `scripts/maintenance/update_menu.py`

#### В `scripts/setup/`:
- `interactive_menu.py` → `scripts/setup/interactive_menu.py` (или удалить, если дублирует simple_menu.py)

---

## 🎯 ОБОСНОВАНИЕ:

### Файлы, которые ДОЛЖНЫ остаться в корне:
1. **`setup.py`** - стандарт Python для конфигурации пакета
2. **`quick_start.sh`** - главная точка входа для пользователей
3. **`simple_menu.py`** - основной интерфейс взаимодействия

### Файлы для перемещения:
1. **Database утилиты** → `scripts/database/`
2. **Анализ данных** → `scripts/analysis/`  
3. **Обслуживание системы** → `scripts/maintenance/`
4. **Настройка системы** → `scripts/setup/`

---

## 📋 КОМАНДЫ ДЛЯ РЕОРГАНИЗАЦИИ:

```bash
# Создаем недостающие папки
mkdir -p scripts/analysis
mkdir -p scripts/maintenance

# Перемещаем файлы базы данных
mv check_db_structure.py scripts/database/check_structure.py

# Перемещаем утилиты анализа
mv show_detailed_data.py scripts/analysis/
mv show_stats.py scripts/analysis/

# Перемещаем утилиты обслуживания
mv update_simple_menu.py scripts/maintenance/update_menu.py

# Анализируем interactive_menu.py
# (нужно проверить, не дублирует ли simple_menu.py)
```

---

## ⚠️ ВАЖНЫЕ ЗАМЕЧАНИЯ:

1. **Обновить ссылки** в документации после перемещения
2. **Проверить импорты** в переносимых файлах
3. **Обновить quick_start.sh** если он ссылается на перемещенные файлы
4. **Обновить simple_menu.py** для корректных путей к скриптам

---

## 🔧 ПОСЛЕ ПЕРЕМЕЩЕНИЯ ОБНОВИТЬ:

### В `simple_menu.py`:
```python
# Изменить пути к скриптам
"scripts/analysis/show_stats.py"
"scripts/analysis/show_detailed_data.py" 
"scripts/database/check_structure.py"
```

### В `quick_start.sh`:
```bash
# Обновить пути к Python скриптам
python scripts/analysis/show_stats.py
python scripts/database/check_structure.py
```

### В документации:
- Обновить все ссылки на перемещенные файлы
- Исправить примеры команд в README и гайдах
