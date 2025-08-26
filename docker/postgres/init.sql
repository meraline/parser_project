-- Инициализация базы данных PostgreSQL для парсера отзывов Drom.ru
-- Нормализованная схема с поддержкой русских названий колонок

-- Установка кодировки и локали
SET client_encoding = 'UTF8';
SET lc_messages = 'ru_RU.UTF-8';
SET lc_monetary = 'ru_RU.UTF-8';
SET lc_numeric = 'ru_RU.UTF-8';
SET lc_time = 'ru_RU.UTF-8';

-- Создание схемы
CREATE SCHEMA IF NOT EXISTS auto_reviews;
SET search_path TO auto_reviews, public;

-- Таблица брендов автомобилей
CREATE TABLE IF NOT EXISTS бренды (
    ид SERIAL PRIMARY KEY,
    название TEXT NOT NULL UNIQUE,
    имя_url TEXT NOT NULL UNIQUE,
    полный_url TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица моделей автомобилей
CREATE TABLE IF NOT EXISTS модели (
    ид SERIAL PRIMARY KEY,
    ид_бренда INTEGER NOT NULL,
    название TEXT NOT NULL,
    имя_url TEXT NOT NULL,
    полный_url TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    количество_коротких_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ид_бренда) REFERENCES бренды(ид) ON DELETE CASCADE,
    UNIQUE(ид_бренда, имя_url)
);

-- Таблица отзывов (длинные и короткие)
CREATE TABLE IF NOT EXISTS отзывы (
    ид SERIAL PRIMARY KEY,
    ид_модели INTEGER NOT NULL,
    тип_отзыва TEXT NOT NULL DEFAULT 'длинный', -- 'длинный' или 'короткий'
    
    -- Основная информация отзыва
    заголовок TEXT,
    содержание TEXT,
    плюсы TEXT,
    минусы TEXT,
    поломки TEXT,
    
    -- Информация об авторе
    имя_автора TEXT,
    город_автора TEXT,
    дата_отзыва DATE,
    
    -- Характеристики автомобиля
    год_автомобиля INTEGER,
    объем_двигателя DECIMAL(3,1),
    тип_топлива TEXT,
    коробка_передач TEXT,
    тип_привода TEXT,
    тип_кузова TEXT,
    цвет TEXT,
    пробег INTEGER,
    
    -- Оценки (по 5-балльной шкале)
    общая_оценка DECIMAL(2,1),
    оценка_комфорта DECIMAL(2,1),
    оценка_надежности DECIMAL(2,1),
    оценка_расхода_топлива DECIMAL(2,1),
    оценка_управления DECIMAL(2,1),
    оценка_внешнего_вида DECIMAL(2,1),
    
    -- Дополнительные характеристики для коротких отзывов
    стоимость_покупки DECIMAL(12,2),
    стоимость_обслуживания DECIMAL(12,2),
    
    -- Метаданные
    исходный_url TEXT,
    ид_отзыва_на_сайте TEXT,
    количество_фото INTEGER DEFAULT 0,
    полезность INTEGER DEFAULT 0,
    дата_создания TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ид_модели) REFERENCES модели(ид) ON DELETE CASCADE
);

-- Создание индексов для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_бренды_имя_url ON бренды(имя_url);
CREATE INDEX IF NOT EXISTS idx_модели_ид_бренда ON модели(ид_бренда);
CREATE INDEX IF NOT EXISTS idx_модели_имя_url ON модели(имя_url);
CREATE INDEX IF NOT EXISTS idx_отзывы_ид_модели ON отзывы(ид_модели);
CREATE INDEX IF NOT EXISTS idx_отзывы_тип ON отзывы(тип_отзыва);
CREATE INDEX IF NOT EXISTS idx_отзывы_дата ON отзывы(дата_отзыва);
CREATE INDEX IF NOT EXISTS idx_отзывы_год_автомобиля ON отзывы(год_автомобиля);
CREATE INDEX IF NOT EXISTS idx_отзывы_общая_оценка ON отзывы(общая_оценка);

-- Создание функции для автоматического обновления времени
CREATE OR REPLACE FUNCTION обновить_дату_изменения()
RETURNS TRIGGER AS $$
BEGIN
    NEW.дата_обновления = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического обновления времени
CREATE TRIGGER trigger_обновление_бренды
    BEFORE UPDATE ON бренды
    FOR EACH ROW
    EXECUTE FUNCTION обновить_дату_изменения();

CREATE TRIGGER trigger_обновление_модели
    BEFORE UPDATE ON модели
    FOR EACH ROW
    EXECUTE FUNCTION обновить_дату_изменения();

-- Создание представлений для удобного анализа данных
CREATE OR REPLACE VIEW статистика_по_брендам AS
SELECT 
    б.название AS бренд,
    COUNT(м.ид) AS количество_моделей,
    SUM(м.количество_отзывов) AS общее_количество_отзывов,
    AVG(о.общая_оценка) AS средняя_оценка
FROM бренды б
LEFT JOIN модели м ON б.ид = м.ид_бренда
LEFT JOIN отзывы о ON м.ид = о.ид_модели
GROUP BY б.ид, б.название
ORDER BY общее_количество_отзывов DESC;

CREATE OR REPLACE VIEW топ_модели_по_оценкам AS
SELECT 
    б.название AS бренд,
    м.название AS модель,
    COUNT(о.ид) AS количество_отзывов,
    ROUND(AVG(о.общая_оценка), 2) AS средняя_оценка,
    ROUND(AVG(о.оценка_надежности), 2) AS средняя_надежность,
    ROUND(AVG(о.оценка_комфорта), 2) AS средний_комфорт
FROM бренды б
JOIN модели м ON б.ид = м.ид_бренда
JOIN отзывы о ON м.ид = о.ид_модели
WHERE о.общая_оценка IS NOT NULL
GROUP BY б.ид, б.название, м.ид, м.название
HAVING COUNT(о.ид) >= 5
ORDER BY средняя_оценка DESC, количество_отзывов DESC;

-- Создание пользователя для приложения (если запускается не в Docker)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'parser_app') THEN
        CREATE ROLE parser_app WITH LOGIN PASSWORD 'parser_app_password';
    END IF;
END
$$;

-- Предоставление прав доступа
GRANT USAGE ON SCHEMA auto_reviews TO parser_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA auto_reviews TO parser_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA auto_reviews TO parser_app;

-- Комментарии к таблицам
COMMENT ON TABLE бренды IS 'Каталог автомобильных брендов';
COMMENT ON TABLE модели IS 'Модели автомобилей для каждого бренда';
COMMENT ON TABLE отзывы IS 'Отзывы пользователей о моделях автомобилей';

-- Комментарии к ключевым колонкам
COMMENT ON COLUMN отзывы.тип_отзыва IS 'Тип отзыва: длинный (подробный) или короткий';
COMMENT ON COLUMN отзывы.общая_оценка IS 'Общая оценка от 1 до 5 баллов';
COMMENT ON COLUMN отзывы.объем_двигателя IS 'Объем двигателя в литрах';
COMMENT ON COLUMN отзывы.пробег IS 'Пробег автомобиля в километрах';
