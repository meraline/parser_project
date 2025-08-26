-- ===============================
-- ENHANCED POSTGRESQL SCHEMA
-- ===============================

-- Создаем схему auto_reviews
CREATE SCHEMA IF NOT EXISTS auto_reviews;

-- ===============================
-- БАЗОВЫЕ СПРАВОЧНИКИ
-- ===============================

-- Таблица брендов (автопроизводителей)
CREATE TABLE IF NOT EXISTS auto_reviews.бренды (
    ід SERIAL PRIMARY KEY,
    название VARCHAR(255) UNIQUE NOT NULL,
    название_в_url VARCHAR(255),
    полная_ссылка TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица моделей автомобилей
CREATE TABLE IF NOT EXISTS auto_reviews.модели (
    ід SERIAL PRIMARY KEY,
    ід_бренда INTEGER NOT NULL REFERENCES auto_reviews.бренды(ід),
    название VARCHAR(255) NOT NULL,
    название_в_url VARCHAR(255),
    полная_ссылка TEXT,
    количество_отзывов INTEGER DEFAULT 0,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(ід_бренда, название)
);

-- Таблица городов
CREATE TABLE IF NOT EXISTS auto_reviews.города (
    ід SERIAL PRIMARY KEY,
    название VARCHAR(255) UNIQUE NOT NULL
);

-- Таблица авторов отзывов
CREATE TABLE IF NOT EXISTS auto_reviews.авторы (
    ід SERIAL PRIMARY KEY,
    псевдоним VARCHAR(255) UNIQUE,
    настоящее_имя VARCHAR(255),
    город_ід INTEGER REFERENCES auto_reviews.города(ід),
    дата_регистрации TIMESTAMP
);

-- ===============================
-- ОТЗЫВЫ
-- ===============================

-- Таблица полных отзывов (расширенная версия существующей)
CREATE TABLE IF NOT EXISTS auto_reviews.отзывы (
    ід SERIAL PRIMARY KEY,
    модель_ід INTEGER NOT NULL REFERENCES auto_reviews.модели(ід),
    автор_ід INTEGER REFERENCES auto_reviews.авторы(ід),
    
    -- Базовая информация
    ссылка TEXT UNIQUE NOT NULL,
    заголовок TEXT,
    содержание TEXT,
    плюсы TEXT,
    минусы TEXT,
    
    -- Рейтинги
    общий_рейтинг DECIMAL(3,1),
    рейтинг_владельца DECIMAL(3,1),
    
    -- Характеристики автомобиля
    год_приобретения INTEGER,
    пробег_км INTEGER,
    цвет_кузова VARCHAR(100),
    цвет_салона VARCHAR(100),
    
    -- Статистика
    количество_просмотров INTEGER DEFAULT 0,
    количество_лайков INTEGER DEFAULT 0,
    количество_дизлайков INTEGER DEFAULT 0,
    количество_голосов INTEGER DEFAULT 0,
    
    -- Даты
    дата_публикации TIMESTAMP,
    дата_парсинга TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Мета-данные
    длина_контента INTEGER,
    хеш_содержания VARCHAR(64),
    статус_парсинга VARCHAR(50) DEFAULT 'успех',
    детали_ошибки TEXT
);

-- НОВАЯ: Таблица коротких отзывов
CREATE TABLE IF NOT EXISTS auto_reviews.короткие_отзывы (
    ід SERIAL PRIMARY KEY,
    модель_ід INTEGER NOT NULL REFERENCES auto_reviews.модели(ід),
    автор_ід INTEGER REFERENCES auto_reviews.авторы(ід),
    
    -- Уникальный идентификатор отзыва с сайта
    внешний_id VARCHAR(50),
    
    -- Основное содержание
    плюсы TEXT,
    минусы TEXT,
    поломки TEXT,
    
    -- Информация об автомобиле
    год_автомобиля INTEGER,
    объем_двигателя DECIMAL(3,1),
    тип_топлива VARCHAR(50),
    тип_трансмиссии VARCHAR(50),
    тип_привода VARCHAR(50),
    
    -- Дополнительная информация
    количество_фото INTEGER DEFAULT 0,
    город_автора VARCHAR(255),
    
    -- Даты
    дата_публикации TIMESTAMP,
    дата_парсинга TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Мета-данные
    хеш_содержания VARCHAR(64),
    статус_парсинга VARCHAR(50) DEFAULT 'успех',
    
    UNIQUE(модель_ід, внешний_id)
);

-- ===============================
-- СВЯЗАННЫЕ ДАННЫЕ
-- ===============================

-- Таблица комментариев
CREATE TABLE IF NOT EXISTS auto_reviews.комментарии (
    ід SERIAL PRIMARY KEY,
    отзыв_ід INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ід),
    автор_комментария_ід INTEGER REFERENCES auto_reviews.авторы(ід),
    содержание TEXT,
    дата_комментария TIMESTAMP,
    лайки INTEGER DEFAULT 0,
    дизлайки INTEGER DEFAULT 0
);

-- Таблица характеристик
CREATE TABLE IF NOT EXISTS auto_reviews.характеристики (
    ід SERIAL PRIMARY KEY,
    отзыв_ід INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ід),
    название VARCHAR(255) NOT NULL,
    значение TEXT
);

-- Таблица детальных рейтингов
CREATE TABLE IF NOT EXISTS auto_reviews.рейтинги_деталей (
    ід SERIAL PRIMARY KEY,
    отзыв_ід INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ід),
    оценка_внешнего_вида INTEGER,
    оценка_салона INTEGER,
    оценка_двигателя INTEGER,
    оценка_управления INTEGER
);

-- Таблица расхода топлива
CREATE TABLE IF NOT EXISTS auto_reviews.расход_топлива (
    ід SERIAL PRIMARY KEY,
    отзыв_ід INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ід),
    расход_город_л_100км DECIMAL(4,1),
    расход_трасса_л_100км DECIMAL(4,1),
    расход_смешанный_л_100км DECIMAL(4,1)
);

-- ===============================
-- СЕССИИ ПАРСИНГА
-- ===============================

-- Таблица сессий парсинга для отслеживания прогресса
CREATE TABLE IF NOT EXISTS auto_reviews.сессии_парсинга (
    ід SERIAL PRIMARY KEY,
    тип_парсинга VARCHAR(50) NOT NULL,  -- 'catalog', 'full_reviews', 'short_reviews'
    бренд_ід INTEGER REFERENCES auto_reviews.бренды(ід),
    модель_ід INTEGER REFERENCES auto_reviews.модели(ід),
    
    -- Статистика сессии
    статус VARCHAR(50) DEFAULT 'запущена',  -- 'запущена', 'завершена', 'ошибка'
    начало_сессии TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    конец_сессии TIMESTAMP,
    
    -- Результаты парсинга
    всего_найдено INTEGER DEFAULT 0,
    успешно_спарсено INTEGER DEFAULT 0,
    ошибок INTEGER DEFAULT 0,
    
    -- Детали
    сообщение_ошибки TEXT,
    параметры_парсинга JSONB,
    
    UNIQUE(тип_парсинга, бренд_ід, модель_ід, начало_сессии)
);

-- ===============================
-- ИНДЕКСЫ
-- ===============================

-- Основные индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_бренды_название_в_url ON auto_reviews.бренды(название_в_url);
CREATE INDEX IF NOT EXISTS idx_модели_ід_бренда ON auto_reviews.модели(ід_бренда);
CREATE INDEX IF NOT EXISTS idx_модели_название_в_url ON auto_reviews.модели(название_в_url);
CREATE INDEX IF NOT EXISTS idx_авторы_псевдоним ON auto_reviews.авторы(псевдоним);

-- Индексы для отзывов
CREATE INDEX IF NOT EXISTS idx_отзывы_модель_ід ON auto_reviews.отзывы(модель_ід);
CREATE INDEX IF NOT EXISTS idx_отзывы_автор_ід ON auto_reviews.отзывы(автор_ід);
CREATE INDEX IF NOT EXISTS idx_отзывы_ссылка ON auto_reviews.отзывы(ссылка);
CREATE INDEX IF NOT EXISTS idx_отзывы_общий_рейтинг ON auto_reviews.отзывы(общий_рейтинг);
CREATE INDEX IF NOT EXISTS idx_отзывы_дата_парсинга ON auto_reviews.отзывы(дата_парсинга);

-- Индексы для коротких отзывов
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_модель_ід ON auto_reviews.короткие_отзывы(модель_ід);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_автор_ід ON auto_reviews.короткие_отзывы(автор_ід);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_внешний_id ON auto_reviews.короткие_отзывы(внешний_id);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_дата_парсинга ON auto_reviews.короткие_отзывы(дата_парсинга);

-- Индексы для связанных таблиц
CREATE INDEX IF NOT EXISTS idx_комментарии_отзыв_ід ON auto_reviews.комментарии(отзыв_ід);
CREATE INDEX IF NOT EXISTS idx_характеристики_отзыв_ід ON auto_reviews.характеристики(отзыв_ід);
CREATE INDEX IF NOT EXISTS idx_рейтинги_деталей_отзыв_ід ON auto_reviews.рейтинги_деталей(отзыв_ід);
CREATE INDEX IF NOT EXISTS idx_расход_топлива_отзыв_ід ON auto_reviews.расход_топлива(отзыв_ід);

-- Индексы для сессий парсинга
CREATE INDEX IF NOT EXISTS idx_сессии_парсинга_тип ON auto_reviews.сессии_парсинга(тип_парсинга);
CREATE INDEX IF NOT EXISTS idx_сессии_парсинга_статус ON auto_reviews.сессии_парсинга(статус);
CREATE INDEX IF NOT EXISTS idx_сессии_парсинга_начало ON auto_reviews.сессии_парсинга(начало_сессии);

-- ===============================
-- ТРИГГЕРЫ И ФУНКЦИИ
-- ===============================

-- Функция автоматического подсчета количества отзывов
CREATE OR REPLACE FUNCTION auto_reviews.update_review_counts()
RETURNS TRIGGER AS $$
BEGIN
    -- Обновляем счетчик отзывов для модели
    UPDATE auto_reviews.модели 
    SET количество_отзывов = (
        SELECT COUNT(*) 
        FROM auto_reviews.отзывы 
        WHERE модель_ід = NEW.модель_ід
    ) + (
        SELECT COUNT(*) 
        FROM auto_reviews.короткие_отзывы 
        WHERE модель_ід = NEW.модель_ід
    )
    WHERE ід = NEW.модель_ід;
    
    -- Обновляем счетчик отзывов для бренда
    UPDATE auto_reviews.бренды 
    SET количество_отзывов = (
        SELECT SUM(количество_отзывов) 
        FROM auto_reviews.модели 
        WHERE ід_бренда = (
            SELECT ід_бренда 
            FROM auto_reviews.модели 
            WHERE ід = NEW.модель_ід
        )
    )
    WHERE ід = (
        SELECT ід_бренда 
        FROM auto_reviews.модели 
        WHERE ід = NEW.модель_ід
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического подсчета
CREATE TRIGGER trigger_update_review_counts_full
    AFTER INSERT OR DELETE ON auto_reviews.отзывы
    FOR EACH ROW EXECUTE FUNCTION auto_reviews.update_review_counts();

CREATE TRIGGER trigger_update_review_counts_short
    AFTER INSERT OR DELETE ON auto_reviews.короткие_отзывы
    FOR EACH ROW EXECUTE FUNCTION auto_reviews.update_review_counts();

-- ===============================
-- КОММЕНТАРИИ К ТАБЛИЦАМ
-- ===============================

COMMENT ON SCHEMA auto_reviews IS 'Схема для системы парсинга автомобильных отзывов';
COMMENT ON TABLE auto_reviews.бренды IS 'Справочник автомобильных брендов';
COMMENT ON TABLE auto_reviews.модели IS 'Справочник моделей автомобилей';
COMMENT ON TABLE auto_reviews.отзывы IS 'Полные детальные отзывы пользователей';
COMMENT ON TABLE auto_reviews.короткие_отзывы IS 'Короткие отзывы с базовой информацией';
COMMENT ON TABLE auto_reviews.сессии_парсинга IS 'Отслеживание сессий парсинга данных';

-- ===============================
-- НАЧАЛЬНЫЕ ДАННЫЕ И НАСТРОЙКИ
-- ===============================

-- Настройка search_path для удобства
-- ALTER ROLE parser SET search_path TO auto_reviews, public;
