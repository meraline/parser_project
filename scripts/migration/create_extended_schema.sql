-- Расширенная схема PostgreSQL с брендами, моделями и короткими отзывами
-- Версия: 2.0 с нормализацией и каталогом

-- Создаем схему auto_reviews если её нет
CREATE SCHEMA IF NOT EXISTS auto_reviews;

-- ===============================
-- СПРАВОЧНИКИ И КАТАЛОГИ
-- ===============================

-- Таблица городов (остается прежней)
CREATE TABLE IF NOT EXISTS auto_reviews.города (
    id SERIAL PRIMARY KEY,
    название VARCHAR(255) UNIQUE NOT NULL
);

-- Таблица авторов (остается прежней)
CREATE TABLE IF NOT EXISTS auto_reviews.авторы (
    id SERIAL PRIMARY KEY,
    псевдоним VARCHAR(255) UNIQUE,
    настоящее_имя VARCHAR(255),
    город_id INTEGER REFERENCES auto_reviews.города(id),
    дата_регистрации TIMESTAMP
);

-- НОВАЯ: Таблица брендов (из каталога)
CREATE TABLE IF NOT EXISTS auto_reviews.бренды (
    id SERIAL PRIMARY KEY,
    название VARCHAR(255) NOT NULL,
    название_в_url VARCHAR(255) UNIQUE NOT NULL, -- toyota, mazda, bmw
    url VARCHAR(500),                            -- полная ссылка на бренд
    количество_отзывов INTEGER DEFAULT 0,
    дата_добавления TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ОБНОВЛЕННАЯ: Таблица моделей (связанная с брендами)
CREATE TABLE IF NOT EXISTS auto_reviews.модели (
    id SERIAL PRIMARY KEY,
    бренд_id INTEGER NOT NULL REFERENCES auto_reviews.бренды(id),
    название VARCHAR(255) NOT NULL,
    название_в_url VARCHAR(255) NOT NULL,       -- familia, 4runner, x5
    поколение VARCHAR(255),
    тип_кузова VARCHAR(100),
    трансмиссия VARCHAR(100),
    тип_привода VARCHAR(100),
    руль VARCHAR(50),
    объем_двигателя_куб_см INTEGER,
    мощность_двигателя_лс INTEGER,
    тип_топлива VARCHAR(50),
    url VARCHAR(500),                           -- полная ссылка на модель
    количество_отзывов INTEGER DEFAULT 0,
    количество_коротких_отзывов INTEGER DEFAULT 0,
    дата_добавления TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(бренд_id, название_в_url, поколение, тип_кузова)
);

-- ===============================
-- ОТЗЫВЫ
-- ===============================

-- ОБНОВЛЕННАЯ: Таблица полных отзывов (связанная с моделями)
CREATE TABLE IF NOT EXISTS auto_reviews.отзывы (
    id SERIAL PRIMARY KEY,
    модель_id INTEGER NOT NULL REFERENCES auto_reviews.модели(id),
    автор_id INTEGER REFERENCES auto_reviews.авторы(id),
    
    -- Уникальные для отзыва данные
    ссылка VARCHAR(1000) UNIQUE NOT NULL,
    заголовок TEXT,
    содержание TEXT,
    плюсы TEXT,
    минусы TEXT,
    
    -- Рейтинги
    общий_рейтинг DECIMAL(3,2),
    рейтинг_владельца DECIMAL(3,2),
    
    -- Персональные данные владельца
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
    ид SERIAL PRIMARY KEY,
    модель_ид INTEGER NOT NULL REFERENCES auto_reviews.модели(ид),
    автор_ид INTEGER REFERENCES auto_reviews.авторы(ид),
    
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
    
    UNIQUE(модель_ид, внешний_id)
);

-- ===============================
-- СВЯЗАННЫЕ ДАННЫЕ
-- ===============================

-- Таблица комментариев (остается прежней, но привязывается к отзывам)
CREATE TABLE IF NOT EXISTS auto_reviews.комментарии (
    ид SERIAL PRIMARY KEY,
    отзыв_ид INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ид),
    автор_комментария_ид INTEGER REFERENCES auto_reviews.авторы(ид),
    содержание TEXT,
    дата_комментария TIMESTAMP,
    лайки INTEGER DEFAULT 0,
    дизлайки INTEGER DEFAULT 0
);

-- Таблица характеристик (остается прежней)
CREATE TABLE IF NOT EXISTS auto_reviews.характеристики (
    ид SERIAL PRIMARY KEY,
    отзыв_ид INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ид),
    название VARCHAR(255) NOT NULL,
    значение TEXT
);

-- Таблица детальных рейтингов (остается прежней)
CREATE TABLE IF NOT EXISTS auto_reviews.рейтинги_деталей (
    ид SERIAL PRIMARY KEY,
    отзыв_ід INTEGER NOT NULL REFERENCES auto_reviews.отзывы(ід),
    оценка_внешнего_вида INTEGER,
    оценка_салона INTEGER,
    оценка_двигателя INTEGER,
    оценка_управления INTEGER
);

-- Таблица расхода топлива (остается прежней)
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
    модель_id INTEGER REFERENCES auto_reviews.модели(id),
    
    начало_парсинга TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    конец_парсинга TIMESTAMP,
    статус VARCHAR(50) DEFAULT 'в_процессе', -- 'в_процессе', 'завершен', 'ошибка'
    
    обработано_страниц INTEGER DEFAULT 0,
    найдено_отзывов INTEGER DEFAULT 0,
    сохранено_отзывов INTEGER DEFAULT 0,
    ошибок INTEGER DEFAULT 0,
    
    детали_ошибки TEXT,
    версия_парсера VARCHAR(50)
);

-- ===============================
-- ИНДЕКСЫ
-- ===============================

-- Индексы для авторов
CREATE INDEX IF NOT EXISTS idx_авторы_псевдоним ON auto_reviews.авторы(псевдоним);
CREATE INDEX IF NOT EXISTS idx_авторы_город ON auto_reviews.авторы(город_id);

-- Индексы для брендов
CREATE INDEX IF NOT EXISTS idx_бренды_название_url ON auto_reviews.бренды(название_в_url);
CREATE INDEX IF NOT EXISTS idx_бренды_название ON auto_reviews.бренды(название);

-- Индексы для моделей
CREATE INDEX IF NOT EXISTS idx_модели_бренд ON auto_reviews.модели(бренд_id);
CREATE INDEX IF NOT EXISTS idx_модели_название_url ON auto_reviews.модели(название_в_url);
CREATE INDEX IF NOT EXISTS idx_модели_бренд_название ON auto_reviews.модели(бренд_id, название_в_url);

-- Индексы для полных отзывов
CREATE INDEX IF NOT EXISTS idx_отзывы_модель ON auto_reviews.отзывы(модель_id);
CREATE INDEX IF NOT EXISTS idx_отзывы_автор ON auto_reviews.отзывы(автор_id);
CREATE INDEX IF NOT EXISTS idx_отзывы_ссылка ON auto_reviews.отзывы(ссылка);
CREATE INDEX IF NOT EXISTS idx_отзывы_рейтинг ON auto_reviews.отзывы(общий_рейтинг);
CREATE INDEX IF NOT EXISTS idx_отзывы_дата ON auto_reviews.отзывы(дата_парсинга);

-- Индексы для коротких отзывов
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_модель ON auto_reviews.короткие_отзывы(модель_id);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_автор ON auto_reviews.короткие_отзывы(автор_id);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_внешний_id ON auto_reviews.короткие_отзывы(внешний_id);
CREATE INDEX IF NOT EXISTS idx_короткие_отзывы_дата ON auto_reviews.короткие_отзывы(дата_парсинга);

-- Индексы для связанных данных
CREATE INDEX IF NOT EXISTS idx_комментарии_отзыв ON auto_reviews.комментарии(отзыв_id);
CREATE INDEX IF NOT EXISTS idx_характеристики_отзыв ON auto_reviews.характеристики(отзыв_id);
CREATE INDEX IF NOT EXISTS idx_рейтинги_отзыв ON auto_reviews.рейтинги_деталей(отзыв_id);
CREATE INDEX IF NOT EXISTS idx_расход_отзыв ON auto_reviews.расход_топлива(отзыв_id);

-- Индексы для сессий парсинга
CREATE INDEX IF NOT EXISTS idx_сессии_тип ON auto_reviews.сессии_парсинга(тип_парсинга);
CREATE INDEX IF NOT EXISTS idx_сессии_статус ON auto_reviews.сессии_парсинга(статус);
CREATE INDEX IF NOT EXISTS idx_сессии_дата ON auto_reviews.сессии_парсинга(начало_парсинга);

-- ===============================
-- ФУНКЦИИ И ТРИГГЕРЫ
-- ===============================

-- Функция для обновления счетчиков отзывов
CREATE OR REPLACE FUNCTION auto_reviews.update_review_counts()
RETURNS TRIGGER AS $$
BEGIN
    -- Обновляем счетчик полных отзывов для модели
    IF TG_TABLE_NAME = 'отзывы' THEN
        IF TG_OP = 'INSERT' THEN
            UPDATE auto_reviews.модели 
            SET количество_отзывов = количество_отзывов + 1,
                дата_обновления = CURRENT_TIMESTAMP
            WHERE id = NEW.модель_id;
        ELSIF TG_OP = 'DELETE' THEN
            UPDATE auto_reviews.модели 
            SET количество_отзывов = GREATEST(количество_отзывов - 1, 0),
                дата_обновления = CURRENT_TIMESTAMP
            WHERE id = OLD.модель_id;
        END IF;
    END IF;
    
    -- Обновляем счетчик коротких отзывов для модели
    IF TG_TABLE_NAME = 'короткие_отзывы' THEN
        IF TG_OP = 'INSERT' THEN
            UPDATE auto_reviews.модели 
            SET количество_коротких_отзывов = количество_коротких_отзывов + 1,
                дата_обновления = CURRENT_TIMESTAMP
            WHERE id = NEW.модель_id;
        ELSIF TG_OP = 'DELETE' THEN
            UPDATE auto_reviews.модели 
            SET количество_коротких_отзывов = GREATEST(количество_коротких_отзывов - 1, 0),
                дата_обновления = CURRENT_TIMESTAMP
            WHERE id = OLD.модель_id;
        END IF;
    END IF;
    
    -- Обновляем общий счетчик отзывов для бренда
    UPDATE auto_reviews.бренды 
    SET количество_отзывов = (
        SELECT COALESCE(SUM(количество_отзывов + количество_коротких_отзывов), 0)
        FROM auto_reviews.модели 
        WHERE бренд_id = COALESCE(NEW.модель_id, OLD.модель_id)
    ),
    дата_обновления = CURRENT_TIMESTAMP
    WHERE id = (
        SELECT бренд_id 
        FROM auto_reviews.модели 
        WHERE id = COALESCE(NEW.модель_id, OLD.модель_id)
    );
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического обновления счетчиков
CREATE TRIGGER trigger_update_review_counts_full
    AFTER INSERT OR DELETE ON auto_reviews.отзывы
    FOR EACH ROW EXECUTE FUNCTION auto_reviews.update_review_counts();

CREATE TRIGGER trigger_update_review_counts_short
    AFTER INSERT OR DELETE ON auto_reviews.короткие_отзывы
    FOR EACH ROW EXECUTE FUNCTION auto_reviews.update_review_counts();

-- ===============================
-- КОММЕНТАРИИ К ТАБЛИЦАМ
-- ===============================

COMMENT ON SCHEMA auto_reviews IS 'Схема для хранения отзывов об автомобилях с drom.ru';

COMMENT ON TABLE auto_reviews.бренды IS 'Каталог автомобильных брендов';
COMMENT ON TABLE auto_reviews.модели IS 'Модели автомобилей, связанные с брендами';
COMMENT ON TABLE auto_reviews.отзывы IS 'Полные развернутые отзывы';
COMMENT ON TABLE auto_reviews.короткие_отзывы IS 'Короткие отзывы (5kopeek)';
COMMENT ON TABLE auto_reviews.сессии_парсинга IS 'Логи и статистика сессий парсинга';

COMMENT ON COLUMN auto_reviews.бренды.название_в_url IS 'Слаг бренда для URL (toyota, bmw, mercedes-benz)';
COMMENT ON COLUMN auto_reviews.модели.название_в_url IS 'Слаг модели для URL (camry, x5, c-class)';
COMMENT ON COLUMN auto_reviews.короткие_отзывы.внешний_id IS 'ID короткого отзыва с сайта drom.ru';
