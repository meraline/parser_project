-- Удаляем старые таблицы если есть
DROP TABLE IF EXISTS отзывы CASCADE;
DROP TABLE IF EXISTS модели CASCADE;
DROP TABLE IF EXISTS бренды CASCADE;
DROP TABLE IF EXISTS сессии_парсинга CASCADE;

-- Создаем таблицу брендов
CREATE TABLE бренды (
    ид SERIAL PRIMARY KEY,
    название VARCHAR(100) NOT NULL UNIQUE,
    ссылка TEXT NOT NULL,
    количество_отзывов INTEGER DEFAULT 0,
    создано TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    обновлено TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу моделей
CREATE TABLE модели (
    ид SERIAL PRIMARY KEY,
    название VARCHAR(100) NOT NULL,
    бренд_ид INTEGER NOT NULL REFERENCES бренды(ид) ON DELETE CASCADE,
    ссылка TEXT NOT NULL,
    количество_отзывов INTEGER DEFAULT 0,
    создано TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    обновлено TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(название, бренд_ид)
);

-- Создаем таблицу отзывов
CREATE TABLE отзывы (
    ид SERIAL PRIMARY KEY,
    модель_ид INTEGER NOT NULL REFERENCES модели(ид) ON DELETE CASCADE,
    заголовок TEXT,
    содержание TEXT NOT NULL,
    автор VARCHAR(100),
    дата_отзыва DATE,
    рейтинг INTEGER CHECK (рейтинг >= 1 AND рейтинг <= 5),
    плюсы TEXT,
    минусы TEXT,
    год_автомобиля INTEGER,
    пробег VARCHAR(50),
    тип_топлива VARCHAR(20),
    тип_кпп VARCHAR(20),
    привод VARCHAR(20),
    город VARCHAR(100),
    ссылка TEXT,
    дром_ид VARCHAR(50) UNIQUE,
    создано TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    обновлено TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу сессий парсинга
CREATE TABLE сессии_парсинга (
    ид SERIAL PRIMARY KEY,
    начало TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    конец TIMESTAMP,
    статус VARCHAR(20) DEFAULT 'запущена' CHECK (статус IN ('запущена', 'завершена', 'ошибка')),
    обработано_брендов INTEGER DEFAULT 0,
    обработано_моделей INTEGER DEFAULT 0,
    обработано_отзывов INTEGER DEFAULT 0,
    ошибок INTEGER DEFAULT 0,
    описание TEXT
);

-- Создаем индексы для оптимизации
CREATE INDEX idx_модели_бренд_ид ON модели(бренд_ид);
CREATE INDEX idx_отзывы_модель_ид ON отзывы(модель_ид);
CREATE INDEX idx_отзывы_дром_ид ON отзывы(дром_ид);
CREATE INDEX idx_отзывы_дата ON отзывы(дата_отзыва);
CREATE INDEX idx_отзывы_рейтинг ON отзывы(рейтинг);

-- Комментарии к таблицам
COMMENT ON TABLE бренды IS 'Автомобильные бренды с сайта Drom.ru';
COMMENT ON TABLE модели IS 'Модели автомобилей для каждого бренда';
COMMENT ON TABLE отзывы IS 'Отзывы пользователей об автомобилях';
COMMENT ON TABLE сессии_парсинга IS 'Журнал сессий парсинга для отслеживания прогресса';

-- Устанавливаем владельца таблиц
ALTER TABLE бренды OWNER TO parser;
ALTER TABLE модели OWNER TO parser;
ALTER TABLE отзывы OWNER TO parser;
ALTER TABLE сессии_парсинга OWNER TO parser;
