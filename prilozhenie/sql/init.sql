-- Создание таблиц
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    bio TEXT,
    birth_date DATE,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE publishers (
    publisher_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    address TEXT,
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genres (
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    isbn VARCHAR(20) UNIQUE,
    title VARCHAR(255) NOT NULL,
    publisher_id INT REFERENCES publishers(publisher_id) ON DELETE SET NULL,
    publication_year INT CHECK (publication_year > 1800 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    page_count INT CHECK (page_count > 0),
    price DECIMAL(10,2) CHECK (price >= 0),
    quantity_in_stock INT DEFAULT 1 CHECK (quantity_in_stock >= 0),
    description TEXT,
    language VARCHAR(30) DEFAULT 'Русский',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE book_authors (
    book_id INT REFERENCES books(book_id) ON DELETE CASCADE,
    author_id INT REFERENCES authors(author_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, author_id)
);

CREATE TABLE book_genres (
    book_id INT REFERENCES books(book_id) ON DELETE CASCADE,
    genre_id INT REFERENCES genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, genre_id)
);

CREATE TABLE readers (
    reader_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    notes TEXT
);

CREATE TABLE book_loans (
    loan_id SERIAL PRIMARY KEY,
    book_id INT REFERENCES books(book_id) ON DELETE CASCADE,
    reader_id INT REFERENCES readers(reader_id) ON DELETE CASCADE,
    loan_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    due_date TIMESTAMP NOT NULL,
    return_date TIMESTAMP,
    fine_amount DECIMAL(10,2) DEFAULT 0.00 CHECK (fine_amount >= 0),
    is_returned BOOLEAN DEFAULT false,
    notes TEXT
);

CREATE TABLE fines (
    fine_id SERIAL PRIMARY KEY,
    loan_id INT REFERENCES book_loans(loan_id) ON DELETE CASCADE,
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    reason TEXT NOT NULL,
    issue_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_paid BOOLEAN DEFAULT false,
    paid_date TIMESTAMP
);

-- Вставка начальных данных
INSERT INTO authors (first_name, last_name, bio, birth_date) VALUES
('Лев', 'Толстой', 'Русский писатель, один из самых известных писателей мира', '1828-09-09'),
('Федор', 'Достоевский', 'Русский писатель, мыслитель, философ и публицист', '1821-11-11'),
('Агата', 'Кристи', 'Английская писательница, автор детективных романов и рассказов', '1890-09-15'),
('Джоан', 'Роулинг', 'Британская писательница, автор серии книг о Гарри Поттере', '1965-07-31'),
('Стивен', 'Кинг', 'Американский писатель в жанре ужасов, фантастики и триллера', '1947-09-21');

INSERT INTO publishers (name, address, phone, email) VALUES
('Эксмо', 'Москва, Россия', '+7 (495) 123-45-67', 'info@eksmo.ru'),
('АСТ', 'Москва, Россия', '+7 (495) 789-10-11', 'contact@ast.ru'),
('HarperCollins', 'Нью-Йорк, США', '+1 (212) 207-7000', 'info@harpercollins.com'),
('Penguin Books', 'Лондон, Великобритания', '+44 (20) 7139-4848', 'enquiries@penguin.co.uk');

INSERT INTO genres (name, description) VALUES
('Роман', 'Художественное произведение эпического характера'),
('Детектив', 'Литературный жанр, в котором действие строится на раскрытии преступления'),
('Фэнтези', 'Жанр литературы, основанный на использовании мифологических и фольклорных мотивов'),
('Ужасы', 'Жанр литературы, который стремится вызвать у читателя страх'),
('Научная литература', 'Литература, излагающая результаты научных исследований');

INSERT INTO books (isbn, title, publisher_id, publication_year, page_count, price, quantity_in_stock, description, language) VALUES
('978-5-07-081066-1', 'Война и мир', 1, 1869, 1300, 850.00, 5, 'Эпопея Льва Толстого', 'Русский'),
('978-5-17-115625-0', 'Преступление и наказание', 2, 1866, 670, 650.00, 3, 'Роман Федора Достоевского', 'Русский'),
('978-0-00-815308-4', 'Убийство в Восточном экспрессе', 4, 1934, 256, 550.00, 7, 'Детектив Агаты Кристи', 'Английский'),
('978-0-7475-3274-3', 'Гарри Поттер и философский камень', 4, 1997, 332, 990.00, 10, 'Фэнтези для детей и взрослых', 'Английский'),
('978-0-385-12167-5', 'Сияние', 3, 1977, 447, 790.00, 4, 'Роман ужасов Стивена Кинга', 'Английский');

INSERT INTO book_authors (book_id, author_id) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

INSERT INTO book_genres (book_id, genre_id) VALUES
(1, 1), (2, 1), (3, 2), (4, 3), (5, 4);

INSERT INTO readers (first_name, last_name, email, phone, address) VALUES
('Александр', 'Иванов', 'alex.ivanov@example.com', '+7 (926) 123-45-67', 'г. Москва, ул. Тверская, д. 10, кв. 5'),
('Мария', 'Петрова', 'maria.petrova@example.com', '+7 (916) 234-56-78', 'г. Москва, ул. Арбат, д. 15, кв. 22'),
('Дмитрий', 'Сидоров', 'dmitry.sidorov@example.com', '+7 (903) 345-67-89', 'г. Москва, ул. Ленина, д. 20, кв. 33'),
('Екатерина', 'Козлова', 'ekaterina.kozlova@example.com', '+7 (925) 456-78-90', 'г. Москва, ул. Пушкина, д. 25, кв. 44');

INSERT INTO book_loans (book_id, reader_id, loan_date, due_date, return_date, fine_amount, is_returned) VALUES
(1, 1, '2023-01-15 10:30:00', '2023-02-15 23:59:59', '2023-02-10 14:20:00', 0.00, true),
(2, 2, '2023-01-20 11:15:00', '2023-02-20 23:59:59', NULL, 0.00, false),
(3, 3, '2023-01-25 13:45:00', '2023-02-25 23:59:59', '2023-03-05 16:30:00', 150.00, true),
(4, 4, '2023-02-01 09:20:00', '2023-03-01 23:59:59', NULL, 0.00, false),
(5, 1, '2023-02-05 15:10:00', '2023-03-05 23:59:59', NULL, 0.00, false);

INSERT INTO fines (loan_id, amount, reason, is_paid, paid_date) VALUES
(3, 150.00, 'Просрочка возврата на 8 дней', true, '2023-03-06 10:15:00'),
(2, 75.00, 'Просрочка возврата на 3 дня', false, NULL);