START TRANSACTION;

DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS books CASCADE;


CREATE TABLE IF NOT EXISTS books (
    title TEXT NOT NULL PRIMARY KEY,
    description TEXT,
    authors TEXT[],
    image TEXT,
    preview_link TEXT,
    publisher TEXT,
    published_date TEXT,
    info_link TEXT,
    categories TEXT[],
    ratings_count DECIMAL(7, 1)

);


CREATE TABLE IF NOT EXISTS reviews (
    book_id VARCHAR(255) NOT NULL,
    book_title TEXT REFERENCES books(title),
    price DECIMAL(10, 2),
    user_id VARCHAR(255),
    profile_name TEXT,
    helpfulness TEXT,
    score DECIMAL(2, 1),
    review_time TIMESTAMP,
    summary TEXT,
    review_text TEXT

);


CREATE INDEX IF NOT EXISTS idx_books_title ON books(title);
CREATE INDEX IF NOT EXISTS idx_reviews_score ON reviews(score);
CREATE INDEX IF NOT EXISTS idx_reviews_book ON reviews(book_title);

COMMIT;