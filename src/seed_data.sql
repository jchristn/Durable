-- Seed data for test database

-- Insert Companies
INSERT OR IGNORE INTO companies (id, name, industry) VALUES 
(1, 'Penguin Random House', 'Publishing'),
(2, 'HarperCollins Publishers', 'Publishing'),
(3, 'Simon & Schuster', 'Publishing');

-- Insert Categories
INSERT OR IGNORE INTO categories (id, name, description) VALUES 
(1, 'Horror', 'Horror and supernatural fiction'),
(2, 'Fantasy', 'Fantasy and magical realism'),
(3, 'Mystery', 'Mystery and detective fiction'),
(4, 'Thriller', 'Thriller and suspense'),
(5, 'Fiction', 'General fiction'),
(6, 'Young Adult', 'Young adult literature');

-- Insert Books for existing authors
INSERT OR IGNORE INTO books (id, title, author_id, publisher_id) VALUES 
-- Stephen King books (AuthorId: 1)
(1, 'The Shining', 1, 1),
(2, 'It', 1, 2),
(3, 'The Stand', 1, 1),
(4, 'Pet Sematary', 1, 3),
(5, 'Carrie', 1, 1),

-- J.K. Rowling books (AuthorId: 2)
(6, 'Harry Potter and the Philosopher''s Stone', 2, 1),
(7, 'Harry Potter and the Chamber of Secrets', 2, 1),
(8, 'Harry Potter and the Prisoner of Azkaban', 2, 1),
(9, 'Harry Potter and the Goblet of Fire', 2, 1),
(10, 'The Casual Vacancy', 2, 2),

-- George R.R. Martin books (AuthorId: 3)
(11, 'A Game of Thrones', 3, 2),
(12, 'A Clash of Kings', 3, 2),
(13, 'A Storm of Swords', 3, 2),
(14, 'A Feast for Crows', 3, 2),
(15, 'A Dance with Dragons', 3, 2),

-- Agatha Christie books (AuthorId: 4)
(16, 'Murder on the Orient Express', 4, 3),
(17, 'Death on the Nile', 4, 3),
(18, 'And Then There Were None', 4, 3),
(19, 'The Murder of Roger Ackroyd', 4, 3),
(20, 'The ABC Murders', 4, 3),

-- John Grisham books (AuthorId: 5)
(21, 'The Firm', 5, 1),
(22, 'The Pelican Brief', 5, 1),
(23, 'The Client', 5, 1),
(24, 'A Time to Kill', 5, 1),
(25, 'The Runaway Jury', 5, 1);

-- Insert Author-Category relationships
INSERT OR IGNORE INTO author_categories (author_id, category_id) VALUES 
-- Stephen King - Horror, Thriller, Fiction
(1, 1),
(1, 4),
(1, 5),

-- J.K. Rowling - Fantasy, Young Adult, Fiction
(2, 2),
(2, 6),
(2, 5),

-- George R.R. Martin - Fantasy, Fiction
(3, 2),
(3, 5),

-- Agatha Christie - Mystery, Thriller, Fiction
(4, 3),
(4, 4),
(4, 5),

-- John Grisham - Thriller, Mystery, Fiction
(5, 4),
(5, 3),
(5, 5);