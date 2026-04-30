"""
Task 01 · Create, Insert & Query [Medium]
Independent task — build your first MySQL database from scratch
Goal:
Create a MySQL database, populate it with data, and run queries to answer questions about it.
1. Create a database called library.db with a table books
   (id, title, author, year, genre, rating REAL)
2. Insert at least 8 books — use a mix of genres, years, and ratings
3. Query 1:
   SELECT all books published after 2000, ordered by rating (highest first)
4. Query 2:
   SELECT all books in the 'Fiction' genre with rating above 4.0
5. Query 3:
   Find the average rating across all books
6. Query 4:
   Count how many books exist per genre — use GROUP BY genre
7. Print all query results neatly with labels — not just raw tuples

Bonus : Add a reviews table and link it to books via book_id foreign key
Deliverable: library.db + script showing all 4 query outputs
"""
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

try:
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        )
    cursor = conn.cursor()
except mysql.connector.Error as err:
    print(f"Error connecting to MySQL: {err}")
    exit(1)

cursor.execute("create database if not exists library_db")
cursor.execute("use library_db")

cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    author VARCHAR(255),
    year INT,
    genre VARCHAR(100),
    rating REAL
)
""")

cursor.execute("""
create table if not exists reviews (
    id int auto_increment primary key,
    book_id int,
    review_text text,
    rating int,
    foreign key (book_id) references books(id)
)
""")

books_data = [
    ("Pride and Prejudice", "Jane Austen", 1813, "Romance", 4.6),
    ("The Lord of the Rings", "J.R.R. Tolkien", 1954, "Fantasy", 4.9),
    ("The Hunger Games", "Suzanne Collins", 2008, "Dystopian", 4.3),
    ("Gone Girl", "Gillian Flynn", 2012, "Thriller", 4.0),
    ("The Martian", "Andy Weir", 2011, "Science Fiction", 4.7),
    ("Educated", "Tara Westover", 2018, "Memoir", 4.5),
    ("Where the Crawdads Sing", "Delia Owens", 2018, "Fiction", 4.4),
    ("Becoming", "Michelle Obama", 2018, "Biography", 4.8)
]

reviews_data = [
    (1, "A timeless romance with witty dialogue and memorable characters.", 5),
    (2, "An unparalleled fantasy epic that defined a genre.", 5),
    (3, "A gripping survival story with strong social commentary.", 4),
    (4, "A psychological thriller that keeps you guessing until the end.", 4),
    (5, "A brilliant blend of science and humor in a survival tale.", 5),
    (6, "A powerful memoir about education and self-discovery.", 5),
    (7, "A beautifully written mystery set in the marshlands.", 4),
    (8, "An inspiring and honest account of a remarkable life.", 5)
]

for book in books_data:
    cursor.execute("""
    INSERT INTO books (title, author, year, genre, rating)
    VALUES (%s, %s, %s, %s, %s)
    """, book)

for review in reviews_data:
    cursor.execute("""
    INSERT INTO reviews (book_id, review_text, rating)
    VALUES (%s, %s, %s)
    """, review)

conn.commit()

cursor.execute("""
    SELECT title, author, year, genre, rating FROM books
    WHERE year > 2000
    ORDER BY rating DESC
""")
results = cursor.fetchall()
print("Books published after 2000, ordered by rating:")
for title, author, year, genre, rating in results:
    print(f"{title} by {author} ({year}) - Genre: {genre}, Rating: {rating}")
    

cursor.execute("""
    SELECT title, author, year, rating FROM books
    WHERE genre = 'Fiction' AND rating > 4.0
""")
results = cursor.fetchall()
print("\nFiction books with rating above 4.0:")
for title, author, year, rating in results:
    print(f"{title} by {author} ({year}) - Rating: {rating}")   
    

cursor.execute("""
    SELECT AVG(rating) FROM books
""")
result = cursor.fetchone()
print(f"\nAverage rating across all books: {result[0]:.2f}")

cursor.execute("""
    SELECT genre, COUNT(*) FROM books
    GROUP BY genre
""")
results = cursor.fetchall()
print("\nNumber of books per genre:")
for genre, count in results:
    print(f"{genre}: {count} books")
    

cursor.execute("""
    SELECT b.title, r.review_text, r.rating FROM books b
    JOIN reviews r ON b.id = r.book_id
""")
results = cursor.fetchall()
print("\nBook reviews:")
for title, review_text, rating in results:
    print(f"{title} - Review: {review_text} (Rating: {rating})")

cursor.close()
conn.close()
