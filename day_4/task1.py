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

def run_library_database():
    """Main function to run the library database operations."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            db_cursor.execute("CREATE DATABASE IF NOT EXISTS library_db")
            db_cursor.execute("USE library_db")

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                author VARCHAR(255),
                year INT,
                genre VARCHAR(100),
                rating REAL
            )
            """)

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id INT AUTO_INCREMENT PRIMARY KEY,
                book_id INT,
                review_text TEXT,
                rating INT,
                FOREIGN KEY (book_id) REFERENCES books(id)
            )
            """)

            book_entries = [
                ("Pride and Prejudice", "Jane Austen", 1813, "Romance", 4.6),
                ("The Lord of the Rings", "J.R.R. Tolkien", 1954, "Fantasy", 4.9),
                ("The Hunger Games", "Suzanne Collins", 2008, "Dystopian", 4.3),
                ("Gone Girl", "Gillian Flynn", 2012, "Thriller", 4.0),
                ("The Martian", "Andy Weir", 2011, "Science Fiction", 4.7),
                ("Educated", "Tara Westover", 2018, "Memoir", 4.5),
                ("Where the Crawdads Sing", "Delia Owens", 2018, "Fiction", 4.4),
                ("Becoming", "Michelle Obama", 2018, "Biography", 4.8)
            ]

            review_entries = [
                (1, "A timeless romance with witty dialogue and memorable characters.", 5),
                (2, "An unparalleled fantasy epic that defined a genre.", 5),
                (3, "A gripping survival story with strong social commentary.", 4),
                (4, "A psychological thriller that keeps you guessing until the end.", 4),
                (5, "A brilliant blend of science and humor in a survival tale.", 5),
                (6, "A powerful memoir about education and self-discovery.", 5),
                (7, "A beautifully written mystery set in the marshlands.", 4),
                (8, "An inspiring and honest account of a remarkable life.", 5)
            ]

            for book_entry in book_entries:
                db_cursor.execute("""
                INSERT INTO books (title, author, year, genre, rating)
                VALUES (%s, %s, %s, %s, %s)
                """, book_entry)

            for review_entry in review_entries:
                db_cursor.execute("""
                INSERT INTO reviews (book_id, review_text, rating)
                VALUES (%s, %s, %s)
                """, review_entry)

            db_conn.commit()

            db_cursor.execute("""
                SELECT title, author, year, genre, rating FROM books
                WHERE year > 2000
                ORDER BY rating DESC
            """)
            query_results = db_cursor.fetchall()
            print("Books published after 2000, ordered by rating:")
            for title, author, year, genre, rating in query_results:
                print(f"{title} by {author} ({year}) - Genre: {genre}, Rating: {rating}")

            db_cursor.execute("""
                SELECT title, author, year, rating FROM books
                WHERE genre = 'Fiction' AND rating > 4.0
            """)
            query_results = db_cursor.fetchall()
            print("\nFiction books with rating above 4.0:")
            for title, author, year, rating in query_results:
                print(f"{title} by {author} ({year}) - Rating: {rating}")

            db_cursor.execute("""
                SELECT AVG(rating) FROM books
            """)
            query_result = db_cursor.fetchone()
            print(f"\nAverage rating across all books: {query_result[0]:.2f}")

            db_cursor.execute("""
                SELECT genre, COUNT(*) FROM books
                GROUP BY genre
            """)
            query_results = db_cursor.fetchall()
            print("\nNumber of books per genre:")
            for genre, count in query_results:
                print(f"{genre}: {count} books")

            db_cursor.execute("""
                SELECT b.title, r.review_text, r.rating FROM books b
                JOIN reviews r ON b.id = r.book_id
            """)
            query_results = db_cursor.fetchall()
            print("\nBook reviews:")
            for title, review_text, rating in query_results:
                print(f"{title} - Review: {review_text} (Rating: {rating})")

if __name__ == "__main__":
    run_library_database()
