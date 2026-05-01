# Task 02 · API → MySQL Pipeline [Hard]
# Goal:
# Fetch user data from an API, store it in MySQL, and query it — complete automated pipeline.
# 1. Fetch all users from:
# https://jsonplaceholder.typicode.com/users
# 2. Create app.db with a users table:
# id, name, email, phone, city, company_name
# 3. Extract:
# - city from address.city
# - company_name from company.name (nested JSON)
# 4. Insert all 10 users into the database with proper error handling
# 5. Query 1:
# Print all users sorted alphabetically by name
# 6. Query 2:
# Find users from the same city
# (GROUP BY city, HAVING COUNT > 1)
# 7. Add a second table: posts
# Fetch from:
# https://jsonplaceholder.typicode.com/posts
# Insert only posts where user_id is 1, 2, and 3
# Deliverable:
# app.db with users + posts tables + both query outputs
# Bonus:
# JOIN users and posts and print each user's posts

import os
import requests
from dotenv import load_dotenv
import mysql.connector

load_dotenv()


def run_app_pipeline():
    """Main function to run the API to MySQL pipeline."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            db_cursor.execute("CREATE DATABASE IF NOT EXISTS app_db")
            db_cursor.execute("USE app_db")

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255) UNIQUE,
                phone VARCHAR(50) UNIQUE,
                city VARCHAR(100),
                company_name VARCHAR(255)
            )
            """)

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY,
                user_id INT,
                title VARCHAR(255),
                body TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )""")

            api_response = requests.get("https://jsonplaceholder.typicode.com/users")
            if api_response.status_code == 200:
                user_data = api_response.json()
                for user in user_data:
                    try:
                        db_cursor.execute("""
                        INSERT INTO users (id, name, email, phone, city, company_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                name = VALUES(name),
                                email = VALUES(email),
                                phone = VALUES(phone),
                                city = VALUES(city),
                                company_name = VALUES(company_name)
                        """, (
                            user.get("id"),
                            user.get("name"),
                            user.get("email"),
                            user.get("phone"),
                            user.get("address", {}).get("city"),
                            user.get("company", {}).get("name")
                        ))
                    except Exception as error:
                        print("User insert error:", error)

            db_conn.commit()

            posts_response = requests.get("https://jsonplaceholder.typicode.com/posts")
            if posts_response.status_code == 200:
                posts_data = posts_response.json()
                for post in posts_data:
                    if post["userId"] in [1, 2, 3]:
                        try:
                            db_cursor.execute("""
                            INSERT INTO posts (id, user_id, title, body)
                            VALUES (%s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    user_id = VALUES(user_id),
                                    title = VALUES(title),
                                    body = VALUES(body)
                            """, (
                                post.get("id"),
                                post.get("userId"),
                                post.get("title"),
                                post.get("body")
                            ))
                        except Exception as error:
                            print("Post insert error:", error)

            db_conn.commit()

            print("\n--- All Users (sorted by name) ---")
            db_cursor.execute("select name, email, city from users order by name")
            query_results = db_cursor.fetchall()
            for name, email, city in query_results:
                print(f"Name: {name}, Email: {email}, City: {city}")

            print("\n--- Users from the same city ---")
            db_cursor.execute("""
            SELECT city, COUNT(*)
            FROM users
            GROUP BY city
            HAVING COUNT(*) > 1
            """)
            query_results = db_cursor.fetchall()
            for city, count in query_results:
                print(f"City: {city}, Count: {count}")

            print("\n--- Users and their posts ---")
            db_cursor.execute("""
            SELECT users.name, posts.title
            FROM users
            JOIN posts ON users.id = posts.user_id
            ORDER BY users.name
            """)
            query_results = db_cursor.fetchall()
            for name, title in query_results:
                print(f"User: {name}, Post Title: {title}")


if __name__ == "__main__":
    run_app_pipeline()