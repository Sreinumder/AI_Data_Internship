"""
Task 05 · The Full System [Hard — Capstone]

Bring everything together — Weeks 1, 2, and 3 in one script

Goal
Build a complete automated data system — fetch, store, analyse, and export. No manual steps.

Flow:
Fetch API → Error handle → Store MySQL → Analyse → Export(csv+text)

Must:
- Fetch data from any public API with error handling
- Store ALL fetched data in a properly structured MySQL database
- Run at least 3 meaningful SQL queries and print results with labels
- Export query results to a CSV file (combine Week 1 + Week 3)
- Handle errors at every step — API, database, file

Should:
- Write reusable functions: fetch_data(), store_data(), run_report()

Bonus:
- Schedule it: run the whole thing every time you run the script fresh
"""

import requests
import mysql.connector
import pandas as pd
import os
import time
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

NEWS_API_ENDPOINT = "https://gnews.io/api/v4/search/"
NEWS_API_KEY = os.getenv("GNEWS_API")

TARGET_COUNTRIES = {
    "np": "Nepal",
    "in": "India",
    "us": "USA"
}


def fetch_news_articles():
    """Fetch news articles from GNews API for multiple countries."""
    all_articles = []
    for country_code in TARGET_COUNTRIES.keys():
        time.sleep(2)
        request_params = {
            "q": "news",
            "country": country_code,
            "apikey": NEWS_API_KEY,
            "max": 10
        }
        try:
            api_response = requests.get(NEWS_API_ENDPOINT, params=request_params)
            api_response.raise_for_status()
            response_data = api_response.json()

            for article in response_data.get("articles", []):
                all_articles.append((article, country_code))
        except requests.exceptions.RequestException as error:
            print(f"Error fetching news for {TARGET_COUNTRIES[country_code]}: {error}")

    return all_articles


def process_article(raw_article):
    """Clean and process a raw article from the API."""
    raw_date = raw_article.get("publishedAt")
    try:
        published_datetime = (
            datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%SZ")
            if raw_date else None
        )
    except Exception:
        published_datetime = None

    return {
        "id": raw_article.get("id"),
        "title": raw_article.get("title", "N/A"),
        "description": raw_article.get("description", "N/A"),
        "content": raw_article.get("content", "N/A"),
        "url": raw_article.get("url", "N/A"),
        "lang": raw_article.get("lang", "N/A"),
        "source_name": raw_article.get("source", {}).get("name", "N/A"),
        "source_url": raw_article.get("source", {}).get("url", "N/A"),
        "published_at": published_datetime
    }


def store_articles(db_connection, db_cursor, articles):
    """Store processed articles in the database."""
    for raw_article, country_code in articles:
        processed_article = process_article(raw_article)

        try:
            db_cursor.execute("""
            INSERT INTO articles
            (id, title, description, content, url, lang, source_name, source_url, country, published_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                description = VALUES(description),
                content = VALUES(content),
                url = VALUES(url),
                lang = VALUES(lang),
                source_name = VALUES(source_name),
                source_url = VALUES(source_url),
                country = VALUES(country),
                published_at = VALUES(published_at)
            """, (
                processed_article["id"],
                processed_article["title"],
                processed_article["description"],
                processed_article["content"],
                processed_article["url"],
                processed_article["lang"],
                processed_article["source_name"],
                processed_article["source_url"],
                country_code,
                processed_article["published_at"]
            ))
        except mysql.connector.Error as error:
            print(f"Error inserting article {processed_article['id']}: {error}")

    db_connection.commit()


def export_data_to_csv(db_connection):
    """Export all articles to CSV file."""
    try:
        dataframe = pd.read_sql("SELECT * FROM articles", db_connection)
        dataframe.drop_duplicates(subset=["id"], inplace=True)
        dataframe.to_csv("news_data.csv", index=False)
        print("Data exported to news_data.csv successfully!")
    except Exception as error:
        print(f"Error exporting data to CSV: {error}")


def run_news_system():
    """Main function to run the complete news system."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            db_cursor.execute("CREATE DATABASE IF NOT EXISTS news_db")
            db_cursor.execute("USE news_db")
            db_cursor.execute("DROP TABLE IF EXISTS articles")

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id VARCHAR(255) PRIMARY KEY,
                title TEXT,
                description TEXT,
                content TEXT,
                url TEXT,
                lang VARCHAR(10),
                source_name VARCHAR(255),
                source_url TEXT,
                country VARCHAR(10),
                published_at DATETIME
            )
            """)

            print("Connected to MySQL database successfully!")

            fetched_articles = fetch_news_articles()
            if fetched_articles:
                store_articles(db_conn, db_cursor, fetched_articles)
                export_data_to_csv(db_conn)
            else:
                print("No articles were fetched. Skipping storage and export.")

            print("Done: News fetched and stored successfully.")


if __name__ == "__main__":
    run_news_system()