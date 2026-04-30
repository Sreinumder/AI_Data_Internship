"""
Task 05 Analysis · News Database Analysis Report

Analyze the news articles stored in the database and generate a summary report.
"""

import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()


def generate_analysis_report(db_cursor):
    """Run analysis queries and collect results."""
    report_lines = []

    db_cursor.execute("""
        SELECT country, COUNT(*)
        FROM articles
        GROUP BY country
    """)
    country_counts = db_cursor.fetchall()
    report_lines.append("Articles per country:")
    for row in country_counts:
        report_lines.append(f"{row[0]}: {row[1]}")

    db_cursor.execute("""
        SELECT source_name, COUNT(*) as total
        FROM articles
        GROUP BY source_name
        ORDER BY total DESC
        LIMIT 5
    """)
    top_sources = db_cursor.fetchall()
    report_lines.append("\nTop 5 news sources:")
    for row in top_sources:
        report_lines.append(f"{row[0]}: {row[1]} articles")

    db_cursor.execute("""
        SELECT title, country, published_at
        FROM articles
        ORDER BY published_at DESC
        LIMIT 5
    """)
    recent_articles = db_cursor.fetchall()
    report_lines.append("\nLatest 5 articles:")
    for row in recent_articles:
        report_lines.append(f"{row[0]} ({row[1]}) - {row[2]}")

    db_cursor.execute("""
        SELECT lang, COUNT(*)
        FROM articles
        GROUP BY lang
    """)
    language_counts = db_cursor.fetchall()
    report_lines.append("\nArticles per language:")
    for row in language_counts:
        report_lines.append(f"{row[0]}: {row[1]}")

    db_cursor.execute("""
        SELECT MIN(published_at), MAX(published_at)
        FROM articles
    """)
    date_range = db_cursor.fetchone()
    report_lines.append("\nTime range:")
    report_lines.append(f"Oldest: {date_range[0]}")
    report_lines.append(f"Newest: {date_range[1]}")

    return report_lines


def write_summary_to_file(report_lines):
    """Save the analysis report to a text file."""
    with open("summary.txt", "w", encoding="utf-8") as report_file:
        for line in report_lines:
            report_file.write(line + "\n")
    print("Summary saved to summary.txt")


def run_analysis():
    """Main function to run the news analysis."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database="news_db"
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            print("Connected to MySQL database successfully!")

            analysis_report = generate_analysis_report(db_cursor)

            print("\n--- ANALYSIS REPORT ---")
            for line in analysis_report:
                print(line)

            write_summary_to_file(analysis_report)


if __name__ == "__main__":
    run_analysis()