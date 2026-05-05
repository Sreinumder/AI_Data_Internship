# Task B · API Monitor with Change Detection [Hard]
# Using mysql-connector-python and https://jsonplaceholder.typicode.com/posts,
# build a monitoring script that detects changes between runs.
# Create a database called monitor_db with two tables — posts (stores fetched data) and
# change_log (records what changed and when). On first run, insert all posts and log each as 'NEW'.
# Then manually update one row in MySQL (UPDATE posts SET title='Changed' WHERE id=1) and
# re-run — your script must detect the mismatch and log it as 'MODIFIED' in change_log.
# Print results for: post count per user, all change log entries from the latest run,
# and which user triggered the most change events.
# Wrap every API call and DB operation in try/except with printed error messages.
# Deliverable: MySQL monitor_db with both tables populated + Python script

import os
import json
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

import mysql.connector
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env", override=True)

url = os.getenv("POSTS_API_URL", "https://jsonplaceholder.typicode.com/posts")


def get_connection(db=None):
    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=db,
        )
    except Exception as e:
        print(f"[DB ERROR] Connection failed: {e}")
        return None


def create_database_table():
    conn = None
    cursor = None

    try:
        conn = get_connection()
        if conn is None:
            return False

        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS monitor_db")
        print("[DB] Database ensured: monitor_db")
    except Exception as e:
        print(f"[DB ERROR] Database creation failed: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    try:
        conn = get_connection("monitor_db")
        if conn is None:
            return False

        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY,
                userId INT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                last_seen_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_log (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(36) NOT NULL,
                post_id INT NOT NULL,
                userId INT NOT NULL,
                change_type VARCHAR(20) NOT NULL,
                old_title TEXT,
                new_title TEXT,
                changed_at DATETIME NOT NULL,
                FOREIGN KEY (post_id) REFERENCES posts(id)
                    ON DELETE CASCADE
            )
        """)

        ensure_table_columns(cursor)

        conn.commit()
        print("[DB] Setup complete")
        return True
    except Exception as e:
        print(f"[DB ERROR] Table creation failed: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def column_exists(cursor, table_name, column_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
    return cursor.fetchone() is not None


def ensure_table_columns(cursor):
    try:
        if column_exists(cursor, "posts", "user_id") and not column_exists(cursor, "posts", "userId"):
            cursor.execute("ALTER TABLE posts CHANGE user_id userId INT NOT NULL")

        if column_exists(cursor, "change_log", "user_id") and not column_exists(cursor, "change_log", "userId"):
            cursor.execute("ALTER TABLE change_log CHANGE user_id userId INT NOT NULL")

        if not column_exists(cursor, "change_log", "run_id"):
            cursor.execute("ALTER TABLE change_log ADD run_id VARCHAR(36) NOT NULL DEFAULT 'legacy-run'")

        if not column_exists(cursor, "change_log", "old_title"):
            cursor.execute("ALTER TABLE change_log ADD old_title TEXT")

        if not column_exists(cursor, "change_log", "new_title"):
            cursor.execute("ALTER TABLE change_log ADD new_title TEXT")

        if not column_exists(cursor, "posts", "last_seen_at"):
            cursor.execute("ALTER TABLE posts ADD last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")

        if not column_exists(cursor, "posts", "updated_at"):
            cursor.execute("ALTER TABLE posts ADD updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")
    except Exception as e:
        print(f"[DB ERROR] Schema compatibility update failed: {e}")
        raise


def fetch_api():
    try:
        with urlopen(url, timeout=10) as response:
            posts = json.loads(response.read().decode("utf-8"))
        print(f"[API] Fetched {len(posts)} posts")
        return posts
    except Exception as e:
        print(f"[API ERROR] {e}")
        return []


def load_db_posts(cursor):
    try:
        cursor.execute("SELECT id, userId, title, body FROM posts")
        return {
            row[0]: {"userId": row[1], "title": row[2], "body": row[3]}
            for row in cursor.fetchall()
        }
    except Exception as e:
        print(f"[DB ERROR] Loading posts failed: {e}")
        return {}


def log_change(cursor, run_id, post, change_type, old_title=None):
    try:
        cursor.execute("""
            INSERT INTO change_log (
                run_id,
                post_id,
                userId,
                change_type,
                old_title,
                new_title,
                changed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id,
            post["id"],
            post["userId"],
            change_type,
            old_title,
            post["title"],
            datetime.now().replace(microsecond=0),
        ))
    except Exception as e:
        print(f"[DB ERROR] Logging {change_type} for post {post['id']} failed: {e}")
        raise


def sync():
    conn = None
    cursor = None
    run_id = str(uuid4())
    run_time = datetime.now().replace(microsecond=0)

    try:
        conn = get_connection("monitor_db")
        if conn is None:
            return None

        cursor = conn.cursor()
        api_data = fetch_api()
        if not api_data:
            return None

        db_posts = load_db_posts(cursor)
        new_count = 0
        mod_count = 0

        for post in api_data:
            pid = post["id"]

            if pid not in db_posts:
                try:
                    cursor.execute("""
                        INSERT INTO posts (id, userId, title, body, last_seen_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        pid,
                        post["userId"],
                        post["title"],
                        post["body"],
                        run_time,
                        run_time,
                    ))
                    log_change(cursor, run_id, post, "NEW")
                    new_count += 1
                except Exception as e:
                    print(f"[DB ERROR] Insert failed for post {pid}: {e}")
                    raise

            else:
                old = db_posts[pid]

                if (
                    old["title"] != post["title"]
                    or old["body"] != post["body"]
                    or old["userId"] != post["userId"]
                ):
                    try:
                        cursor.execute("""
                            UPDATE posts
                            SET userId=%s,
                                title=%s,
                                body=%s,
                                last_seen_at=%s,
                                updated_at=%s
                            WHERE id=%s
                        """, (
                            post["userId"],
                            post["title"],
                            post["body"],
                            run_time,
                            run_time,
                            pid,
                        ))
                        log_change(cursor, run_id, post, "MODIFIED", old["title"])
                        mod_count += 1
                    except Exception as e:
                        print(f"[DB ERROR] Update failed for post {pid}: {e}")
                        raise
                else:
                    try:
                        cursor.execute("""
                            UPDATE posts
                            SET last_seen_at=%s
                            WHERE id=%s
                        """, (run_time, pid))
                    except Exception as e:
                        print(f"[DB ERROR] Last-seen update failed for post {pid}: {e}")
                        raise

        conn.commit()
        print(f"[SYNC] Run ID: {run_id}")
        print(f"[SYNC] Done | NEW: {new_count} | MODIFIED: {mod_count}")
        return run_id

    except Exception as e:
        print(f"[SYNC ERROR] {e}")
        if conn:
            conn.rollback()
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_analysis(run_id):
    conn = None
    cursor = None

    try:
        conn = get_connection("monitor_db")
        if conn is None:
            return

        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT userId, COUNT(*)
                FROM posts
                GROUP BY userId
                ORDER BY userId
            """)

            print("\n--- Posts per User ---")
            for userId, count in cursor.fetchall():
                print(f"User {userId}: {count}")
        except Exception as e:
            print(f"[DB ERROR] Posts-per-user query failed: {e}")

        try:
            cursor.execute("""
                SELECT post_id, userId, change_type, old_title, new_title, changed_at
                FROM change_log
                WHERE run_id = %s
                ORDER BY log_id
            """, (run_id,))

            print("\n--- Change Log From Latest Run ---")
            rows = cursor.fetchall()
            if not rows:
                print("No changes in latest run.")
            for post_id, userId, change_type, old_title, new_title, changed_at in rows:
                print(
                    f"Post {post_id} | User {userId} | {change_type} | "
                    f"old={old_title!r} | new={new_title!r} | {changed_at}"
                )
        except Exception as e:
            print(f"[DB ERROR] Change-log query failed: {e}")

        try:
            cursor.execute("""
                SELECT userId, COUNT(log_id) AS changes
                FROM change_log
                GROUP BY userId
                ORDER BY changes DESC, userId
                LIMIT 1
            """)

            print("\n--- Top User by Changes ---")
            result = cursor.fetchone()
            if result:
                print(f"User {result[0]}: {result[1]} changes")
            else:
                print("No change events found.")
        except Exception as e:
            print(f"[DB ERROR] Top-user query failed: {e}")

    except Exception as e:
        print(f"[ERROR] {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    if create_database_table():
        latest_run_id = sync()
        if latest_run_id:
            run_analysis(latest_run_id)
