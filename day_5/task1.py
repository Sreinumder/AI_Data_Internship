# Task A · Multi-Table Relational System [Hard]
# Using mysql-connector-python, create a database called store_db with three tables
# — customers, products, and orders — where orders links to the other two via foreign keys.
# Insert at least 10 customers, 8 products, and 20 orders using %s parameterized queries.
# Then run these 4 queries and print the results with clear labels:
# total money spent per customer (JOIN + price × quantity, sorted highest first),
# most ordered product by total quantity, customers who placed more than 2 orders (HAVING COUNT),
# and average order value per city.
# Finally, export the revenue-per-customer result into a revenue_report.csv using Python's csv module.
# Deliverable: MySQL store_db + Python script + revenue_report.csv

import csv
import os
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
REPORT_PATH = Path(__file__).with_name("revenue_report.csv")

load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env", override=True)


def get_db_config():
    missing_vars = [
        var_name
        for var_name in ("DB_HOST", "DB_USER", "DB_PASSWORD")
        if os.getenv(var_name) is None
    ]

    if missing_vars:
        missing_list = ", ".join(missing_vars)
        raise RuntimeError(
            f"Missing database setting(s): {missing_list}. "
            "Add them to your .env file before running this script."
        )

    return {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def create_tables(cursor):
    """Create store_db tables with foreign-key relationships."""
    cursor.execute("CREATE DATABASE IF NOT EXISTS store_db")
    cursor.execute("USE store_db")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            city VARCHAR(100) NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE,
            category VARCHAR(100) NOT NULL,
            price DECIMAL(10, 2) NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            order_date DATE NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)


def seed_data(cursor):
    """Insert sample rows using %s parameterized queries."""
    customers = [
        (1, "Aarav Sharma", "aarav@example.com", "Kathmandu"),
        (2, "Nisha Tamang", "nisha@example.com", "Pokhara"),
        (3, "Rohan Gurung", "rohan@example.com", "Lalitpur"),
        (4, "Sita Thapa", "sita@example.com", "Biratnagar"),
        (5, "Maya Rai", "maya@example.com", "Kathmandu"),
        (6, "Kiran Shrestha", "kiran@example.com", "Pokhara"),
        (7, "Prakash Lama", "prakash@example.com", "Bhaktapur"),
        (8, "Anita Karki", "anita@example.com", "Lalitpur"),
        (9, "Deepak Bista", "deepak@example.com", "Butwal"),
        (10, "Samjhana KC", "samjhana@example.com", "Kathmandu"),
    ]

    products = [
        (1, "Laptop", "Electronics", 85000.00),
        (2, "Wireless Mouse", "Electronics", 1500.00),
        (3, "Office Chair", "Furniture", 12000.00),
        (4, "Notebook", "Stationery", 250.00),
        (5, "Desk Lamp", "Furniture", 3200.00),
        (6, "Backpack", "Accessories", 4500.00),
        (7, "Water Bottle", "Accessories", 900.00),
        (8, "Keyboard", "Electronics", 2800.00),
    ]

    orders = [
        (1, 1, 1, 1, "2026-04-01"),
        (2, 1, 2, 2, "2026-04-03"),
        (3, 1, 8, 1, "2026-04-07"),
        (4, 2, 3, 1, "2026-04-02"),
        (5, 2, 4, 10, "2026-04-05"),
        (6, 3, 6, 2, "2026-04-04"),
        (7, 3, 7, 4, "2026-04-09"),
        (8, 4, 1, 1, "2026-04-08"),
        (9, 4, 5, 2, "2026-04-10"),
        (10, 5, 2, 3, "2026-04-11"),
        (11, 5, 4, 12, "2026-04-12"),
        (12, 5, 7, 5, "2026-04-14"),
        (13, 6, 8, 2, "2026-04-13"),
        (14, 7, 3, 1, "2026-04-15"),
        (15, 7, 6, 1, "2026-04-16"),
        (16, 8, 4, 20, "2026-04-17"),
        (17, 8, 5, 1, "2026-04-18"),
        (18, 9, 7, 8, "2026-04-19"),
        (19, 10, 2, 1, "2026-04-20"),
        (20, 10, 8, 1, "2026-04-21"),
        (21, 10, 4, 5, "2026-04-22"),
    ]

    cursor.executemany("""
        INSERT INTO customers (customer_id, name, email, city)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            email = VALUES(email),
            city = VALUES(city)
    """, customers)

    cursor.executemany("""
        INSERT INTO products (product_id, name, category, price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            category = VALUES(category),
            price = VALUES(price)
    """, products)

    cursor.executemany("""
        INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            customer_id = VALUES(customer_id),
            product_id = VALUES(product_id),
            quantity = VALUES(quantity),
            order_date = VALUES(order_date)
    """, orders)


def print_rows(label, rows, formatter):
    print(f"\n{label}")
    print("-" * len(label))
    for row in rows:
        print(formatter(row))


def export_revenue_report(revenue_rows):
    with REPORT_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["customer_id", "customer_name", "city", "total_spent"])
        writer.writerows(revenue_rows)


def run_store_database():
    with mysql.connector.connect(**get_db_config()) as db_conn:
        with db_conn.cursor() as cursor:
            create_tables(cursor)
            seed_data(cursor)
            db_conn.commit()

            cursor.execute("""
                SELECT
                    c.customer_id,
                    c.name,
                    c.city,
                    SUM(p.price * o.quantity) AS total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN products p ON o.product_id = p.product_id
                GROUP BY c.customer_id, c.name, c.city
                ORDER BY total_spent DESC
            """)
            revenue_per_customer = cursor.fetchall()
            print_rows(
                "Total money spent per customer",
                revenue_per_customer,
                lambda row: f"{row[1]} ({row[2]}) - Rs. {row[3]:,.2f}",
            )

            cursor.execute("""
                SELECT
                    p.name,
                    SUM(o.quantity) AS total_quantity
                FROM products p
                JOIN orders o ON p.product_id = o.product_id
                GROUP BY p.product_id, p.name
                ORDER BY total_quantity DESC
                LIMIT 1
            """)
            most_ordered_product = cursor.fetchall()
            print_rows(
                "Most ordered product by total quantity",
                most_ordered_product,
                lambda row: f"{row[0]} - {row[1]} units",
            )

            cursor.execute("""
                SELECT
                    c.name,
                    c.city,
                    COUNT(o.order_id) AS order_count
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name, c.city
                HAVING COUNT(o.order_id) > 2
                ORDER BY order_count DESC
            """)
            frequent_customers = cursor.fetchall()
            print_rows(
                "Customers who placed more than 2 orders",
                frequent_customers,
                lambda row: f"{row[0]} ({row[1]}) - {row[2]} orders",
            )

            cursor.execute("""
                SELECT
                    c.city,
                    AVG(order_totals.order_value) AS average_order_value
                FROM customers c
                JOIN (
                    SELECT
                        o.order_id,
                        o.customer_id,
                        SUM(p.price * o.quantity) AS order_value
                    FROM orders o
                    JOIN products p ON o.product_id = p.product_id
                    GROUP BY o.order_id, o.customer_id
                ) AS order_totals ON c.customer_id = order_totals.customer_id
                GROUP BY c.city
                ORDER BY average_order_value DESC
            """)
            average_order_value_per_city = cursor.fetchall()
            print_rows(
                "Average order value per city",
                average_order_value_per_city,
                lambda row: f"{row[0]} - Rs. {row[1]:,.2f}",
            )

            export_revenue_report(revenue_per_customer)
            print(f"\nRevenue report exported to: {REPORT_PATH}")


if __name__ == "__main__":
    run_store_database()
