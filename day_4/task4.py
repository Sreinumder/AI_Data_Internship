# Task 04 · Update, Delete & Data Integrity [Hard]

# Databases aren't just for storing — managing and maintaining data is equally important

# Goal
# Build a student grade management system — insert, update, delete, and validate data.

# Create grades.db with a students table: id, name, subject, score, grade TEXT
# Insert 15 students with various scores (mix them between 40-100)
# Write a function assign_grade(score) that returns A/B/C/D/F based on score
# UPDATE all rows — set the grade column using your function
# DELETE all students who scored below 50 — they didn't pass
# Add a new column passed BOOLEAN using ALTER TABLE — set it based on score >= 50
# Query: show count of students per grade, ordered from A to F
# Handle the case where a student name is entered twice — check before inserting

import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def calculate_grade(score_value):
    """Calculate letter grade based on numeric score."""
    if score_value >= 90:
        return 'A'
    elif score_value >= 80:
        return 'B'
    elif score_value >= 70:
        return 'C'
    elif score_value >= 60:
        return 'D'
    else:
        return 'F'


def add_student(db_connection, db_cursor, student_name, subject_name, score_value):
    """Insert a student with duplicate name checking."""
    letter_grade = calculate_grade(score_value)

    db_cursor.execute(
        "SELECT COUNT(*) FROM students WHERE name = %s",
        (student_name,)
    )
    if db_cursor.fetchone()[0] > 0:
        print(f"Student with name '{student_name}' already exists. Skipping insertion.")
        return

    db_cursor.execute(
        "INSERT INTO students (name, subject, score, grade) VALUES (%s, %s, %s, %s)",
        (student_name, subject_name, score_value, letter_grade)
    )
    db_connection.commit()
    print(
        f"Inserted student: {student_name}, Subject: {subject_name}, "
        f"Score: {score_value}, Grade: {letter_grade}"
    )


def run_grades_system():
    """Main function to run the student grade management system."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            db_cursor.execute("CREATE DATABASE IF NOT EXISTS grades_db")
            db_cursor.execute("USE grades_db")

            db_cursor.execute("DROP TABLE IF EXISTS students")

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS students (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                subject VARCHAR(100) NOT NULL,
                score INT NOT NULL,
                grade CHAR(1)
            )
            """)

            student_records = [
                ("Alice", "Math", 95),
                ("Bob", "Science", 82),
                ("Charlie", "History", 76),
                ("David", "Math", 65),
                ("Eve", "Science", 55),
                ("Frank", "History", 45),
                ("Grace", "Math", 88),
                ("Heidi", "Science", 92),
                ("Ivan", "History", 70),
                ("Judy", "Math", 60),
                ("Karl", "Science", 50),
                ("Leo", "History", 40),
                ("Mallory", "Math", 85),
                ("Nina", "Science", 78),
                ("Oscar", "History", 68)
            ]

            for student_info in student_records:
                add_student(db_conn, db_cursor, *student_info)

            db_cursor.execute("SELECT id, score FROM students")
            student_list = db_cursor.fetchall()
            for sid, sc in student_list:
                grade_val = calculate_grade(sc)
                db_cursor.execute(
                    "UPDATE students SET grade = %s WHERE id = %s",
                    (grade_val, sid)
                )
                print(f"Updated student ID {sid} with grade {grade_val}")
            db_conn.commit()

            db_cursor.execute("ALTER TABLE students ADD COLUMN passed BOOLEAN")

            db_cursor.execute(
                "UPDATE students SET passed = (score >= 50)"
            )
            db_cursor.execute("SELECT name, passed FROM students")
            student_list = db_cursor.fetchall()
            for sname, passed_val in student_list:
                print(f"Updated student {sname} with passed status {passed_val}")
            db_conn.commit()

            db_cursor.execute("DELETE FROM students WHERE score < 50")
            db_conn.commit()

            db_cursor.execute("""
            SELECT grade, COUNT(*) as count
            FROM students
            GROUP BY grade
            ORDER BY FIELD(grade, 'A', 'B', 'C', 'D', 'F')
            """)
            grade_distribution = db_cursor.fetchall()
            for grade_val, cnt in grade_distribution:
                print(f"Grade: {grade_val}, Count: {cnt}")

            db_cursor.execute("SELECT * FROM students")
            print("\nFinal Data:")
            for row in db_cursor.fetchall():
                print(row)


if __name__ == "__main__":
    run_grades_system()