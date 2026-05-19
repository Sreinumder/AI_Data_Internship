# Task 01 · Clean a Messy CSV [Medium]
# Independent — practice every cleaning technique on a prepared dataset
# Goal Create a messy CSV yourself, then write a cleaning script that fixes every problem.
# 1 Create messy_students.csv manually — add at least 15 rows with these problems deliberately:
#   Some rows with missing score or name Duplicate rows Names in inconsistent casing
#   Score stored as string e.g. '85' Extra spaces in names Some scores below 0 (invalid)
# 2 Load it with Pandas and print df.info() + df.isnull().sum() — show the problems first
# 3 Fix all 6 problems: nulls, duplicates, casing, type, whitespace, invalid values
# 4 Add a grade column: A ( 90), B ( 75), C ( 50), F (<50) — use df['score'].apply()
# 5 Save the cleaned result to clean_students.csv and print a before/after row count

import numpy as np
import pandas as pd

# 1. CREATE A MESSY CSV MANUALLY (Expanded with 4 subject columns)
# Problems injected: Whitespace, case inconsistency, missing values (None/NaN),
# string-formatted numbers, invalid negative/extreme scores, and duplicate rows.

messy_data = [
    # name, math, english, science, social_studies
    [" alice ", "85", "90", "88", "92"],  # Whitespace, strings
    ["BOB", "92", "78", "85", "80"],  # Uppercase name
    ["charlie", "48", "55", "50", "60"],
    ["DAVID", "-5", "75", "82", "70"],  # Invalid negative math score
    ["Emma", "95", None, "89", "91"],  # Missing English score
    [None, "77", "81", "84", "79"],  # Missing Name
    ["frank", "65", "70", "120", "68"],  # Invalid >100 science score
    [" Grace ", "88", "92", "85", "90"],  # Whitespace
    ["alice", "85", "90", "88", "92"],  # Duplicate-like row (after stripping)
    ["BOB", "92", "78", "85", "80"],  # Exact duplicate
    ["Henry", "90", "85", "88", "95"],
    [" isabella ", "73", "80", "78", "82"],
    ["JACK", "44", "52", "49", "51"],
    ["kate", "85", "-10", "90", "88"],  # Invalid negative English score
    ["Leo", "91", "88", "93", "87"],
    ["mia", "58", "62", "60", "64"],
    ["NORA ", "81", "85", "88", "83"],  # Trailing whitespace
]

columns = ["name", "math_score", "english_score", "science_score", "social_study_score"]
score_cols = ["math_score", "english_score", "science_score", "social_study_score"]

messy_df = pd.DataFrame(messy_data, columns=columns)
messy_df.to_csv("messy_students.csv", index=False)
print("messy_students.csv created successfully!\n")


# 2. LOAD CSV + SHOW PROBLEMS
df = pd.read_csv("messy_students.csv")

print("--- ORIGINAL MESSY DATA ---")
print(df)
print("\n--- DATAFRAME INFO ---")
print(df.info())
print("\n--- MISSING VALUES PER COLUMN ---")
print(df.isnull().sum())

before_rows = len(df)


# 3. FIX ALL 6 PROBLEMS

# Fix whitespace in names
df["name"] = df["name"].astype(str).str.strip()

# Fix inconsistent casing
df["name"] = df["name"].str.title()

# Convert all score columns to numeric (handles string digits and forces errors to NaN)
for col in score_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Handle missing values: Drop rows where name or ANY score is missing
df = df.dropna(subset=["name"] + score_cols)

# Remove invalid scores: Keep rows where ALL scores are between 0 and 100
valid_scores_condition = True
for col in score_cols:
    valid_scores_condition &= (df[col] >= 0) & (df[col] <= 100)

df = df[valid_scores_condition]

# Remove duplicates
df = df.drop_duplicates()


# 4. ADD GRADE COLUMNS
def assign_grade(score):
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 50:
        return "C"
    else:
        return "F"


# Automatically generate grade columns for every subject
for col in score_cols:
    grade_col_name = col.replace("_score", "_grade")
    df[grade_col_name] = df[col].apply(assign_grade)


# AFTER CLEANING ROW COUNT
after_rows = len(df)

# 5. SAVE CLEANED CSV
df.to_csv("clean_students.csv", index=False)

print("\n" + "=" * 50)
print("CLEANED DATA")
print("=" * 50)
print(df)

print("\n--- ROW COUNT SUMMARY ---")
print(f"Before cleaning: {before_rows}")
print(f"After cleaning : {after_rows}")
print(f"Dropped rows   : {before_rows - after_rows}")

print("\nclean_students.csv saved successfully!")