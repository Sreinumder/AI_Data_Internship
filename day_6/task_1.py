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
    [" alice ", "85", "90", "88", "92"],
    ["BOB", "92", "78", "85", "40"],
    ["charlie", "48", "95", "50", "42"],
    ["DAVID", "-5", "75", "82", "88"],
    ["Emma", "95", None, "89", "91"],
    [None, "77", "81", "84", "45"],
    ["frank", "65", "80", "120", "50"],
    [" Grace ", "12", "92", "85", "90"],
    ["isabella", "85", "90", "88", "92"],
    ["JACK", "100", "78", "85", "44"],
    ["Henry", "90", "85", "88", "95"],
    [" kate ", "22", "80", "78", "41"],
    ["LEO", "44", "98", "49", "51"],
    ["mia", "85", "-10", "90", "88"],
    ["Nathan", "91", "88", "93", "90"],
    ["oliver", "8", "92", "60", "43"],
    ["PENELOPE ", "81", "85", "88", "83"],
    ["QUINN", "72", "75", "70", "46"],
    ["  ryan", "88", "91", "85", "89"],
    ["Sophia", "94", "93", "999", "96"],
    ["thomas", "45", "90", "48", "52"],
    ["Uma", "89", "84", "91", None],
    ["Victor ", "62", "95", "61", "40"],
    ["WILLIAM", "72", "75", "70", "85"],
    ["Xavier", "-15", "88", "84", "86"],
    ["YARA", "95", "92", "94", "91"],
    ["  zach  ", "78", "82", "80", "49"],
    ["Amelia", None, "96", "72", "94"],
    ["DANIEL", "83", "85", "82", "87"],
    ["elena", "55", "78", "54", "56"],
    ["Finley", "0", "84", "91", None],
    [None, "68", "92", "70", "74"],
    ["Grayson", "92", "95", "135", "93"],
    ["  Hannah", "92", "78", "85", "41"],
    ["Ian", "48", "95", "50", "44"],
    ["Julia", "74", "-8", "77", "75"],
    ["Kevin", "31", "83", "80", "82"],
    ["Lily", "67", "94", "66", "48"],
    ["milo", "90", "88", "92", "89"],
    ["Nora", "79", "81", "78", "40"],
    ["Owen ", "5", "87", "84", "86"],
    ["Paige", "44", "92", "49", "51"],
    ["Quentin", "93", None, "95", "92"],
    ["Rachel", "105", "85", "88", "90"],
    ["Samuel", "76", "78", "75", "47"],
    ["Tristan", "18", "84", "86", "85"],
    ["Ursula", "81", "95", "88", "83"],
    ["Valerie", "59", "91", "58", "40"],
    ["Wyatt", "92", "94", "91", "95"],
    ["Zane", "45", "90", "48", "52"],
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