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

import pandas as pd
import numpy as np

# 1. CREATE A MESSY CSV MANUALLY

messy_data = [
    [" alice ", "85"],        # preceeding Whitespace
    ["BOB", "92"],            # uppercase name
    ["charlie", "48"],
    ["DAVID", "-5"],          # invalid score
    ["Emma", None],           # missing score
    [None, "77"],             # missing name
    ["frank", "65"],
    [" Grace ", "88"],
    ["alice", "85"],          # duplicate-like row
    ["BOB", "92"],            # exact duplicate
    ["Henry", "105"],         # unrealistic but valid unless filtered
    [" isabella ", "73"],
    ["JACK", "44"],
    ["kate", "-10"],          # invalid score
    ["Leo", "91"],
    ["mia", "58"],
    ["NORA ", "81"],
]

messy_df = pd.DataFrame(messy_data, columns=["name", "score"])

messy_df.to_csv("messy_students.csv", index=False)

print("messy_students.csv created!\n")


# 2. LOAD CSV + SHOW PROBLEMS

df = pd.read_csv("messy_students.csv")

print(df)
print(df.info())
print(df.isnull().sum())
before_rows = len(df)

# 3. FIX ALL 6 PROBLEMS

# Fix whitespace in names
df["name"] = df["name"].astype(str).str.strip()

# Fix inconsistent casing
df["name"] = df["name"].str.title()

# Convert score to numeric
df["score"] = pd.to_numeric(df["score"], errors="coerce")

# Handle missing values
# Remove rows with missing name or score
df = df.dropna(subset=["name", "score"])

# Remove invalid scores
# Keep only scores >= 0
# -------------------------
df = df[df["score"] >= 0]

# Remove duplicates
df = df.drop_duplicates()


# 4. ADD GRADE COLUMN

def assign_grade(score):
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 50:
        return "C"
    else:
        return "F"

df["grade"] = df["score"].apply(assign_grade)


# AFTER CLEANING ROW COUNT
after_rows = len(df)

# 5. SAVE CLEANED CSV
df.to_csv("clean_students.csv", index=False)

print("\nCLEANED DATA")
print(df)

print("\nROW COUNT")
print(f"Before cleaning: {before_rows}")
print(f"After cleaning : {after_rows}")

print("\nclean_students.csv saved successfully!")