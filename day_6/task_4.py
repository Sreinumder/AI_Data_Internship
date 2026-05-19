# Task 04 · Transform & Enrich [Hard]
# Focus purely on the Transform step — advanced column engineering with Pandas
# Goal Take your clean_students.csv from Task 01 and enrich it with multiple new calculated columns.
# 1 Load clean_students.csv from Task 01 into a DataFrame
# 2 Add grade column: A ( 90), B ( 75), C ( 60), D ( 50), F (<50) — use apply() with a function≥ ≥ ≥ ≥
# 3 Add passed column: True if score >= 50, False otherwise
# 4 Add score_category column: 'High' ( 80), 'Medium' (50–79), 'Low' (<50)≥
# 5 Add rank column: rank all students by score — highest score gets rank 1 (use .rank(ascending=False))
# 6 Group by grade — print count, mean score, and min/max score per grade using groupby()
# 7 Sort the final DataFrame by rank and reset the index
# 8 Save enriched data to enriched_students.csv and print the top 5 ranked students

import pandas as pd

# 1. LOAD CSV
df = pd.read_csv("clean_students.csv")

# Identify the score columns dynamically
score_cols = ["math_score", "english_score", "science_score", "social_study_score"]

# First, let's create a core 'overall_score' to handle your global ranking and categorization
df["overall_score"] = df[score_cols].mean(axis=1).round(2)


# 2. ADD GRADE COLUMN (Using your updated tier logic: A>=90, B>=75, C>=60, D>=50, F<50)
def assign_grade(score):
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    elif score >= 50:
        return "D"
    else:
        return "F"


# Generate grades for each subject AND the overall score
for col in score_cols:
    grade_col = col.replace("_score", "_grade")
    df[grade_col] = df[col].apply(assign_grade)

df["overall_grade"] = df["overall_score"].apply(assign_grade)


# 3. ADD PASSED COLUMN (True if overall score >= 50)
df["passed"] = df["overall_score"] >= 50


# 4. ADD SCORE_CATEGORY COLUMN ('High' >= 80, 'Medium' 50-79, 'Low' < 50)
# Note: right=True is default, so we shift our boundaries slightly or use include_lowest
df["score_category"] = pd.cut(
    df["overall_score"],
    bins=[-1, 49.99, 79.99, 100],
    labels=["Low", "Medium", "High"],
)


# 5. ADD RANK COLUMN (Highest overall score gets rank 1)
# 'min' method ensures that ties share the upper rank (e.g., two people tied for 1st both get 1.0)
df["rank"] = df["overall_score"].rank(ascending=False, method="min").astype(int)


# 6. GROUP BY GRADE AND CALCULATE STATS
# Grouping by the new 'overall_grade' to check class distribution and performance bounds
print("=" * 60)
# Format the headers nicely using a joined multi-index display
grouped_stats = df.groupby("overall_grade").agg(
    {"overall_score": ["count", "mean", "min", "max"]}
)
print("GROUPBY STATS (BY OVERALL GRADE):")
print(grouped_stats)
print("=" * 60 + "\n")


# 7. SORT BY RANK AND RESET INDEX
# To have the top students at the top of the file, we sort ascending=True for rank (1, 2, 3...)
df = df.sort_values("rank", ascending=True).reset_index(drop=True)


# 8. SAVE ENRICHED DATA AND PRINT TOP 5
df.to_csv("enriched_students.csv", index=False)

# Selecting a clean preview subset of columns so it prints readable text in your console
preview_cols = ["rank", "name", "overall_score", "overall_grade", "passed", "score_category"]
print("TOP 5 RANKED STUDENTS:")
print(df[preview_cols].head(5))

print("\nenriched_students.csv saved successfully!")