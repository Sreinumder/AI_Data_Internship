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

# 1 load csv
df = pd.read_csv("clean_students.csv")

def assign_grade(score):
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 50:
        return "C"
    elif score >= 40:
        return "D"
    else:
        return "F"

# 2 apply function on dataframe to create column
# this is already done in first task tho
# df["grade"] = df["score"].apply(assign_grade)

# 3 apply passed or failed boolean column
df["passed"] = df["score"] >= 50

# 4 score_category column based on values range
df["score_category"] = pd.cut(df["score"], bins=[0, 49, 79, 100], labels=["Low", "Medium", "High"])

# 5 rank column based on order of score
df["rank"] = df["score"].rank(ascending=False)

# 6 group by grade and calcaulate stats
print(df.groupby("grade").agg({"score": ["count", "mean", "min", "max"]}))

# 7 sort by rank and reset the index to be in order of rank
df = df.sort_values("rank", ascending=False).reset_index(drop=True)

# 8 save it to enriched csv
df.to_csv("enriched_students.csv", index=False)

print(df.head())