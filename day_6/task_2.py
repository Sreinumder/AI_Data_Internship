# Task 02 · API Clean Save [Hard]→ →
# Full ETL pipeline — real API data, Pandas clean, saved to CSV and SQLite
# Goal Build a complete ETL pipeline from an API, clean the data with Pandas, and load to both CSV and SQLite.
# 1 EXTRACT: Fetch all 100 posts from https://jsonplaceholder.typicode.com/posts
# 2 Load into a Pandas DataFrame using pd.DataFrame(data)
# 3 TRANSFORM: Keep only columns userId, id, title, body
# 4 Add a word_count column — count words in the title using .str.split().str.len()
# 5 Filter: keep only posts where word_count >= 4
# 6 Standardise: title to title case, strip whitespace from body
# 7 LOAD: Save cleaned data to clean_posts.csv (no index) and to SQLite posts.db using df.to_sql()
# 8 Print: total posts fetched, posts after filter, and top 3 users by post count

import pandas as pd
from urllib.request import urlopen
import sqlite3
import json

try:
    print("# 1 Fetch 100 posts from jsonplaceholder\n========================================")
    with urlopen("https://jsonplaceholder.typicode.com/posts", timeout=10) as res:
        if (res.status == 200):
            print("successfully fetched data!")
            rawPostsData = json.loads(res.read().decode("utf-8"))
            # for post in postsData:
            #     print(post)
            print(type(rawPostsData))
            
            print("# 2 load it\n===========")
            df = pd.DataFrame(rawPostsData) 
            print(df.head(1))
            
            print("# 3 only keep those columns\n===========================")
            df=df[["userId", "id", "title", "body"]]
            print(df.head())

            print("# 4 add word count column\n=========================")
            df["word_count"] = df["title"].str.split().str.len()
            print(df.head())
            
            print("# 5 filter\n==========")
            df = df[df["word_count"] >= 4]
            print(df.head())
            
            print("# 6 standardise\n===============")
            df["title"] = df["title"].str.title()
            df["body"] = df["body"].str.strip()
            print(df.head())
            
            print("# 7 save to csv and sql\n=======================")
            df.to_csv("clean_posts.csv", index=False)
            df.to_sql("posts", con=sqlite3.connect("posts.db"), if_exists="replace")
            
            print("# 8 print\n===========")
            print("total posts fetched:", len(rawPostsData))
            print("posts after filter:", len(df))
            print("top 3 users by post count:")
            print(df.groupby("userId").count().sort_values("title", ascending=False).head(3))
        else:
            print("failed to fetch data. Status code is", res.status)
except Exception as e:
    print(e)
    