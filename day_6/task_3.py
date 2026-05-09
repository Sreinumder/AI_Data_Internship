# Task 03 · Multi-Source ETL [Hard]
# Merge data from two sources, clean and combine, load as one unified dataset
# Goal Combine users and their posts from two API endpoints into one clean merged DataFrame.
# 1 EXTRACT: Fetch users from /users and posts from /posts — two separate API calls
# 2 Create two DataFrames: df_users and df_posts
# 3 From df_users keep: id, name, email, city (from address.city — requires pd.json_normalize!)
# 4 From df_posts keep: userId, title — rename userId to id to match users
# 5 TRANSFORM: Merge the two DataFrames on id using pd.merge(df_users, df_posts, on='id')
# 6 Count posts per user — add a post_count column to df_users using df_posts.groupby('id').size()
# 7 Clean: lowercase email, strip whitespace from name and city, drop nulls

import sqlite3

import pandas as pd
from urllib.request import urlopen
import json

try:
    with urlopen("https://jsonplaceholder.typicode.com/users", timeout=10) as res:
        if (res.status == 200):
            print("successfully fetched data!")
            usersData = json.loads(res.read().decode("utf-8"))
            df_users = pd.DataFrame(usersData)

            print("# 2 load it\n===========")
            print(df_users.head(1))
            print(type(df_users))
            print(df_users.columns)
            print(df_users["address"])

            # Flatten address column
            address_df = pd.json_normalize( df_users["address"].tolist())

            # Add flattened columns back but we only need city column and no lat lng street
            df_users = pd.concat([df_users, address_df["city"]], axis=1)
            
            # only keep id name email city
            df_users = df_users[["id","name", "email", "city"]]

            print("df_users.head(1)")
            print("================")
            print(df_users.head(1))

            # opening posts data
            with urlopen("https://jsonplaceholder.typicode.com/posts", timeout=10) as pres:
                if (pres.status == 200):
                    print("successfully fetched data!")
                    postsData = json.loads(pres.read().decode("utf-8"))
                    df_posts = pd.DataFrame(postsData)

                    print("\n\n# loaded posts\n===========")
                    print(df_posts.head(1))
                    print(type(df_posts))
                    print(df_posts.columns)
                    df_posts = df_posts[["userId", "title"]]
                    df_posts.rename(columns={"userId":"id"}, inplace=True)
                    print(df_posts.columns)

                    # 5 merge the two dataframes!
                    df_userPosts =pd.merge(df_users, df_posts, on='id')

                    # 6 count posts per user
                    # df_userPosts["post_count"] = df_userPosts.groupby("id").size()
                    df_userPosts["post_count"] = df_userPosts.groupby("id")["id"].transform("count")
                    # print(df_users)
                    print(df_userPosts.head())
                    
                    # 7 clean lowercase email strip whitespace from name and city and drop nulls
                    df_userPosts["email"] = df_userPosts["email"].str.lower()
                    df_userPosts["name"] = df_userPosts["name"].str.strip()
                    df_userPosts["city"] = df_userPosts["city"].str.strip()
                    df_userPosts.dropna(inplace=True)
                    print(df_userPosts.head())
                    

                    # 8 saving to merged_dat.csv and merged.db
                    df_userPosts.to_csv("merged_data.csv", index=False)
                    df_userPosts.to_sql("merged", con=sqlite3.connect("merged.db"), if_exists="replace")

                    # top 3 result
                    with sqlite3.connect("merged.db") as conn:
                        cur = conn.cursor()

                        cur.execute("""
                            SELECT id, name, email, city, COUNT(*) AS post_count
                            FROM merged
                            GROUP BY id
                            ORDER BY post_count DESC
                            LIMIT 3;
                        """)

                        result = cur.fetchall()
                        for r in result:
                            print(r)

                else:
                    print("failed to fetch data. Status code is", pres.status)  

        else:
            print("failed to fetch data. Status code is", res.status)
except Exception as e:
    print(e)