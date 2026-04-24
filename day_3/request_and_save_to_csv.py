from dataclasses import field
from functools import reduce
import os
import csv

import requests
import dotenv

# api documentations at https://docs.gnews.io/ api request limit: 100
# layout: https://gnews.io/api/v4/{endpoint}?{parameters}&apikey=YOUR_API_KEY
# query parameters documentation: https://docs.gnews.io/endpoints/search-endpoint
# use query parameter max: 100 and page=1, 2 ..... and so on to get maximum amount of data lets say 20 request for 2000 data points (hopefully)
# plan: 

dotenv.load_dotenv()
API_KEY = os.getenv("GNEWS_API")

url = "https://gnews.io/api/v4/search"
params = {
        "max": "100", # regardless of its value only 10 articles seems to be returned on free tier
        "page": "1",
        "apikey": API_KEY,
        "q": "None"
}

news_csv_header = [
    "id",
    "title",
    "description",
    "content",
    "image",
    "lang",
    "published_at",
    "source_id",
    "source_name",
    "source_url",
    "source_country"
]

article_pages = []
# trying to get articles 100 * 10 pages of data
# (there is rate limit of 100 request daily be careful only sit the range to (1, 2) for testing)
for page in range(1, 100+1): 
    params["page"] = page
    # print(params["page"])
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print("request for", page,  "was successful with output of", len(data["articles"]))
            # print(data)
            article_pages.append(data["articles"])
        else:
            print("there was failure on api request. The status code is ", response.status_code)
            print(response.json())
    except Exception as e:
        print(e)
        break
    
filename = "articles.csv"

# write the output of the request onto the csv and 
# we can ensure that the new news wasnt already present by matching id of new news and older ones
if not os.path.exists(filename):
    with open(filename,"w", newline="") as f:
        csvWriter = csv.DictWriter(f, fieldnames=news_csv_header)
        csvWriter.writeheader()

# read entire row of csv with all the ids
present_id = []
with open(filename, "r") as f:
    csvReader = csv.DictReader(f)
    for row in csvReader:
        present_id.append(row["id"])
print(f"{present_id} those were already present.")

duplicate_found = []
total_article = 0
with open(filename, "a", newline="") as f:
    csvWriter = csv.DictWriter(f, fieldnames=news_csv_header)
    for article_page in article_pages:
        total_article += len(article_page)
        for article in article_page:
            if article.get("id") in present_id:
                duplicate_found.append(article)
                continue
            row = {
                "id": article.get("id"),
                "title": article.get("title"),
                "description": article.get("description"),
                "content": article.get("content"),
                "image": article.get("image"),
                "lang": article.get("lang"),
                "published_at": article.get("publishedAt"),
                "source_id": article.get("source", {}).get("id"),
                "source_name": article.get("source", {}).get("name"),
                "source_url": article.get("source", {}).get("url"),
                "source_country": article.get("source", {}).get("country"),
            }
            csvWriter.writerow(row)
        
print(f"a total of {total_article} articles were fetched and {len(duplicate_found)} were found to be duplicate.")