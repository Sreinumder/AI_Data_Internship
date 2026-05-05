import csv
import json
from pathlib import Path
from urllib.request import urlopen

request_url="https://jsonplaceholder.typicode.com"
endpoint="/posts"
BASE_DIR = Path(__file__).resolve().parent
posts_csv = BASE_DIR / "posts.csv"
filtered_posts_csv = BASE_DIR / "postsWithMoreThan5Words.csv"

try:
    with urlopen(request_url+endpoint, timeout=10) as res:
        posts_data = json.loads(res.read().decode("utf-8"))
    # print(type(posts_data))
    # print(posts_data)
    # print(posts_data[0])

except:
    print("failed to make the request")
    exit

with posts_csv.open("w", newline="") as file:
    header = ["id", "title", "body"]
    writer = csv.DictWriter(file, fieldnames=header, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(posts_data)
    
with posts_csv.open("r", newline="") as file:
    reader = csv.DictReader(file)
    # for row in reader:
    #     print(row)
    MoreThanFive = filter(lambda row: len(row["title"].split()) > 5, reader)
    # print(MoreThanFive.__sizeof__(), reader.__sizeof__())
    # for row in MoreThanFive:
    #     print(row)
    with filtered_posts_csv.open("w", newline="") as fileWrite:
        header = ["id", "title", "body"]
        writer = csv.DictWriter(fileWrite, fieldnames=header)
        writer.writeheader()
        writer.writerows(MoreThanFive)
