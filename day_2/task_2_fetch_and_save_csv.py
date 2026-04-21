import requests
import csv

request_url="https://jsonplaceholder.typicode.com"
endpoint="/posts"

try:
    res = requests.get(request_url+endpoint)
    posts_data = res.json()
    # print(type(posts_data))
    # print(posts_data)
    # print(posts_data[0])

except:
    print("failed to make the request")
    exit

with open("posts.csv", "w", newline="") as file:
    header = ["id", "title", "body"]
    writer = csv.DictWriter(file, fieldnames=header, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(posts_data)
    
with open("posts.csv", "r", newline="") as file:
    reader = csv.DictReader(file)
    # for row in reader:
    #     print(row)
    MoreThanFive = filter(lambda row: len(row["title"].split()) > 5, reader)
    # print(MoreThanFive.__sizeof__(), reader.__sizeof__())
    # for row in MoreThanFive:
    #     print(row)
    with open("postsWithMoreThan5Words.csv", "w", newline="") as fileWrite:
        header = ["id", "title", "body"]
        writer = csv.DictWriter(fileWrite, fieldnames=header)
        writer.writeheader()
        writer.writerows(MoreThanFive)