import csv
from datetime import datetime, timedelta
import pycountry

filename="articles.csv"
with open(filename, "r") as f:
    articles = csv.DictReader(f)
    rows = list(articles)

    print("\n# 1 Which country out of Nepal, India, USA, UK and Australia published the most headlines today?")
    print("  ---------------------------------------------------------------------------------------------- ")

    articles_by_country = {}
    for row in rows:
        if row["source_country"] not in articles_by_country:
            articles_by_country[row["source_country"]] = [row]
        else:
            articles_by_country[row["source_country"]].append(row)
    articles_by_country_count = [ [ k,len(v) ] for k, v in articles_by_country.items() ]
    among_country_codes = {
        "np": "Nepal",
        "in": "India",
        "us": "USA",
        "uk": "UK",
        "au": "Australia"
    }
    filtered_articles_by_country = list(filter(lambda c: c[0] in among_country_codes.keys(), articles_by_country_count))
    sorted_articles_by_country = sorted(filtered_articles_by_country, key=lambda x: x[1], reverse=True)

    # print(pycountry.countries.get())j
    print(f"{among_country_codes[sorted_articles_by_country[0][0]]} has most published headlines today of {sorted_articles_by_country[0][1]}")
    # for c in sorted_articles_by_country:
    #     print(f"{among_country_codes[c[0]]} published {c[1]} headlines today.")

    print("\n# 2 What is the average number of words in a headline title — per country?")
    print("  ------------------------------------------------------------------------")
    
    average_words_in_title_by_country = []
    for country, data in articles_by_country.items():
        total = 0
        more_than_6_hours_count = len(data)
        for article in data:
            total += len(article["title"].split())
        average_words_in_title_by_country.append([country, total/more_than_6_hours_count])
        print(f"{pycountry.countries.get(alpha_2=country).name} ({country}) has an average of {total/more_than_6_hours_count} words in their headline title.")

    # print(average_words_in_title_by_country)
    
    print("\n# 3  Are there any headlines that appeared in more than one country? If yes, which ones?")
    print("  -------------------------------------------------------------------------------------- ")
    
    titles_country= dict() # title is key and country-list is value
    # found= dict() # title is key and country-list is value
    for data in rows:
        title = data["title"]
        country = data["source_country"]
        if title not in titles_country:
            titles_country[title] = [country]
        elif country not in titles_country[title]:
            titles_country.setdefault(title, []).append(country)

    found = [ [k, v] for k, v in titles_country.items() if len(v) > 1 ]
    for f in found:
        print(">>", f[0])
        print(f"   Appeared in {len(f[1])} countries: {', '.join([pycountry.countries.get(alpha_2=c).name for c in f[1]])}")
    
    
    print("\n# 4  Which news source published the most headlines across all 5 countries combined?")
    print("  ----------------------------------------------------------------------------------")
    source_country_count = dict() # source and count of headlines in each country.
    for data in rows:
        source = data["source_name"]
        country = data["source_country"]
        if country not in among_country_codes.keys():
            continue
        if source not in source_country_count:
            source_country_count[source] = {}
        if country not in source_country_count[source]:
            source_country_count[source][country] = 1
        else:
            source_country_count[source][country] += 1
    source_count_among_countries = [[k, sum(v.values())] for k, v in source_country_count.items()]
    source_count_among_countries = sorted(source_count_among_countries, key=lambda s: s[1], reverse=True)
    # print(source_count_among_countries)
    # for s in source_count_among_countries:
    s = source_count_among_countries[0]
    print(f"<<{s[0]}>> source has the most headlines across 5 countries of {s[1]} headlines. ")
    


    print("\n# 5 What percentage of all headlines were published in the last 6 hours vs older than 6 hours?")
    print("  -------------------------------------------------------------------------------------------")
    
    now = datetime.now()
    totalDataCount=len(rows)
    more_than_6_hours_count=0
    for row in rows:
        date_obj = datetime.strptime(row["published_at"], "%Y-%m-%dT%H:%M:%SZ")
        time_difference = abs(date_obj - now)
        if time_difference > timedelta(hours=6):
            more_than_6_hours_count+=1
    more_than_6_hours_percentage = more_than_6_hours_count/totalDataCount * 100
    print(f"{100-more_than_6_hours_percentage}% of task was published in last 6 hours and")
    print(f"{more_than_6_hours_percentage}% of task are older than 6 hours")
    
    print("\n# 6  If you run your script twice, does your database end up with duplicate rows? How did you prevent that?")
    print("  ---------------------------------------------------------------------------------------------------------")
    print("no, before adding new entries into the csv files.")
    print("I collected the entire [ id ] column and")
    print("checked if there are any data entry that has an id that already exist in the csv")
    
    print("\n# 7  Save only headlines with a title longer than 6 words to a CSV. How many passed that filter?" )
    print("  ----------------------------------------------------------------------------------------------" )
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
    gt6_filename="more_than_6_words.csv"
    headlines_gt_6 = 0
    with open(gt6_filename, mode="w", newline="") as ff:
        writer = csv.DictWriter(ff, fieldnames=news_csv_header)
        writer.writeheader()
        for row in rows:
            if len(row["title"].split()) > 6:
                headlines_gt_6 += 1
                writer.writerow(row)
        print(f"there are {headlines_gt_6} numbers of articles that have words_count greater than 6 and saved into {gt6_filename}")