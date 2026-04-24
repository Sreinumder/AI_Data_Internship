import csv
from functools import reduce
import pycountry

filename="articles.csv"
with open(filename, "r") as f:
    articles = csv.DictReader(f)
    rows = list(articles)

    print("\n# 1 Which country out of Nepal, India, USA, UK and Australia published the most headlines today? \n ---------------------------------------------------------------------------------------------- ")

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

    print("\n# 2 What is the average number of words in a headline title — per country? \n ------------------------------------------------------------------------ ")
    
    average_words_in_title_by_country = []
    for country, data in articles_by_country.items():
        total = 0
        count = len(data)
        for article in data:
            total += len(article["title"].split())
        average_words_in_title_by_country.append([country, total/count])
        print(f"{pycountry.countries.get(alpha_2=country).name} ({country}) has an average of {total/count} words in their headline title.")

    # print(average_words_in_title_by_country)
    
    print("\n# 3  Are there any headlines that appeared in more than one country? If yes, which ones? \n -------------------------------------------------------------------------------------- ")
    
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
    
    
    print("\n# 4  Which news source published the most headlines across all 5 countries combined? \n ----------------------------------------------------------------------------------")
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