# example curl request from the https://open-meteo.com/ homepage

# $ curl "https://api.open-meteo.com/v1/forecast?
# latitude=52.52&longitude=13.41&current=temperature
# _2m,wind_speed_1em&hour ly=temperature_2m, relative_
# humidity_2m,wind_speed_16m

from operator import indexOf

import requests
import json
import csv

request_url="https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": "27.6748", # bhaktapur's lat lng
    "longitude": "85.4274",
    "daily": "temperature_2m_max"
}

try:
    res = requests.get(request_url, params=params)
    if res.status_code == 200:
        print("successfully fetched the data")
        print(json.dumps( res.json(), indent=2))
    
    weather_data = res.json()
    days = weather_data["daily"]["time"]
    temperatures = weather_data["daily"]["temperature_2m_max"]
    maximum_temp = max(temperatures)
    max_temp_indices = [i for i, temp in enumerate(temperatures) if temp == maximum_temp] 
    max_temp_dates = [date for i, date in enumerate(days) if i in max_temp_indices]
    # print(temperatures, type(temperatures))
    print(maximum_temp, max_temp_dates)

    minimum_temp = min(temperatures)
    min_temp_indices = [i for i, temp in enumerate(temperatures) if temp == minimum_temp] 
    min_temp_dates = [date for i, date in enumerate(days) if i in min_temp_indices]
    # print(temperatures, type(temperatures))
    print(minimum_temp, min_temp_dates)

    with open("weather.csv", mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["time", "temperature_2m_max"])
        writer.writeheader()
        for day, temp in zip(days, temperatures):
            writer.writerow({"time": day, "temperature_2m_max": temp})
    
    with open("temperature_summary.txt", "w", newline="") as file:
        file.write(f"location: {weather_data["latitude"]}, {weather_data["longitude"]}\r")
        file.write(f"\rThe maximum temperature of {maximum_temp} degree celcius is predicted to be reached \rwithin next 7 days on ")
        file.write(f" {max_temp_dates}\r")
        
        file.write("\rpeak temperature summary for next 7 days:\r")
        for day, temp in zip(days, temperatures):
            file.write(f"{day} -> {temp}\r")

except Exception as e:
    print("exception occured: ", e)