# Task 03 · Weather Data + Analysis [Hard]
# Store real weather data in MySQL and run meaningful analysis queries
# Goal
# Use the Open-Meteo API to fetch 7-day weather for 3 cities and store + compare them in MySQL
# 1. Fetch 7-day forecast (max + min temp) for 3 cities of your choice using Open-Meteo API
# 2. Create weather.db with a forecasts table: id, city, date, max_temp, min_temp
# 3. Insert all 21 rows (3 cities * 7 days) into the database
# 4. Query 1: Which city has the highest average max temperature?
# 5. Query 2: Find the single hottest day across all 3 cities
# 6. Query 3: Find days where the temperature difference (max - min) is greater than 10°C
# 7. Save a summary report to a summary.txt file using Python file handling (Week 1 skill!)
# Deliverable: weather.db + summary.txt + script showing all 3 query outputs
# Bonus: add humidity data as a 4th column

import os
import requests
from dotenv import load_dotenv
import mysql.connector
from collections import defaultdict

load_dotenv()


def run_weather_analysis():
    """Main function to run the weather data analysis."""
    with mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    ) as db_conn:
        with db_conn.cursor() as db_cursor:
            db_cursor.execute("CREATE DATABASE IF NOT EXISTS weather_db")
            db_cursor.execute("USE weather_db")

            db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecasts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                city VARCHAR(100),
                date DATE,
                max_temp FLOAT,
                min_temp FLOAT,
                humidity FLOAT,
                UNIQUE KEY unique_city_date (city, date)
            )
            """)

            api_params = {
                "daily": "temperature_2m_max,temperature_2m_min",
                "hourly": "relative_humidity_2m",
                "timezone": "auto",
            }

            city_coordinates = {
                "New York": {"latitude": 40.7128, "longitude": -74.0060},
                "London": {"latitude": 51.5074, "longitude": -0.1278},
                "Tokyo": {"latitude": 35.6762, "longitude": 139.6503}
            }

            for city_name, coords in city_coordinates.items():
                api_params["latitude"] = coords["latitude"]
                api_params["longitude"] = coords["longitude"]

                api_response = requests.get(
                    "https://api.open-meteo.com/v1/forecast",
                    params=api_params
                )

                if api_response.status_code == 200:
                    weather_data = api_response.json()

                    daily_data = weather_data.get("daily", {})
                    hourly_data = weather_data.get("hourly", {})

                    date_list = daily_data.get("time", [])
                    max_temperatures = daily_data.get("temperature_2m_max", [])
                    min_temperatures = daily_data.get("temperature_2m_min", [])

                    hourly_times = hourly_data.get("time", [])
                    hourly_humidity = hourly_data.get("relative_humidity_2m", [])

                    humidity_by_day = defaultdict(list)

                    for time_val, humidity_val in zip(hourly_times, hourly_humidity):
                        day = time_val.split("T")[0]
                        humidity_by_day[day].append(humidity_val)

                    daily_humidity = {
                        day: sum(values) / len(values)
                        for day, values in humidity_by_day.items()
                    }

                    for date_str, max_temp, min_temp in zip(
                        date_list, max_temperatures, min_temperatures
                    ):
                        db_cursor.execute("""
                            INSERT INTO forecasts (city, date, max_temp, min_temp, humidity)
                            VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                max_temp = VALUES(max_temp),
                                min_temp = VALUES(min_temp),
                                humidity = VALUES(humidity)
                        """, (
                            city_name,
                            date_str,
                            max_temp,
                            min_temp,
                            daily_humidity.get(date_str)
                        ))

                    db_conn.commit()
                    print(f"{city_name} data inserted successfully!")
                else:
                    print(f"Failed to fetch data for {city_name}: {api_response.status_code}")

            db_cursor.execute("""
            SELECT city, AVG(max_temp) as avg_max
            FROM forecasts
            GROUP BY city
            ORDER BY avg_max DESC
            LIMIT 1
            """)
            warmest_city = db_cursor.fetchone()

            db_cursor.execute("""
            SELECT city, date, max_temp
            FROM forecasts
            ORDER BY max_temp DESC
            LIMIT 1
            """)
            hottest_day = db_cursor.fetchone()

            db_cursor.execute("""
            SELECT city, date, max_temp, min_temp, humidity
            FROM forecasts
            WHERE (max_temp - min_temp) > 10
            """)
            high_variance_days = db_cursor.fetchall()

            with open("summary.txt", "w", encoding="utf-8") as report_file:
                report_file.write("Weather Summary Report\n")
                report_file.write("=====================\n\n")

                if warmest_city:
                    report_file.write(
                        f"Hottest city on average: {warmest_city[0]} "
                        f"with avg max temp {warmest_city[1]:.2f}°C\n\n"
                    )

                if hottest_day:
                    report_file.write(
                        f"Hottest day: {hottest_day[0]} on {hottest_day[1]} "
                        f"with {hottest_day[2]:.2f}°C\n\n"
                    )

                report_file.write("Days with temperature difference > 10°C:\n")

                for city, date, max_t, min_t, hum in high_variance_days:
                    temp_diff = max_t - min_t
                    report_file.write(
                        f"{city} on {date}: Max {max_t:.2f}°C, Min {min_t:.2f}°C, "
                        f"Humidity {hum:.2f}% | Diff {temp_diff:.2f}°C\n"
                    )

            print("summary.txt created successfully!")


if __name__ == "__main__":
    run_weather_analysis()