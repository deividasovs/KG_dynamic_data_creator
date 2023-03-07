# import meteireann
from metEireann import weatherData
import asyncio
import datetime
import pytz

import pandas as pd

weather_data = weatherData.WeatherData()


async def fetch_historical_data():
    """Fetch data from API - (current weather and forecast)."""
    await weather_data.fetch_data()

    date = datetime.datetime.now(pytz.utc).replace(
        minute=0, second=0, microsecond=0)

    current_weather_data = weather_data.get_weather(date, max_hour=12)

    print('current:', current_weather_data)


async def fetch_forecasts(days_to_forecast=7):
    await weather_data.fetch_data()
    today = datetime.datetime.now(pytz.utc).replace(
        hour=10, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    # Add 1 hour to times until hitting 5 o clock, then incremenet the day
    times = []
    for i in range(0, days_to_forecast):
        for h in range(10, 18):
            times.append(today.replace(
                hour=h) + datetime.timedelta(days=i))

    extract_relevant_weather(
        [weather_data.get_weather(time, hourly=True) for time in times])


def extract_relevant_weather(weatherJson):
    df = pd.DataFrame(columns=["timestamp", "temperature", "rain"])

    for i in range(0, len(weatherJson)):
        weather = weatherJson[i]
        if weather is not None:
            # print(weather)
            df = df.append({
                "timestamp": weather["datetime"],
                "temperature": weather["temperature"],
                "rain": weather["precipitation"]
            }, ignore_index=True)

    print(df)


async def main():
    await fetch_historical_data()
    await fetch_forecasts()
    await weather_data.close_session()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
