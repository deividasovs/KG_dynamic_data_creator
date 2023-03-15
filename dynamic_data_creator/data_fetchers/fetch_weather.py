import meteireann
import datetime as dt
import asyncio
import datetime
import pytz
from meteostat import Hourly, Stations

import pandas as pd

from common.helper_fns import parse_date

weather_data = meteireann.WeatherData()

# Fantastic weather API -> https://meteostat.net/en/place/ie/dublin?s=03969&t=2023-02-21/2023-02-28

# TODO: Make sure to also include todays weather!


async def fetch_historical_weather(start_date, end_date):
    # Add a day to start_date

    start_date, end_date = parse_date(start_date), parse_date(end_date)

    start_date = start_date + datetime.timedelta(days=1)
    end_date = end_date + datetime.timedelta(days=2)

    # Have to use a different API to fetch historical data, met doesn't provide it
    Stations.max_age = 0
    Hourly.max_age = 0

    stations = Stations()

    stations = stations.nearby(53.34, -6.25)
    station = stations.fetch(1)

    data = Hourly(station, start_date, end_date)
    data = data.fetch()

    return extract_relevant_weather_from_meteostat(data)


async def fetch_forecasts(days_to_forecast=7):

    await weather_data.fetching_data()
    today = datetime.datetime.now(pytz.utc).replace(
        hour=10, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    # Add 1 hour to times until hitting 5 o clock, then increment the day
    times = []
    for i in range(0, days_to_forecast):
        for h in range(10, 18):
            times.append(today.replace(
                hour=h) + datetime.timedelta(days=i))

    return extract_relevant_weather_from_met(
        [weather_data.get_weather(time, hourly=True) for time in times])


# Re-write to use the meteostat API
def extract_relevant_weather_from_met(weather_json):
    df = pd.DataFrame(columns=["Timestamp", "temperature", "rain"])

    for i, weather in enumerate(weather_json):
        if weather is not None:
            # print(weather)
            new_df = pd.DataFrame({
                "Timestamp": [weather["datetime"]],
                "temperature": [weather["temperature"]],
                "rain": [weather["precipitation"]]
            })
            df = pd.concat([df, new_df], ignore_index=True)

    # interpolate any nan temperatures, stick to 1 decimal place
    df["temperature"] = df["temperature"].interpolate().round(1)

    # print(df)

    df = df.reset_index()

    return df


def extract_relevant_weather_from_meteostat(weather_df):
    df = pd.DataFrame(columns=["Timestamp", "temperature", "rain"])

    weather_df = pd.DataFrame(weather_df)

    df["temperature"] = weather_df["temp"]
    df["rain"] = weather_df["prcp"]
    df["Timestamp"] = weather_df.index

    # reset the index
    df = df.reset_index(drop=True)

    df = df[df["Timestamp"].dt.hour.between(10, 18)]
    df = df.reset_index()

    return df


async def main():
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 10)

    historical_data = await fetch_historical_weather(start_date, end_date)
    forecasted_data = await fetch_forecasts()

    print(forecasted_data)
    print(historical_data)
    await weather_data.close_session()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
