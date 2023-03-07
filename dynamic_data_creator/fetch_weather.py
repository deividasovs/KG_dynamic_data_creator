import meteireann
import asyncio
import datetime
import pytz
from meteostat import Hourly, Stations, Point

import pandas as pd

weather_data = meteireann.WeatherData()


# Fantastic weather API -> https://meteostat.net/en/place/ie/dublin?s=03969&t=2023-02-21/2023-02-28

# TODO: Make sure to also include todays weather!

async def fetch_historical_data(startTime, endTime):
    # Have to use a diffenent API to fetch historical data, met doesnt provide it
    stations = Stations()
    stations = stations.nearby(53.34, -6.25)
    station = stations.fetch(1)

    data = Hourly(station, startTime, endTime)
    data = data.fetch()

    return extract_relevant_weather_from_meteostat(data)


async def fetch_forecasts(days_to_forecast=7):
    await weather_data.fetching_data()
    today = datetime.datetime.now(pytz.utc).replace(
        hour=10, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

    # Add 1 hour to times until hitting 5 o clock, then incremenet the day
    times = []
    for i in range(0, days_to_forecast):
        for h in range(10, 18):
            times.append(today.replace(
                hour=h) + datetime.timedelta(days=i))

    return extract_relevant_weather_from_met(
        [weather_data.get_weather(time, hourly=True) for time in times])


# Re-write to use the meteostat API
def extract_relevant_weather_from_met(weatherJson):
    df = pd.DataFrame(columns=["timestamp", "temperature", "rain"])

    for i in range(0, len(weatherJson)):
        weather = weatherJson[i]
        if weather is not None:
            print(weather)
            df = df.append({
                "timestamp": weather["datetime"],
                "temperature": weather["temperature"],
                "rain": weather["precipitation"]
            }, ignore_index=True)

    # interpolate any nan temperatures, stick to 1 decimal place
    df["temperature"] = df["temperature"].interpolate().round(1)

    return df


def extract_relevant_weather_from_meteostat(weatherDf):
    df = pd.DataFrame(columns=["timestamp", "temperature", "rain"])

    weatherDf = pd.DataFrame(weatherDf)

    df["temperature"] = weatherDf["temp"]
    df["rain"] = weatherDf["prcp"]
    df["timestamp"] = weatherDf.index

    # reset the index
    df = df.reset_index(drop=True)

    df = df[df["timestamp"].dt.hour.between(10, 18)]

    return df


async def main():
    startTime = datetime.datetime(2023, 1, 1)
    endTime = datetime.datetime(2023, 1, 10)

    historical_data = await fetch_historical_data(startTime, endTime)
    forecasted_data = await fetch_forecasts()

    print(forecasted_data)
    print(historical_data)
    await weather_data.close_session()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
