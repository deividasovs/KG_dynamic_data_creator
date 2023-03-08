import json
import datetime as dt
import asyncio

from common.consts import DATE_FORMAT

from data_manager import DataManager
from data_fetchers.fetch_oil import fetch_oil_data
from data_fetchers.fetch_weather import fetch_historical_weather, fetch_forecasts
from data_fetchers.fetch_hourly_sales import fetch_hourly_sales


def lambda_handler(event, context):
    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }


def create_date():
    start_date = (dt.datetime.today() -
                  dt.timedelta(days=60)).replace(hour=0, minute=0, second=0)

    start_date = start_date.strftime(
        DATE_FORMAT)

    end_date = (dt.datetime.today() - dt.timedelta(days=1)
                ).replace(hour=0, minute=0, second=0)

    end_date = end_date.strftime(DATE_FORMAT)

    return start_date, end_date


async def main():
    start_date, end_date = create_date()
    """
    salesdf = fetch_hourly_sales(start_date, end_date)

    DataManager.add_to_dataset('timestamp', salesdf['timestamp'])
    DataManager.add_to_dataset('subtotal', salesdf['subtotal'])
    DataManager.add_to_dataset(
        'transaction_count', salesdf['transaction_count'])

    DataManager.reset_index()

    oil_df = fetch_oil_data(start_date, end_date)

    # oil_df.to_csv('OIL.csv', index=False)

    DataManager.add_to_dataset('oil_price', oil_df['oil_price'])

    weather_history_df = await fetch_historical_weather(start_date, end_date)

    DataManager.add_to_dataset('rain', weather_history_df['rain'])
    DataManager.add_to_dataset(
        'temperature', weather_history_df['temperature'])
    """

    weather_forecast_df = await fetch_forecasts()

    weather_forecast_df['timestamp'] = weather_forecast_df['timestamp'].dt.tz_localize(
        None)

    DataManager.extend_column(
        ['timestamp', 'rain', 'temperature'], weather_forecast_df)

    # export weather history to csv
    # weather_history_df.to_csv('weather_history.csv', index=False

    # export data
    DataManager.export_to_csv()
    # DataManager.print_dataset()

    lambda_handler(None, None)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


# Sample data format after squashing:
# Timestamp	subtotal	transaction_count	rain	temperature	holiday	oil_price	workforce_type_1	workforce_type_2	workforce_type_3	workforce_type_4
# 17/05/2021 10:00	428.03	11	0	12.7	FALSE	69.62	1	2	3	0


# Minimum encoder value = max_encoder_length = 9*60 //2 (~ 1 month / so do 31 days)

# max_prediction_length = 9*7  # How many datapoints will be predicted (~1 week)
# max_encoder_length = 9*60  # Determines the look back period (~2 months)
