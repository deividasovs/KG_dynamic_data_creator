import datetime as dt
import asyncio

from common.consts import DATE_FORMAT
from common.helper_fns import create_date

from data_manager import DataManager
from data_fetchers.fetch_oil import fetch_oil_data
from data_adders.add_holidays import add_holidays
from data_adders.add_staff import add_staff
from data_fetchers.fetch_weather import fetch_historical_weather, fetch_forecasts
from data_fetchers.fetch_hourly_sales import fetch_hourly_sales


# Minimum encoder value = max_encoder_length = 9*60 //2 (~ 1 month / so do 31 days)

# max_prediction_length = 9*7  # How many datapoints will be predicted (~1 week)
# max_encoder_length = 9*60  # Determines the look back period (~2 months)

def lambda_handler(event, context):

    loop = asyncio.get_event_loop()
    new_dataset = loop.run_until_complete(create_dataset())

    # convert new_dataset to json
    new_dataset_json = new_dataset.to_json()

    return {
        "statusCode": 200,
        "body": new_dataset_json,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token, X-Amz-User-Agent',
            'Content-Type': 'application/json',
        }
    }


async def create_dataset():
    start_date, end_date = create_date()

    weather_history_df = await fetch_historical_weather(start_date, end_date)

    oil_df = fetch_oil_data(start_date, end_date)

    salesdf = fetch_hourly_sales(start_date, end_date)

    weather_forecast_df = await fetch_forecasts()

    DataManager.add_to_dataset('Timestamp', weather_history_df['Timestamp'])
    DataManager.add_to_dataset('rain', weather_history_df['rain'])
    DataManager.add_to_dataset(
        'temperature', weather_history_df['temperature'])

    DataManager.add_to_dataset('oil_price', oil_df['oil_price'])
    DataManager.add_to_dataset('subtotal', salesdf['subtotal'])
    DataManager.add_to_dataset(
        'transaction_count', salesdf['transaction_count'])

    staff_data = add_staff()

    DataManager.add_to_dataset(
        'workforce_type_1', staff_data['workforce_type_1'])
    DataManager.add_to_dataset(
        'workforce_type_2', staff_data['workforce_type_2'])
    DataManager.add_to_dataset(
        'workforce_type_3', staff_data['workforce_type_3'])
    DataManager.add_to_dataset(
        'workforce_type_4', staff_data['workforce_type_4'])

    weather_forecast_df['Timestamp'] = weather_forecast_df['Timestamp'].dt.tz_localize(
        None)
    DataManager.extend_column(
        ['Timestamp', 'rain', 'temperature'], weather_forecast_df)

    hols = add_holidays()

    # todo: Add time_idx
    DataManager.add_to_dataset("holiday", hols)

    DataManager.fill_na()

    # DataManager.export_to_csv()
    # DataManager.print_dataset()

    return DataManager.get_dataset()


if __name__ == "__main__":
    res = lambda_handler(None, None)
    print(res)
