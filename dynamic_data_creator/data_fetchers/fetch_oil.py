import yfinance as yf
import datetime as dt
import pandas as pd

from common.helper_fns import parse_date
from common.consts import DATE_FORMAT


def create_dataframe(start_date, end_date):
    df = pd.DataFrame(columns=["timestamp", "oil_price"])

    date_generated = [start_date + dt.timedelta(days=x)
                      for x in range(0, (end_date-start_date).days)]

    for date in date_generated:
        for h in range(10, 19):
            new_row = pd.DataFrame(
                {"timestamp": date.replace(hour=h)}, index=[0])
            df = pd.concat([df, new_row], ignore_index=True)

    return df


def fetch_oil_data(start_date, end_date):
    start_date, end_date = parse_date(start_date), parse_date(end_date)
    df = create_dataframe(start_date, end_date)

    ticker_symbol = 'BZ=F'
    # Fetch the historical data for the specified dates
    ticker_data = yf.Ticker(ticker_symbol)

    ticker_df = ticker_data.history(start=start_date, end=end_date)

    # Extract the closing prices and print them
    closing_prices = ticker_df['Close']

    # for each day in the closing_prices index that matches the day in the df timestamp column, add it to df
    for i in range(0, len(closing_prices)):
        df.loc[df['timestamp'].dt.date == closing_prices.index[i].date(),
               'oil_price'] = closing_prices[i]

    # interpolate nan oil price values
    df["oil_price"] = df["oil_price"].interpolate()

    return df
