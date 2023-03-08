from datetime import datetime
from botocore.config import Config
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import pandas as pd

from data_manager import DataManager
from common.helper_fns import parse_date

# 19 = website
# 5 = RSY - The Kilkenny Shop - Killarney
# 7 = Nassau Street
location_id = "7"

opening_time = 10  # incl
closing_time = 19  # not incl


def get_time_range_list(start_date, end_date):
    return pd.date_range(start_date, end_date, freq='d').date.tolist()


def query_dynamodb(table, pk, sk1, sk2, loc_id):
    transactions = []
    try:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(pk).__and__(
                Key('SK').between(sk1, sk2)),
            FilterExpression=Key(
                'Thing_Organization_LocalBusiness_locationId').eq(loc_id),
            ProjectionExpression='Thing_Intangible_Offer_subtotal, SK'
        )
        transactions.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('PK').eq(pk).__and__(
                    Key('SK').between(sk1, sk2)),
                FilterExpression=Key(
                    'Thing_Organization_LocalBusiness_locationId').eq(loc_id),
                ExclusiveStartKey=response['LastEvaluatedKey'],
                ProjectionExpression='Thing_Intangible_Offer_subtotal, SK'
            )
            transactions.extend(response['Items'])
        return transactions
    except ClientError as err:
        # logger.error(
        #     "Couldn't query for movies released in %s. Here's why: %s: %s", year,
        #     err.response['Error']['Code'], err.response['Error']['Message'])
        raise


# Start & end values not included for some reason
def fetch_hourly_sales(start_date_str, end_date_str):

    my_config = Config(
        region_name='eu-west-1',
    )

    dynamodb = boto3.resource('dynamodb', config=my_config)

    table = dynamodb.Table('opsuitestaging')

    # start date day not included in time range
    start_date, end_date = parse_date(start_date_str), parse_date(end_date_str)

    total_hourly_sales = 0
    date_ranges = get_time_range_list(start_date, end_date)

    final_result = []

    # New query for each different day we are calling
    for dIndex in range(0, len(date_ranges)-1):
        sort_key_start = f'TRA#{date_ranges[dIndex].strftime("%Y-%m-%d:%H:%M:%S")}'
        sort_key_end = f'TRA#{date_ranges[dIndex+1].strftime("%Y-%m-%d:%H:%M:%S")}'

        # If month has changed, use next index or else it will call partition key with previous month
        if (date_ranges[dIndex+1].day == 1):
            partition_key = f'TRA#{date_ranges[dIndex+1].strftime("%Y-%m")}'
        else:
            partition_key = f'TRA#{date_ranges[dIndex].strftime("%Y-%m")}'

        transactions = query_dynamodb(
            table, partition_key, sort_key_start, sort_key_end, location_id)

        for i in range(opening_time, closing_time):
            total_hourly_sales = 0
            transaction_count = 0

            hour_check = str(i)

            for x in range(0, len(transactions)):
                item = transactions[x]
                # print(item)
                hour = item['SK'].split(' ')[1][0:2]

                if (i < 10):
                    hour_check = '0' + str(i)

                if (hour_check == hour):
                    transaction_count += 1
                    total_hourly_sales += item['Thing_Intangible_Offer_subtotal']

            current_date = date_ranges[dIndex+1].strftime(
                "%d-%m-%Y") + " " + hour_check + ":00:00"

            hourly_sales = {
                current_date: {
                    "subtotal": str(total_hourly_sales),
                    "transaction_count": str(transaction_count)
                }
            }

            final_result.append(hourly_sales)

    return to_dataset(final_result)


def to_dataset(data):
    headings = ["timestamp", "subtotal", "transaction_count"]

    df = pd.DataFrame(columns=headings)

    for d in data:
        header = list(d.keys())

        v = list(d.values())

        transaction_row = [header[0], v[0]
                           ['subtotal'], v[0]['transaction_count']]

        df2 = pd.DataFrame(columns=headings, data=[transaction_row])

        df = pd.concat([df, df2])

    df = df.reset_index()
    return df
