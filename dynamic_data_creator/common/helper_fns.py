from common.consts import DATE_FORMAT

import datetime as dt


def create_date():
    start_date = (dt.datetime.today() -
                  dt.timedelta(days=60)).replace(hour=0, minute=0, second=0)

    start_date = start_date.strftime(
        DATE_FORMAT)

    end_date = (dt.datetime.today() - dt.timedelta(days=1)
                ).replace(hour=0, minute=0, second=0)

    end_date = end_date.strftime(DATE_FORMAT)

    return start_date, end_date


def parse_date(date):
    return dt.datetime.strptime(date, DATE_FORMAT)
