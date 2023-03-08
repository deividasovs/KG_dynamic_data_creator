from common.consts import DATE_FORMAT

import datetime as dt


def parse_date(date):
    return dt.datetime.strptime(date, DATE_FORMAT)
