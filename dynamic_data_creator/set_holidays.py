# Script to fill missing hours with 0s
# Used in Crude Oil csv

import csv
from datetime import datetime, timedelta
from datetime import date
import holidays


irl_holidays = holidays.Ireland()  # this is a dict

DATE_FORMAT = "%d/%m/%Y %H:%M"

fileName = "data_file.csv"

file_open = open('./Data/' + 'withHols_'+fileName, 'w')
csv_writer = csv.writer(file_open)

with open('./Data/'+fileName, 'r') as csvfile:
    dataReader = csv.reader(csvfile)

    data_list = list(dataReader)

    for row in data_list:

        if (row[0] == 'timestamp'):
            continue
        row[0] = datetime.strptime(row[0], DATE_FORMAT)

        if (row[0] in irl_holidays):
            row[7] = True
        else:
            row[7] = False

        csv_writer.writerow(row)
