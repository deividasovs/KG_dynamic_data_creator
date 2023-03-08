
# %%from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt
from data_manager import DataManager
from datetime import datetime

# df = DataManager.get_dataset()
df = pd.read_csv('./dynamic_data_creator/dataset.csv')[:539]
baseline_df = pd.read_csv('./dynamic_data_creator/baseline_staff.csv')

# print baseline_df keys
print(baseline_df.keys())


def add_staff():
    # Find the day with the lowest transaction_count in both years
    lowest_transaction_count = 10000
    lowest_transaction_count_day = df['timestamp'][0]

    newDf = df.copy()

    for i in range(0, len(newDf)-10, 9):
        temp_lowest = 0

        if (newDf['transaction_count'][i:i+9].all() != 0):

            for a in range(i, i+9):
                # print("Adding: ", newDf['Timestamp'][a], newDf['transaction_count'][a])
                temp_lowest += newDf['transaction_count'][a]

            if (temp_lowest < lowest_transaction_count):
                lowest_transaction_count = temp_lowest
                lowest_transaction_count_day = newDf['timestamp'][i]

    lowest_transaction_count_day, lowest_transaction_count

    baselineStaff_1 = baseline_df['workforce_type_1'].reset_index(drop=True)
    baselineStaff_2 = baseline_df['workforce_type_2'].reset_index(drop=True)
    baselineStaff_3 = baseline_df['workforce_type_3'].reset_index(drop=True)
    baselineStaff_4 = baseline_df['workforce_type_4'].reset_index(drop=True)

# 2023-01-08 10:00:00
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    newDf['timestamp'] = newDf['timestamp'].apply(
        lambda x: datetime.strptime(x, DATE_FORMAT))

    newDf['timestamp'][0].month

    # Go through all days, seeing how much of a transaction_count is returned compared to the lowest. Add that amount to the staff count if above 1.5
    for i in range(0, len(newDf)-10, 9):
        daily_transaction_count = 0

        for a in range(i, i+9):
            daily_transaction_count += newDf['transaction_count'][a]

        # xy iterates only through the baseline staff
        xy = 0
        for a in range(i, i+9):
            # Dividing so it could be spread across staffs
            transaction_multiple = math.floor(
                (round(daily_transaction_count / lowest_transaction_count) - 1)/2)

            # Upperbound of transaction multiple compared to the lowest transaction count
            if (transaction_multiple > 5):
                transaction_multiple = 5

            newDf.at[a, 'workforce_type_1'] = baselineStaff_1[xy]

            newDf.at[a, 'workforce_type_2'] = baselineStaff_2.add(transaction_multiple)[
                xy]
            newDf.at[a, 'workforce_type_3'] = baselineStaff_3.add(transaction_multiple)[
                xy]

            # If it's Christmas, add an extra part timer too
            if (newDf['timestamp'][i].month == 12 and newDf['workforce_type_4'][xy] != 0):
                newDf.at[a, 'workforce_type_4'] = baselineStaff_4.add(1)[xy]
            else:
                # Monday and Tuesday are least busiest days, don't need part-timers then
                mondayOrTuesday = newDf['timestamp'][i].dayofweek == 1 or newDf['timestamp'][i].dayofweek == 2
                if (not mondayOrTuesday):
                    newDf.at[a, 'workforce_type_4'] = baselineStaff_4[xy]
                else:
                    newDf.at[a, 'workforce_type_4'] = 0

            xy += 1

    return newDf


if __name__ == "__main__":

    newDf = add_staff()
    newDf.to_csv('./withstaff.csv')
    print(newDf)
