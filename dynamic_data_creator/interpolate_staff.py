
# %%from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt
from data_manager import DataManager
from datetime import datetime


def add_staff():
    df = DataManager.get_dataset()[:540]
    baseline_df = pd.read_csv('./baseline_staff.csv')
    # Find the day with the lowest transaction_count in both years
    # This was already found when setting the initial baseline
    lowest_transaction_count = 90

    newDf = df.copy()

    baselineStaff_1 = baseline_df['workforce_type_1'].reset_index(drop=True)
    baselineStaff_2 = baseline_df['workforce_type_2'].reset_index(drop=True)
    baselineStaff_3 = baseline_df['workforce_type_3'].reset_index(drop=True)
    baselineStaff_4 = baseline_df['workforce_type_4'].reset_index(drop=True)

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # newDf['Timestamp'] = newDf['Timestamp'].apply(
    # lambda x: datetime.strptime(x, DATE_FORMAT))

    newDf['Timestamp'][0].month

    # Go through all days, seeing how much of a transaction_count is returned compared to the lowest. Add that amount to the staff count if above 1.5
    for i in range(0, len(newDf)-10, 9):
        daily_transaction_count = 0

        for a in range(i, i+9):
            daily_transaction_count += int(newDf['transaction_count'][a])

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
            if (newDf['Timestamp'][i].month == 12 and newDf['workforce_type_4'][xy] != 0):
                newDf.at[a, 'workforce_type_4'] = baselineStaff_4.add(1)[xy]
            else:
                # Monday and Tuesday are least busiest days, don't need part-timers then
                mondayOrTuesday = newDf['Timestamp'][i].dayofweek == 1 or newDf['Timestamp'][i].dayofweek == 2
                if (not mondayOrTuesday):
                    newDf.at[a, 'workforce_type_4'] = baselineStaff_4[xy]
                else:
                    newDf.at[a, 'workforce_type_4'] = 0

            xy += 1

    return newDf
