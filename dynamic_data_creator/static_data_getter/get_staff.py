
import pandas as pd
import math
from data_manager import DataManager


def get_staff():
    df = DataManager.get_dataset()[:540]
    # baseline_df = pd.read_csv('./dynamic_data_creator/baseline_staff.csv') # Use when running code from __main__
    # Use when running local api
    baseline_df = pd.read_csv('./baseline_staff.csv')

    # Find the day with the lowest transaction_count in both years
    # This was already found when setting the initial baseline
    lowest_transaction_count = 90

    staff_df = df.copy()

    baseline_staff_1 = baseline_df['workforce_type_1'].reset_index(drop=True)
    baseline_staff_2 = baseline_df['workforce_type_2'].reset_index(drop=True)
    baseline_staff_3 = baseline_df['workforce_type_3'].reset_index(drop=True)
    baseline_staff_4 = baseline_df['workforce_type_4'].reset_index(drop=True)

    staff_df['Timestamp'][0].month

    # Go through all days, seeing how much of a transaction_count is returned compared to the lowest. Add that amount to the staff count if above 1.5
    for i in range(0, len(staff_df)-10, 9):
        daily_transaction_count = 0

        for a in range(i, i+9):
            daily_transaction_count += int(staff_df['transaction_count'][a])

        # xy iterates only through the baseline staff
        xy = 0
        for a in range(i, i+9):
            # Dividing so it could be spread across staffs
            transaction_multiple = math.floor(
                (round(daily_transaction_count / lowest_transaction_count) - 1)/2)

            # Upperbound of transaction multiple compared to the lowest transaction count
            if (transaction_multiple > 5):
                transaction_multiple = 5

            staff_df.at[a, 'workforce_type_1'] = baseline_staff_1[xy]

            staff_df.at[a, 'workforce_type_2'] = baseline_staff_2.add(transaction_multiple)[
                xy]
            staff_df.at[a, 'workforce_type_3'] = baseline_staff_3.add(transaction_multiple)[
                xy]

            # If it's Christmas, add an extra part timer too
            if (staff_df['Timestamp'][i].month == 12 and staff_df['workforce_type_4'][xy] != 0):
                staff_df.at[a, 'workforce_type_4'] = baseline_staff_4.add(1)[
                    xy]
            else:
                # Monday and Tuesday are least busiest days, don't need part-timers then
                mon_or_tue = staff_df['Timestamp'][i].dayofweek == 1 or staff_df['Timestamp'][i].dayofweek == 2
                if (not mon_or_tue):
                    staff_df.at[a, 'workforce_type_4'] = baseline_staff_4[xy]
                else:
                    staff_df.at[a, 'workforce_type_4'] = 0

            xy += 1

    return staff_df
