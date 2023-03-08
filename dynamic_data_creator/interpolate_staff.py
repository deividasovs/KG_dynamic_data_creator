
# %%from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt

# %%

# %%

df = pd.read_csv(file_to_read)


# %%
df['transaction_count'][0]

# %%
# Find the day with the lowest transaction_count in both years
lowest_transaction_count = 10000
lowest_transaction_count_day = df['Timestamp'][0]

newDf = df.copy()


for i in range(0, len(newDf)-10, 9):
    temp_lowest = 0

    if (newDf['transaction_count'][i:i+9].all() != 0):

        for a in range(i, i+9):
            # print("Adding: ", newDf['Timestamp'][a], newDf['transaction_count'][a])
            temp_lowest += newDf['transaction_count'][a]

        if (temp_lowest < lowest_transaction_count):
            lowest_transaction_count = temp_lowest
            lowest_transaction_count_day = newDf['Timestamp'][i]


lowest_transaction_count_day, lowest_transaction_count

# %%
newDf[2900:2910]

# %%
baselineStaff_1 = newDf['workforce_type_1'][2898:2907].reset_index(drop=True)
baselineStaff_2 = newDf['workforce_type_2'][2898:2907].reset_index(drop=True)
baselineStaff_3 = newDf['workforce_type_3'][2898:2907].reset_index(drop=True)
baselineStaff_4 = newDf['workforce_type_4'][2898:2907].reset_index(drop=True)
baselineStaff_4

# %%

DATE_FORMAT = "%d/%m/%Y %H:%M"

newDf['Timestamp'] = newDf['Timestamp'].apply(
    lambda x: datetime.strptime(x, DATE_FORMAT))

newDf

# %%
# baselineStaff_2.add(1)
newDf['Timestamp'][0].month

# %%
# Go through all days, seeing how much of a transaction_count is returned compared to the lowest. Add that amount to the staff count if above 1.5
for i in range(0, len(newDf)-10, 9):
    daily_trasaction_count = 0

    for a in range(i, i+9):
        daily_trasaction_count += newDf['transaction_count'][a]

    # xy iterates only through the baseline staff
    xy = 0
    for a in range(i, i+9):
        # Dividing so it could be spread across staffs
        transMultiple = math.floor(
            (round(daily_trasaction_count / lowest_transaction_count) - 1)/2)

        # Upperbound
        if (transMultiple > 5):
            transMultiple = 5

        newDf.at[a, 'workforce_type_1'] = baselineStaff_1[xy]

        newDf.at[a, 'workforce_type_2'] = baselineStaff_2.add(transMultiple)[
            xy]
        newDf.at[a, 'workforce_type_3'] = baselineStaff_3.add(transMultiple)[
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

# newDf.head(10)
# newDf[1000:1200]

# %%
# Add to csv
newDf.to_csv(file_to_write)
