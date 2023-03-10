import holidays
import pandas as pd
from data_manager import DataManager

irl_holidays = holidays.Ireland()

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_holidays():
    hols = pd.DataFrame(columns=['holiday'])
    for index, row in DataManager.get_dataset().iterrows():

        date = row['Timestamp']

        if date in irl_holidays:
            # Set the value of the 'Holiday' column to True
            hols.at[index, 'holiday'] = True
        else:
            hols.at[index, 'holiday'] = False

    return hols
