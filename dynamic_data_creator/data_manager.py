# Singleton object managing the dataset

import pandas as pd


class DataManager(object):
    # create empty dataset
    dataset = pd.DataFrame()

    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    def add_to_dataset(column, newData):
        DataManager.dataset[column] = newData

    def print_dataset():
        print(DataManager.dataset)


# Sample data format after squashing:
# Timestamp	subtotal	transaction_count	rain	temperature	holiday	oil_price	workforce_type_1	workforce_type_2	workforce_type_3	workforce_type_4
# 17/05/2021 10:00	428.03	11	0	12.7	FALSE	69.62	1	2	3	0
