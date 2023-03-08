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

    def add_to_dataset(column, new_data):
        DataManager.dataset[column] = new_data

    def extend_column(columns, new_rows):
        # add a row to the dataset
        df = pd.DataFrame(new_rows, columns=columns)

        DataManager.dataset = pd.concat(
            [DataManager.dataset, df], ignore_index=True)

    def get_dataset():
        return DataManager.dataset

    # Na values used for TFT encoder to work
    def fill_na():
        DataManager.dataset = DataManager.dataset.fillna(0)

    def reset_index():
        DataManager.dataset.reset_index(inplace=True)

    def export_to_csv():
        DataManager.dataset.to_csv("dataset.csv")

    def print_dataset():
        print(DataManager.dataset)
