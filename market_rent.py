# -*- coding: utf-8 -*-
"""
Auckland market rent data by district
"""

from datetime import datetime

from pandas.io.parsers import read_csv


DATA_DIR = "data"

INDEX_COLUMN = "Date Lodged"
COLUMNS = (("Auckland", "Auckland City District"),
           ("Manukau", "Manukau City District"),
           ("North Shore", "North Shore City District"),
           ("Waitakere", "Waitakere City District"),
           ("Date.Lodged", INDEX_COLUMN),)

NA_VALUE = "NA"


def compose(*fns):
    def _compose(*args, **kwargs):
        return reduce(lambda r, fn: fn(r),
                      fns[1:],
                      fns[0](*args, **kwargs))

    return _compose


def set_index(df):
    return df.set_index(INDEX_COLUMN)


def dtfy(df):
    def _str_to_dt(s):
        return datetime.strptime(s, "%Y-%m-%d")

    df[INDEX_COLUMN] = df[INDEX_COLUMN].map(_str_to_dt)
    return df


def rename_columns(df):
    return df.rename(columns=dict(COLUMNS), inplace=False)


def read_data(filename):
    return read_csv("{0}/{1}".format(DATA_DIR, filename),
                    usecols=("Auckland",
                             "Franklin District",
                             "Manukau",
                             "North Shore",
                             "Papakura District",
                             "Rodney District",
                             "Waitakere",
                             "National",
                             "Date.Lodged",))


if __name__ == "__main__":
    fn = compose(read_data, rename_columns, dtfy, set_index)
    df = fn("mean-rents-by-ta.csv")
    print("First five rows:")
    print(df.head())
    print("Last five rows:")
    print(df.tail())
    print(df.plot())
