# -*- coding: utf-8 -*-
"""
Auckland market rent data by district
"""

from functools import partial

import pandas as pd


def pipe(*fns):
    def _pipe(*args, **kwargs):
        return reduce(lambda r, fn: fn(r),
                      fns[1:],
                      fns[0](*args, **kwargs))

    return _pipe


def rename(df, *args, **kwargs):
    return getattr(df, rename.__name__)(*args, **kwargs)


def set_index(df, column="", to_datetime=False):
    df = df.set_index(column)
    if to_datetime:
        df.index = df.index.to_datetime()
    return df


if __name__ == "__main__":
    columns = ("Auckland",
               "Franklin District",
               "Manukau",
               "North Shore",
               "Papakura District",
               "Rodney District",
               "Waitakere",
               "National",
               "Date.Lodged",)
    index_column = "Date.Lodged"
    column_names = {"Auckland": "Auckland City District",
                    "Manukau": "Manukau City District",
                    "North Shore": "North Shore City District",
                    "Waitakere": "Waitakere City District",
                    "Date.Lodged": "Date Lodged"}

    df = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=column_names),
        partial(set_index, column="Date Lodged", to_datetime=True)
    )("data/mean-rents-by-ta.csv")
    print(df.head())
