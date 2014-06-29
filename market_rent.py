# -*- coding: utf-8 -*-
"""
Auckland market rent data by district
"""

import sys
from functools import partial
if sys.version_info[0] == 3:
    from functools import reduce
    reduce = reduce

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
    df.index.name = column  # set index name
    return df


def add_postfix(column_names, postfix):
    return lambda c: "%s %s" % (column_names.get(c, c), postfix,)


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
                    "National": "Nationwide",
                    "Date.Lodged": "Date Lodged"}
    years = 15  # last 15 years

    # mean values
    mean = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=column_names),
        partial(set_index, column="Date Lodged", to_datetime=True)
    )("data/mean-rents-by-ta.csv").tail(12 * years)

    # lower-quartile values
    lq = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=add_postfix(column_names, "(LQ)")),
        partial(set_index, column="Date Lodged (LQ)", to_datetime=True)
    )("data/synthetic-lower-quartile-rents-by-ta.csv").tail(12 * years)

    # upper-quartile values
    uq = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=add_postfix(column_names, "(UQ)")),
        partial(set_index, column="Date Lodged (UQ)", to_datetime=True)
    )("data/synthetic-upper-quartile-rents-by-ta.csv").tail(12 * years)

    # merge data frames together
    merged = reduce(lambda df1, df2: pd.merge(
        df1, df2,
        left_index=df1.index.name, right_index=df2.index.name,
        how="inner"
    ), (mean, lq, uq,))

    # north shore city district (mean/lower-quartile/upper-quartile)
    nscd = rename(merged[["North Shore City District",
                          "North Shore City District (LQ)",
                          "North Shore City District (UQ)"]],
                  columns={"North Shore City District": "Mean",
                           "North Shore City District (LQ)": "Lower-Quartile",
                           "North Shore City District (UQ)": "Upper-Quartile"})
    print("[North Shore City (Mean/Lower-Quartile/Upper-Quartile)]")
    print("* first five:")
    print(nscd.head())
    print("* last five:")
    print(nscd.tail())

    # auckland city district (mean/lower-quartile/upper-quartile)
    acd = rename(merged[["Auckland City District",
                         "Auckland City District (LQ)",
                         "Auckland City District (UQ)"]],
                 columns={"Auckland City District": "Mean",
                          "Auckland City District (LQ)": "Lower-Quartile",
                          "Auckland City District (UQ)": "Upper-Quartile"})
    print("[Auckland City (Mean/Lower-Quartile/Upper-Quartile)]")
    print("* first five:")
    print(acd.head())
    print("* last five:")
    print(acd.tail())
