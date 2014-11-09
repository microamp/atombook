# -*- coding: utf-8 -*-
"""
Auckland market rent data by district
"""

from functools import partial

import pandas as pd

from .lib import pipe, rename, set_index, add_postfix, merge_dfs


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
    print("[Mean]")
    print("* last five:")
    print(mean.tail())

    # lower-quartile values
    lq = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=add_postfix(column_names, "(LQ)")),
        partial(set_index, column="Date Lodged (LQ)", to_datetime=True)
    )("data/synthetic-lower-quartile-rents-by-ta.csv").tail(12 * years)
    print("[Lower-Quartile]")
    print("* last five:")
    print(lq.tail())

    # upper-quartile values
    uq = pipe(
        partial(pd.read_csv, usecols=columns),
        partial(rename, columns=add_postfix(column_names, "(UQ)")),
        partial(set_index, column="Date Lodged (UQ)", to_datetime=True)
    )("data/synthetic-upper-quartile-rents-by-ta.csv").tail(12 * years)
    print("[Lower-Quartile]")
    print("* last five:")
    print(uq.tail())

    # merge data frames together
    merged = merge_dfs(mean, lq, uq)

    # north shore city district (mean/lower-quartile/upper-quartile)
    nscd = rename(merged[["North Shore City District",
                          "North Shore City District (LQ)",
                          "North Shore City District (UQ)"]],
                  columns={"North Shore City District": "Mean",
                           "North Shore City District (LQ)": "Lower-Quartile",
                           "North Shore City District (UQ)": "Upper-Quartile"})
    print("[North Shore City (Mean/Lower-Quartile/Upper-Quartile)]")
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
    print("* last five:")
    print(acd.tail())
