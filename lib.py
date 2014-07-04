# -*- coding: utf-8 -*-

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


def merge_dfs(*dfs):
    return reduce(lambda df1, df2: pd.merge(
        df1, df2,
        left_index=df1.index.name, right_index=df2.index.name,
        how="inner"
    ), dfs)
