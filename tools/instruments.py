#!/usr/bin/env python3

import pandas as pd
from re import search


class li7810:
    def __init__(self):
        self.usecols = ["DIAG", "DATE", "TIME", "SECONDS", "NANOSECONDS", "CO2", "CH4"]
        self.dtypes = {
            "DIAG": "int",
            "DATE": "str",
            "TIME": "str",
            "SECONDS": "str",
            "NANOSECONDS": "str",
            "H2O": "float",
            "CO2": "float",
            "CH4": "float",
        }
        self.skiprows = [0, 1, 2, 3, 4, 6]
        self.delimiter = "\t"
        self.date_col = "DATE"
        self.time_col = "TIME"
        self.sec_col = "SECONDS"
        self.nsec_col = "NANOSECONDS"
        self.datetime_col = None
        self.date_fmt = "%Y-%m-%d"
        self.time_fmt = "%H:%M:%S"
        self.diag_col = "DIAG"
        self.gas_cols = ["CO2", "CH4"]

    def read_file(self, f):
        li_id = search(r"TG10-\d\d\d\d\d", f.name).group(0)
        df = pd.read_csv(
            f,
            skiprows=self.skiprows,
            delimiter=self.delimiter,
            usecols=self.usecols,
            dtype=self.dtypes,
        )
        df["li_id"] = li_id
        df["datetime"] = pd.to_datetime(
            df[self.date_col] + df[self.time_col],
            format=self.date_fmt + self.time_fmt,
            # ).dt.tz_localize("UTC")
        )
        df["numeric_datetime"] = (df[self.sec_col] + "." + df[self.nsec_col]).astype(
            float
        )
        return df
