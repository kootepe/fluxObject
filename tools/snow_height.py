import sys
import pandas as pd


def read_snow_measurement(snow_depth_file):
    if snow_depth_file:
        snowdepth = pd.read_excel(snow_depth_file)
        snowdepth["datetime"] = pd.to_datetime(snowdepth["datetime"], format="%d/%m/%Y")
        snowdepth.set_index("datetime", inplace=True)
    else:
        snowdepth = None
    return snowdepth


if __name__ == "__main__":
    snow_depth_file = sys.argv[1]
    read_snow_measurement(snow_depth_file)