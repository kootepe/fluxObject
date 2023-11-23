import sys
import logging
import pandas as pd
from pathlib import Path

logging = logging.getLogger("__main__")


def read_snow_measurement(snow_depth_file):
    set_to_zero = False
    if bool(snow_depth_file) is False:
        logging.info("No snowdepth measurement, setting snowdepth to 0")
        snowdepth = None
        set_to_zero = True
        return snowdepth, set_to_zero
    if not Path(snow_depth_file).is_file():
        logging.info(
            f"Snowdepth measurement is defined as {snow_depth_file} but the file is not found, exiting."
        )
        sys.exit(0)
    else:
        snowdepth = pd.read_excel(snow_depth_file)
        snowdepth["datetime"] = pd.to_datetime(snowdepth["datetime"], format="%d/%m/%Y")
        snowdepth["snowdepth"].astype("float")
        snowdepth.set_index("datetime", inplace=True)
    return snowdepth, set_to_zero


if __name__ == "__main__":
    snow_depth_file = sys.argv[1]
    read_snow_measurement(snow_depth_file)
