import sys
import pandas as pd

def read_snow_measurement(snow_depth_file):
    if snow_depth_file:
        snowdepth = pd.read_excel(snow_depth_file)
        snowdepth["datetime"] = pd.to_datetime(snowdepth["datetime"])
        snowdepth.set_index("datetime", inplace=True)
    else:
        d = {
            "datetime": ["01-01-2023", "02-01-2023"],
            "snowdepth1": [0, 0],
            "snowdepth2": [0, 0],
        }
        snowdepth = pd.DataFrame(data=d)
        snowdepth["datetime"] = pd.to_datetime(
            snowdepth["datetime"], format="%d-%m-%Y"
        )
        snowdepth.set_index("datetime", inplace=True)
        print(snowdepth)

if __name__ == "__main__":
    snow_depth_file = sys.argv[1]
    read_snow_measurement(snow_depth_file)
