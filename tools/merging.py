import logging
import sys
import pandas as pd

logger = logging.getLogger("__main__")


def merge_aux_by_column(measurement_df, aux_df):
    """
    This sole use of this function is to merge AC chamber snowdepth
    measurement into the main dataframe.
    It's not as clean as I'd like but it works fast and well.
    """
    main_df = measurement_df
    other_df = aux_df
    # main_df.to_csv("tests/test_data/snow_main.csv")
    # other_df.to_csv("tests/test_data/snow_other.csv")
    dflist = []
    chamber_num_list = other_df["chamber"].unique().tolist()
    for num in chamber_num_list:
        if is_dataframe_sorted_by_datetime_index(
            measurement_df
        ) and is_dataframe_sorted_by_datetime_index(aux_df):
            main_df = measurement_df.copy()
            other_df = aux_df.copy()
            main_mask = main_df["chamber"] == num
            other_mask = other_df["chamber"] == num
            main_df = main_df[main_mask]
            other_df = other_df[other_mask]
            other_df = other_df.drop(columns=["chamber"])
            df = pd.merge_asof(
                main_df,
                other_df,
                left_on="datetime",
                right_on="datetime",
                tolerance=pd.Timedelta("1000d"),
                direction="backward",
                suffixes=("", "_y"),
            )
            df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
            df.set_index("datetime", inplace=True)
            df["snowdepth"] = df["snowdepth"].fillna(0)
            df["snowdepth"].astype("float")
            dflist.append(df)
        else:
            logger.info("Dataframes are not properly sorted by datetimeindex")
            sys.exit(0)
    df = pd.concat(dflist)
    df.sort_index(inplace=True)
    # df.to_csv("tests/test_data/snow_ready.csv")
    return df


def is_dataframe_sorted_by_datetime_index(df):
    """
    Checks that the dataframe is a dataframe, is sorted by a
    datetimeindex and that the index is ascending

    args:
    ---
    df -- pandas.dataframe

    returns:
    ---
    bool

    """
    if not df.index.is_monotonic_increasing:
        df.sort_index(inplace=True)

    if not isinstance(df, pd.DataFrame):
        logger.info("Not a dataframe.")
        return False

    if not isinstance(df.index, pd.DatetimeIndex):
        logger.info("Index is not a datetimeindex.")
        return False

    if df.index.is_monotonic_decreasing:
        logger.info("Datetimeindex goes backwards.")
        return False

    return df.index.is_monotonic_increasing
