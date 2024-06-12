import logging
import sys
import pandas as pd

logger = logging.getLogger("defaultLogger")


def filter_with_mask(main_df, aux_df, id_value, id_col="chamber"):
    main_mask = main_df[id_col] == id_value
    aux_mask = aux_df[id_col] == id_value
    masked_main = main_df[main_mask]
    masked_aux = aux_df[aux_mask]
    return masked_main, masked_aux


def merge_by_id(main_df, cfg):
    logger.debug(main_df["chamber"])
    logger.debug(cfg.get("df")["chamber"])
    aux_df = cfg.get("df")
    logger.debug(aux_df.index)
    logger.debug(main_df.index)
    main_df["idx_cp"] = main_df.index
    main_df["datetime"] = main_df["idx_cp"]
    main_df.drop("idx_cp", axis=1, inplace=True)
    id_col = "chamber"
    df = pd.merge(main_df, aux_df, left_on=id_col, right_on=id_col, suffixes=("", "_y"))
    df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
    df.set_index("datetime", inplace=True, drop=True)
    logger.debug(df.index)
    return df


def merge_by_dtx(main_df, cfg):
    aux_df = cfg.get("df")
    name = cfg.get("name")
    direction = cfg.get("direction")
    if direction is None:
        direction = "nearest"
    tolerance = cfg.get("tolerance")
    if tolerance is None:
        tolerance = "30d"
    main_df["idx_cp"] = main_df.index
    aux_df[f"idx_cp_{name}"] = aux_df.index
    if is_df_valid(main_df) and is_df_valid(aux_df):
        df = pd.merge_asof(
            main_df,
            aux_df,
            left_on="datetime",
            right_on="datetime",
            tolerance=pd.Timedelta(tolerance),
            direction=direction,
            suffixes=("", "_y"),
        )
        df[f"t_dif_{name}"] = (df["idx_cp"] - df[f"idx_cp_{name}"]).dt.total_seconds()
        df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
        df.set_index("datetime", inplace=True)
        return df
    else:
        logger.info("Dataframes are not properly sorted by datetimeindex")
        sys.exit(0)


def merge_by_dtx_and_id(main_df, cfg):
    """
    This sole use of this function is to merge AC chamber snowdepth
    measurement into the main dataframe.
    It's not as clean as I'd like but it works fast and well.
    """
    measurement_df = main_df
    other_df = cfg.get("df")
    # other_df = aux_df
    # main_df.to_csv("tests/test_data/snow_main.csv")
    # other_df.to_csv("tests/test_data/snow_other.csv")
    dflist = []
    chamber_num_list = other_df["chamber"].unique().tolist()
    for num in chamber_num_list:
        if is_dataframe_sorted_by_datetime_index(
            measurement_df
        ) and is_dataframe_sorted_by_datetime_index(other_df):
            main_df = measurement_df.copy()
            other_df = other_df.copy()
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
