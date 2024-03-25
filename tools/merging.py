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
    aux_df = cfg.get("df")
    main_df["idx_cp"] = main_df.index
    main_df["datetime"] = main_df["idx_cp"]
    main_df.drop("idx_cp", axis=1, inplace=True)
    id_col = "chamber"
    df = pd.merge(
        main_df, aux_df, left_on=id_col, right_on=id_col, suffixes=("", "_y")
    )
    df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
    df.set_index("datetime", inplace=True, drop=True)
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
        df[f"t_dif_{name}"] = (
            df["idx_cp"] - df[f"idx_cp_{name}"]
        ).dt.total_seconds()
        df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
        df.set_index("datetime", inplace=True)
        return df
    else:
        logger.info("Dataframes are not properly sorted by datetimeindex")
        sys.exit(0)


def merge_by_dtx_and_id(main_df, cfg):
    """
    This function merges dataframes by datetimeindex and an id column
    """
    aux_df = cfg.get("df")
    name = cfg.get("name")
    id_col = "chamber"
    # id_col = cfg.get("id_col")
    dflist = []
    chamber_num_list = aux_df["chamber"].unique().tolist()
    direction = cfg.get("direction")
    if direction is None:
        direction = "nearest"
    tolerance = cfg.get("tolerance")
    if tolerance is None:
        tolerance = "30d"
    main_df["idx_cp"] = main_df.index
    # aux_df["idx_cp"] = aux_df.index
    aux_df[f"idx_cp_{name}"] = aux_df.index
    for num in chamber_num_list:
        if is_df_valid(main_df) and is_df_valid(aux_df):
            masked_main, masked_aux = filter_with_mask(
                main_df, aux_df, num, id_col
            )
            # logger.debug(
            #     f"Merging {name}, direction: { direction}, tolerance: {tolerance}"
            # )
            df = pd.merge_asof(
                masked_main,
                masked_aux,
                left_on="datetime",
                right_on="datetime",
                tolerance=pd.Timedelta(tolerance),
                direction=direction,
                suffixes=("", "_y"),
            )
            df[f"t_dif_{name}"] = (
                df["idx_cp"] - df[f"idx_cp_{name}"]
            ).dt.total_seconds()
            df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
            df.set_index("datetime", inplace=True)
            try:
                df["snowdepth"] = df["snowdepth"].fillna(0)
                df["snowdepth"].astype("float")
            except KeyError:
                df["snowdepth"] = 0.0
                logger.debug("No snowdepth measurement, setting to 0")
            dflist.append(df)
        else:
            logger.info("Dataframes are not properly sorted by datetimeindex.")
            logger.debug(f"main_df: {main_df}")
            logger.debug(f"aux_df: {aux_df}")
            sys.exit(0)
    df = pd.concat(dflist)
    df.sort_index(inplace=True)
    return df


def is_df_valid(df):
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
        logger.debug(df)
        return False

    return df.index.is_monotonic_increasing
