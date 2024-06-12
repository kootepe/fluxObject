import pandas as pd
import logging
from tools.influxdb_funcs import read_aux_ifdb

logger = logging.getLogger("defaultLogger")


def read_aux_data(aux_cfgs, s_ts=None, e_ts=None):
    for f in aux_cfgs:
        # NOTE: implement better checks if data is in db or in files
        if f.get("files"):
            dfs = read_files(f)
            if isinstance(dfs.index, pd.DatetimeIndex):
                pass
                # dfs.index = dfs.index.tz_localize("UTC")
                # dfs.set_index(dfs.index.tz_convert("ETC/GMT-0"))
            dfs.sort_index(inplace=True)
            f["df"] = dfs
        # NOTE: implement better checks if data is in db or in files
        if f.get("bucket"):
            df = read_db(f, s_ts, e_ts)
            df = df.rename(columns={f.get("field"): f.get("name")})
            f["df"] = df
    return aux_cfgs


def read_files(cfg):
    dfs = []
    for file in cfg.get("files"):
        argss = cfg.get("args")
        df = pd.read_csv(file, **argss)
        dfs.append(df)
    if len(df) == 0:
        logger.debug(f"No data returned by files found for aux_data {cfg.get('name')}")
    dfs = pd.concat(dfs)
    return dfs


def read_db(cfg, s_ts, e_ts):
    df = read_aux_ifdb(cfg, str(s_ts), (e_ts))
    return df
