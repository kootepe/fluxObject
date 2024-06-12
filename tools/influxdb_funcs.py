import influxdb_client as ifdb
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from urllib3.exceptions import NewConnectionError
import datetime
from tools.time_funcs import ordinal_timer, get_time_diff, convert_timestamp_format
import pandas as pd

logger = logging.getLogger("defaultLogger")


def init_client(ifdb_dict):
    url = ifdb_dict.get("url")
    token = ifdb_dict.get("token")
    org = ifdb_dict.get("organization")
    timeout = ifdb_dict.get("timeout")

    client = ifdb.InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
    return client


def mk_field_q(field_list):
    q = f'\t|> filter(fn: (r) => r["_field"] == "{field_list[0]}"'
    for f in field_list[1:]:
        q += f' or r["_field"] == "{f}"'
    q += ")\n"
    return q


def mk_bucket_q(bucket):
    return f'from(bucket: "{bucket}")\n'


def mk_range_q(start, stop):
    return f"\t|> range(start: {start}, stop: {stop})\n"


def mk_meas_q(measurement):
    return f'\t|> filter(fn: (r) => r["_measurement"] == "{measurement}")\n'


def mk_query(bucket, start, stop, measurement, fields):
    query = (
        f"{mk_bucket_q(bucket)}"
        f"{mk_range_q(start, stop)}"
        f"{mk_meas_q(measurement)}"
        f"{mk_field_q(fields)}"
        '\t|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    )
    return query


def mk_oldest_ts_q(bucket, measurement, fields):
    query = (
        f"{mk_bucket_q(bucket)}"
        f"{mk_range_q(0, 'now()')}"
        f"{mk_meas_q(measurement)}"
        f"{mk_field_q(fields)}"
        '\t|> first(column: "_time")\n'
        '\t|> yield(name: "first")'
    )
    return query


def mk_newest_ts_q(bucket, measurement, fields):
    query = (
        f"{mk_bucket_q(bucket)}"
        f"{mk_range_q(0, 'now()')}"
        f"{mk_meas_q(measurement)}"
        f"{mk_field_q(fields)}"
        '|> last(column: "_time")\n'
        '|> yield(name: "last")'
    )
    return query


def mk_ifdb_ts(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def read_aux_ifdb(dict, s_ts=None, e_ts=None):
    logger.debug(dict)
    # logger.debug(f"Running query from {start_ts} to {stop_ts}")

    bucket = dict.get("bucket")
    measurement = dict.get("measurement_name")
    fields = list(dict.get("field").split(","))

    with init_client(dict) as client:
        q_api = client.query_api()
        query = mk_query(bucket, s_ts, e_ts, measurement, fields)
        logger.debug("Query:\n" + query)
        try:
            df = q_api.query_data_frame(query)[["_time"] + fields]
        except Exception:
            logger.info(f"No data with query:\n {query}")
            return None

        df = df.rename(columns={"_time": "datetime"})
        df["datetime"] = df.datetime.dt.tz_convert(None)
        df.set_index("datetime", inplace=True)
        logger.debug(f"\n{df}")
        return df


def read_ifdb(ifdb_dict, meas_dict, start_ts=None, stop_ts=None):
    logger.debug(f"Running query from {start_ts} to {stop_ts}")

    bucket = ifdb_dict.get("bucket")
    measurement = meas_dict.get("measurement")
    fields = list(meas_dict.get("fields").split(","))

    if start_ts is not None:
        start = mk_ifdb_ts(start_ts)
    else:
        start = 0

    if stop_ts is not None:
        stop = mk_ifdb_ts(stop_ts)
    else:
        stop = "now()"

    with init_client(ifdb_dict) as client:
        q_api = client.query_api()
        query = mk_query(bucket, start, stop, measurement, fields)
        logger.debug("Query:\n" + query)
        try:
            df = q_api.query_data_frame(query)[["_time"] + fields]
        except Exception:
            logger.info(f"No data with query:\n {query}")
            return None

        df = df.rename(columns={"_time": "datetime"})
        df["datetime"] = df.datetime.dt.tz_convert(None)
        df = add_cols_to_ifdb_q(df, meas_dict)
        logger.debug(f"\n{df}")
        return df


def add_cols_to_ifdb_q(df, meas_dict):
    # NOTE: need to figure out a better way of doing this, maybe a matte
    # reformatting what columns are expected down the pipeline. Does there need
    # to be a separate .ini for specifying settings of the whole pipeline?
    """
    current version cant use dataframes returned by ifdb as is, this function
    will add necessary columns
    """
    name = meas_dict.get("name")
    if name == "man_ts":
        df["start_time"] = df["datetime"]
        df["end_time"] = df["start_time"] + pd.Timedelta(seconds=300)
        diff = float(
            get_time_diff(df.iloc[0]["start_time"], df.iloc[0]["end_time"])
        ) * (20 / 100)
        df["open_time"] = df["end_time"] - pd.to_timedelta(diff, unit="s")
        df["close_time"] = df["start_time"] + pd.to_timedelta(diff, unit="s")
        # df["chamber"] = df["Plot Number"]
    # df["datetime"] = df.datetime.dt.tz_convert(None)
    df["DATE"] = df["datetime"].dt.strftime("%Y-%m-%d")
    df["TIME"] = df["datetime"].dt.strftime("%H:%M:%S")
    df["checks"] = ""
    df["is_valid"] = ""

    logger.info("Calculating ordinal times.")
    df["ordinal_date"] = pd.to_datetime(df["DATE"]).map(datetime.datetime.toordinal)
    df["ordinal_time"] = ordinal_timer(df["TIME"].values)
    df["ordinal_datetime"] = df["ordinal_time"] + df["ordinal_date"]
    df.set_index("datetime", inplace=True)
    logger.debug(f"\n{df}")
    return df


def ifdb_push(df, ifdb_dict, tag_columns):
    """
    Push data to InfluxDB

    args:
    ---
    df -- pandas dataframe
        data to be pushed into influxdb

    returns:
    ---

    """
    with ifdb.InfluxDBClient(
        url=ifdb_dict.get("url"),
        token=ifdb_dict.get("token"),
        org=ifdb_dict.get("organization"),
    ) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        try:
            write_api.write(
                bucket=ifdb_dict.get("bucket"),
                record=df,
                data_frame_measurement_name=ifdb_dict.get("measurement_name"),
                data_frame_timestamp_timezone=ifdb_dict.get("timezone"),
                data_frame_tag_columns=tag_columns,
                debug=True,
            )
        except NewConnectionError:
            logger.info(f"Couldn't connect to database at " f"{ifdb_dict.get('url')}")
            pass

        first = str(df.index[0])
        last = str(df.index[-1])
        logging.info(f"Pushed data between {first}-{last} to DB")


def check_last_db_timestamp(influxdb_dict):
    """
    Extract latest date from influxDB

    args:
    ---

    returns:
    ---
    Tables -- object with infludbtimestamps
    """
    # inflxudb query to get the timestamp of the last input
    query = (
        f'from(bucket: "{influxdb_dict.get("bucket")}")'
        "|> range(start: 0, stop: now())"
        f'|> filter(fn: (r) => r["_measurement"] == "{influxdb_dict.get("measurement_name")}")'
        '|> keep(columns: ["_time"])'
        '|> sort(columns: ["_time"], desc: false)'
        '|> last(column: "_time")'
    )

    client = ifdb.InfluxDBClient(
        url=influxdb_dict.get("url"),
        token=influxdb_dict.get("token"),
        org=influxdb_dict.get("organization"),
    )
    try:
        tables = client.query_api().query(query=query)
    except NewConnectionError:
        logging.warning(f"Couldn't connect to database at {influxdb_dict.get('url')}")
        last_ts = None
        return last_ts
    try:
        last_ts = tables[0].records[0]["_time"].replace(tzinfo=None)
    except IndexError:
        logging.warning(
            "Couldn't get timestamp from influxdb, using season_start from .ini"
        )
        # if there's no timestamp, return None
        last_ts = None

    return last_ts
