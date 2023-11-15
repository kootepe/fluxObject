import influxdb_client as ifdb
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from urllib3.exceptions import NewConnectionError

logging = logging.getLogger("__main__")


def influx_push(df, influxdb_dict, tag_columns):
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
        url=influxdb_dict.get("url"),
        token=influxdb_dict.get("token"),
        org=influxdb_dict.get("organization"),
    ) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        try:
            write_api.write(
                bucket=influxdb_dict.get("bucket"),
                record=df,
                data_frame_measurement_name=influxdb_dict.get("measurement_name"),
                data_frame_timestamp_timezone=influxdb_dict.get("timezone"),
                data_frame_tag_columns=tag_columns,
                debug=True,
            )
        except NewConnectionError:
            logging.info(f"Couldn't connect to database at " f"{influxdb_dict.get('url')}")
            pass

    logging.info("Pushed data to DB")


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
        '|> range(start: 0, stop: now())'
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
        logging.info(f"Couldn't connect to database at " f"{influxdb_dict.get('url')}")
        last_ts = None
        return last_ts
    try:
        last_ts = tables[0].records[0]["_time"].replace(tzinfo=None)
    except IndexError:
        # if there's no timestamp, return None
        last_ts = None

    return last_ts
