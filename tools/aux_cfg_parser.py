#!/usr/bin/env python3

from pathlib import Path
import logging

logger = logging.getLogger("defaultLogger")


def parse_aux_cfg(main_cfg):
    """
    Creates a list of dictionaries out of .ini sections with have
    "aux_data_" in them.
    """
    # list all config sections with aux_data_ in them
    cfg_names = [s for s in main_cfg.sections() if "aux_data_" in s]
    # config sections to dictionaries
    aux_cfg_sects = [dict(main_cfg.items(c)) for c in cfg_names]
    logger.info(f"Attempting to parse {len(aux_cfg_sects)} aux configs.")
    influxdb_dict = dict(main_cfg.items("influxDB"))
    # initiate list for the parsed configs
    aux_cfgs = []
    for cfg in aux_cfg_sects:
        data_type = cfg.get("type")
        data_name = cfg.get("name")
        if data_type != "file" and data_type != "db":
            logger.info(
                f"Aux cfg {cfg.get('name')} doesn't have proper type for merging, type must be 'db' or 'file'"
            )
            continue
        logger.info(f"Attempting to parse {data_name} cfg with type {data_type}")
        if data_type == "file":
            new_dict = parse_file_cfg(cfg)
            if new_dict:
                aux_cfgs.append(new_dict)
            else:
                continue
        if data_type == "db":
            new_db_dict = parse_db_cfg(cfg, influxdb_dict)
            if new_db_dict:
                aux_cfgs.append(new_db_dict)
            else:
                continue

    logger.debug(f"Parsed {len(aux_cfgs)} aux cfgs")
    return aux_cfgs


def parse_db_cfg(cfg, ifdb_cfg):
    new_cfg = {**ifdb_cfg, **cfg}
    return new_cfg


def parse_file_cfg(cfg):
    name_key = cfg["name"]
    path = Path(cfg.get("path"))
    # NOTE: rglob is used so multiple files can be matched, needs regex
    # implementation...
    files = list(path.rglob(cfg.get("file_name")))
    if len(files) == 0:
        logger.debug(f"No files found for aux_data {name_key}, skipped.")
        return None
    merge_method = cfg.get("merge_method")
    direction = cfg.get("direction")
    tolerance = cfg.get("tolerance")
    # possible values in the .ini that we don't want passed to pandas
    # read_csv
    excluded = [
        "name",
        "path",
        "file_name",
        "merge_method",
        "direction",
        "tolerance",
        "type",
        "id_col",
    ]

    # create dict with pandas read_csv compatible args
    pd_args = {k: v for k, v in cfg.items() if k not in excluded}
    pd_args = parse_read_csv_args(pd_args)

    new_dict = {
        "name": name_key,
        "merge_method": merge_method,
        "files": files,
        "args": pd_args,
        "direction": direction,
        "tolerance": tolerance,
    }
    return new_dict


def parse_read_csv_args(args_dict):
    # Define converters for specific arguments known to require non-string types
    converters = {
        "header": lambda x: (
            int(x) if x.isdigit() else None if x.lower() == "none" else x
        ),
        "index_col": lambda x: (
            int(x) if x.isdigit() else None if x.lower() == "none" else x
        ),
        "usecols": lambda x: [
            int(item) if item.isdigit() else item for item in x.split(",")
        ],
        "skiprows": parse_skiprows,
        "parse_dates": lambda x: [item for item in x.split(",")],
        "names": lambda x: [
            int(item) if item.isdigit() else item for item in x.split(",")
        ],
        "na_values": lambda x: x.split(",") if "," in x else x,
    }

    parsed_args = {}
    for key, value in args_dict.items():
        if key in converters:
            try:
                # Apply the converter if one is defined for this key
                parsed_args[key] = converters[key](value)
            except ValueError:
                logger.debug(
                    f"Warning: Could not convert argument { key} with value {value}"
                )
        else:
            # Copy over any arguments without a specific converter
            parsed_args[key] = value

    return parsed_args


def parse_skiprows(skiprows):
    def parser(x):
        return [int(item) if item.isdigit() else item for item in x.split(",")]

    value = parser(skiprows)
    if len(value) == 1:
        value = value.pop()
    return value
