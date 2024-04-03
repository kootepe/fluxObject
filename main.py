import os
import sys
import timeit
import functools
import importlib
import configparser

from dotenv import dotenv_values
from pathlib import Path

from tools.fluxer import gas_flux_calculator
from tools.time_funcs import convert_seconds
from tools.logger import init_logger


def timer(func):
    """Decorator for printing execution time of function."""
    logger = init_logger()

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = timeit.default_timer()
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        execution_time = stop - start
        logger.info(
            f"{func.__name__} executed in {convert_seconds(execution_time)}."
        )
        return value

    return wrapper_timer


def list_inis(ini_path):
    """
    List files ending in .ini in given directory
    """
    folder = Path(ini_path)
    files = [file for file in folder.glob("*.ini") if file.is_file()]

    return files


@timer
def class_calc(inifile, env_vars):
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)

    defs = dict(config.items("defaults"))
    module = defs.get("module")
    class_name = defs.get("class_name")
    measurement_name = defs.get("measurement_name")

    if module:
        module = importlib.import_module(module)
    if class_name:
        instrument_class = getattr(module, class_name)
    if measurement_name:
        measurement_class = getattr(module, measurement_name)

    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)
    # gas_flux_calculator(inifile, env_vars, instrument_class, measurement_class)
    # gas_flux_calculator(inifile, env_vars, instrument_class)
    data = gas_flux_calculator(inifile, env_vars)
    return data


def main(ini_path):
    ini_files = list_inis(ini_path)
    # get environment variables
    env_vars = os.environ
    # python does not like % signs, remove them from keys and values
    filtered_env = {
        key: value
        for key, value in env_vars.items()
        if "%" not in key and "%" not in value
    }

    # Update os.environ with the filtered dictionary
    env_vars.clear()
    env_vars.update(filtered_env)
    logger = init_logger()

    for inifile in ini_files:
        logger.debug(f"Reading ini: {inifile}")
        config = configparser.ConfigParser(env_vars, allow_no_value=True)
        config.read(inifile)
        try:
            active = config.getboolean("defaults", "active")
        except configparser.NoSectionError:
            logger.debug(f"Skipped {inifile}, no defaults section.")
            continue
        if active:
            use_dotenv = dict(config.items("defaults")).get("use_dotenv")
            if use_dotenv == "1":
                # get environment variables from dotenv
                env_vars = dotenv_values()
                # pass env_vars to parser and reread .ini
                config = configparser.ConfigParser(
                    env_vars, allow_no_value=True
                )
                config.read(inifile)
            mode = dict(config.items("defaults")).get("mode")
            logger.info(f"Running {inifile}.")
            # if mode == "ac":
            #     ac_push(inifile, env_vars)
            # if mode == "man":
            #     man_push(inifile, env_vars)
            # if mode == "csv":
            #     csv_push(inifile, env_vars)
            # if mode == "eddypro":
            #     eddypro_push(inifile, env_vars)
            # if mode == "class":
            class_calc(inifile, env_vars)
        else:
            logger.info(f"Active set 0, skipped {inifile}")


if __name__ == "__main__":
    # NOTE: Need to use try except blocks in functions to prevent
    # crashes since we are now looping through files in a folder,
    # if one .ini crashes, all the ones after
    ini_path = sys.argv[1]
    # mode = sys.argv[2]
    main(ini_path)
