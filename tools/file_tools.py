from pathlib import Path
import logging
import os
import sys

logging = logging.getLogger("__main__")


def get_newest(path: str, file_extension: str):
    """
    Fetchest name of the newest file in a folder

    args:
    ---

    returns:
    ---
    newest_file -- str
        Name of the newest file in a folder

    """
    files = list(Path(path).rglob(f'*{file_extension}*'))
    if not files:
        logging.info(f'No files found in {path}')
        logging.warning('EXITING')
        sys.exit(0)

    # linux only
    # newest_file = str(max([f for f in files], key=lambda item: item.stat().st_ctime))
    # cross platform
    newest_file = str(max(files, key=os.path.getmtime))
    return newest_file
