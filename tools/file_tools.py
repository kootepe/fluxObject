from pathlib import Path
import logging
import os
import sys

logger = logging.getLogger("defaultLogger")


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
    logger.info(f"Getting first timestamp from {path}")
    files = list(Path(path).rglob(f"*{file_extension}*"))
    if not files:
        logger.info(f"No files found in {path}")
        logger.warning("EXITING")
        # BUG: NEED A BETTER WAY OF EXITING THE FUNCTION BECAUSE THIS
        # STOPS THE LOOPING THROUGH FILES
        sys.exit(0)

    # linux only
    # newest_file = str(max([f for f in files], key=lambda item: item.stat().st_ctime))
    # cross platform
    newest_file = str(max(files, key=os.path.getmtime))
    return newest_file
