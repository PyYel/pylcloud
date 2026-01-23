import os, sys
from typing import Optional
import logging
from datetime import datetime

__all__ = [
    "_config_logger",
]


def _config_logger(
    logs_name: str,
    logs_dir: Optional[str] = None,
    logs_level: Optional[str] = None,
    logs_output: Optional[str] = None,
):
    """
    Configures a standardized logger for ``Database`` modules. Environement configuration is recommended.

    Parameters
    ----------
    logs_name: str
        The name of the logger
    logs_dir: Optional[str]
        The output root folder when 'file' in ``logs_output``. Subfolders will be created from there.
    logs_level: str
        The level of details to track. Should be configured using the ``LOGS_LEVEL`` environment variable.
        ``LOGS_LEVEL <= WARNING`` is recommended.
    logs_output: str
        The output method, whereas printing to console, file, or both.
    """

    def _create_logs_dir(logs_dir: str):
        os.makedirs(logs_dir, exist_ok=True)
        with open(os.path.join(logs_dir, ".gitignore"), "w") as f:
            f.write("*")

    # Must be a valid log level alias
    if logs_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        logs_level = os.getenv("LOGS_LEVEL", "INFO")

    # If kwargs is None
    if logs_dir is None:
        logs_dir = os.getenv("LOGS_DIR", None)
    # If env is None
    if logs_dir is None:
        logs_dir = os.path.join(
            os.getcwd(), "logs", str(datetime.now().strftime("%Y-%m-%d"))
        )
    else:
        logs_dir = os.path.join(logs_dir, str(datetime.now().strftime("%Y-%m-%d")))

    if logs_output is None:
        logs_output = os.getenv("LOGS_OUTPUT", None)
    if logs_output is None:
        logs_output = "console"
    else:
        logs_output = logs_output.lower()
        if "file" in logs_output:
            _create_logs_dir(logs_dir=logs_dir)

    logger = logging.getLogger(logs_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # If a logger already exists, this prevents duplication of the logger handlers
    if logger.hasHandlers():
        for handler in logger.handlers:
            handler.close()

    # Creates/recreates the handler(s)
    if not logger.hasHandlers():

        if "console" in logs_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging._nameToLevel[logs_level])
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            logger.debug(
                f"Logging handler configured for console output, set to level '{logs_level}'."
            )

        if "file" in logs_output:
            file_handler = logging.FileHandler(
                os.path.join(logs_dir, f"{datetime.now().strftime('%H-%M-%S')}.log")
            )
            file_handler.setLevel(logging._nameToLevel[logs_level])
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.debug(
                f"Logging handler configured for file output, set to level '{logs_level}'."
            )

    return logger
