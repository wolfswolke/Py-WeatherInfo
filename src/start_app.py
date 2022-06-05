"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
import os

from collections import deque
from time import sleep

from gutils.config_handle import config
from gutils.logging_handle import logger

from influx_uploader import InfluxHandle
from weather_stats import get_weather_data

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
MODULE_LOGGER_HEAD = "start_app -> "
APP_VERSION = "v01-00-00"

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #


# --------------------------------------- #
#              functions                  #
# --------------------------------------- #
def setup_logging(level):
    logger.set_logging_level(level)
    logger.set_cmd_line_logging_output()


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
if __name__ == "__main__":
    config.load_config("../config/speed_monitor.yml")
    config.set_element("general", "version", APP_VERSION)

    setup_logging(config.get_element("general", "debug_level"))

    logger.info("-----------------------------------------------------------")
    logger.info("            Started Weather To SQL {}".format(APP_VERSION))
    logger.info("-----------------------------------------------------------")

    upload_queue = deque(maxlen=10000)

    influx_db = InfluxHandle()
    influx_db.connect_influx_db(config.get_element("influx", "connection_string"),
                                config.get_element("influx", "token"))
    influx_db.add_section("weather_data", config.get_element("influx", "org"), upload_queue)
    influx_db.start_influx_upload()

    try:
        while True:
            logger.debug(MODULE_LOGGER_HEAD + "going to update stats")
            upload_queue.extend(get_weather_data())
            logger.debug(MODULE_LOGGER_HEAD + "finished updating stats")
            sleep(config.get_element("monitor", "check_cycle_in_minutes") * 60)

    except KeyboardInterrupt:
        influx_db.stop_influx_upload()
        logger.info("-----------------------------------------------------------")
        logger.info("            SpeedMonitor Stopped")
        logger.info("-----------------------------------------------------------")
