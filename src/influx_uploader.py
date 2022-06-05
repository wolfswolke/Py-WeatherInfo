"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
import certifi

from gutils.logging_handle import logger

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, Point, WritePrecision

from threading import Thread, Event

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
MODULE_LOGGER_HEAD = "influx_uploader -> "

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #


# --------------------------------------- #
#              functions                  #
# --------------------------------------- #


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #
class DataSection:
    def __init__(self, bucket, organisation, queue_object):
        self._queue_object = queue_object
        self.bucket = bucket
        self.organisation = organisation

    def data_available(self):
        return True if len(self._queue_object) else False

    def prepare_upload_bucket_data(self):
        data_bucket = []

        while self._queue_object:
            data_point = self._queue_object.pop()

            data_bucket.append(Point("mem")\
                .tag("var_name", data_point["name"])\
                .field("value", data_point["value"] if data_point["type"] in ["bool", "real"] else 0.0) \
                .field("text", data_point["value"] if data_point["type"] in ["string"] else "") \
                .time(data_point["time"], WritePrecision.NS))

        return data_bucket


class InfluxHandle:
    UPLOAD_WORKER_CYCLE_IN_S = 10

    def __init__(self):
        self._upload_thread = None
        self._kill_handle = Event()
        self._data_sections = {}
        self._connection_string = ""
        self._token = ""
        self._client = None
        self._client_writer = None

    def connect_influx_db(self, connection_string, token):
        if self._client_writer:
            logger.error(MODULE_LOGGER_HEAD + "influx db already connected!")
        else:
            logger.info(MODULE_LOGGER_HEAD + "going to connect to {}".format(connection_string))
            self._connection_string = connection_string
            self._token = token
            try:
                if "https" in connection_string:
                    self._client = InfluxDBClient(url=self._connection_string, token=self._token, ssl_ca_cert=certifi.where())
                else:
                    self._client = InfluxDBClient(url=self._connection_string, token=self._token)
                self._client_writer = self._client.write_api(write_options=SYNCHRONOUS)
            except Exception as e:
                logger.error(MODULE_LOGGER_HEAD + "could not connect {} with reason {}".format(self._connection_string, e))

    def shutdown_handle(self):
        self._client.close()
        self._kill_handle.set()

    def reconnect_to_influx_db(self):
        try:
            self._client = InfluxDBClient(url=self._connection_string, token=self._token)
            self._client_writer = self._client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not connect {} with reason {}".format(self._connection_string, e))

    def start_influx_upload(self):
        if self._upload_thread is None:
            self._kill_handle = Event()
            self._upload_thread = Thread(target=self._upload_worker)
            self._upload_thread.start()
        elif not self._upload_thread.isAlive():
            self._kill_handle = Event()
            self._upload_thread = Thread(target=self._upload_worker)
            self._upload_thread.start()
        else:
            logger.error(MODULE_LOGGER_HEAD + "upload thread is already running")

    def stop_influx_upload(self):
        self._kill_handle.set()

    def add_section(self, bucket_name, organisation_name, data_queue):
        if bucket_name not in self._data_sections:
            logger.info(MODULE_LOGGER_HEAD + "going to add section {} to influx db".format(bucket_name))
            self._data_sections[bucket_name] = DataSection(bucket_name, organisation_name, data_queue)
        else:
            logger.error(MODULE_LOGGER_HEAD + "data section {} already added".format(bucket_name))

    def _upload_worker(self):
        while not self._kill_handle.is_set():
            if self._check_influx_alive():
                for section_name, data_section in self._data_sections.items():
                    if data_section.data_available():
                        logger.debug(MODULE_LOGGER_HEAD + "going to upload data from section {}".format(section_name))
                        self._client_writer.write(data_section.bucket,
                                                  data_section.organisation,
                                                  data_section.prepare_upload_bucket_data())

            else:
                self.reconnect_to_influx_db()

            self._kill_handle.wait(self.UPLOAD_WORKER_CYCLE_IN_S)

    def _check_influx_alive(self):
        try:
            self._client.ready()
            return True
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "influxDB no ready! {}".format(e))

        return False


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
