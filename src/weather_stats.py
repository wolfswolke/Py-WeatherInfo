"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
from datetime import datetime
from pyowm.owm import OWM
import pyodbc

from gutils.logging_handle import logger

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
MODULE_LOGGER_HEAD = "weather_stats -> "

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #


# --------------------------------------- #
#              functions                  #
# --------------------------------------- #
def get_weather_data():

    owm = OWM('813afd14001b1403a2598bc54a94ca7c')
    mgr = owm.weather_manager()
    one_call = mgr.one_call(lat=47.149719, lon=9.81667)
    observation = mgr.weather_at_place('Bludenz, AT')
    wind_dict_in_meters_per_sec = observation.weather.wind()
    temperature_id = 0

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=ZKWOLF-LAPTOP-A\TESTDB1;DATABASE=Test_DB'
        ';Trusted_connection=yes')
    cursor = cnxn.cursor()
    temperature = list(one_call.current.temperature('celsius').items())
    temperature_str = str(temperature)
    temperature_sliced = temperature_str[9:14]
    temperature_sliced = temperature_sliced.removeprefix(" ")
    temperature_sliced = temperature_sliced.removesuffix(")")
    temperature_sliced = float(temperature_sliced)
    wind_speed = wind_dict_in_meters_per_sec['speed']
    if one_call.current.humidity:
        humidity = float(one_call.current.humidity)
    else:
        humidity = 0.0
    cursor.execute("insert into weather_data (location, temperature, humidity) values (?, ?, ?)",
                   'bludenz', temperature_sliced, one_call.current.humidity)

    if wind_speed > 1.5:
        logger.debug(MODULE_LOGGER_HEAD + "Wind is fast! Over 1.5 m/s!")
        cursor.execute("SELECT IDENT_CURRENT('weather_data')")
        temperature_id = str(cursor.fetchall())
        temperature_id2 = temperature_id.removeprefix('[(Decimal(')
        temperature_id3 = temperature_id2.removesuffix('), )]')
        temperature_id4 = temperature_id3.replace("'", "")
        temperature_id = int(temperature_id4)
        humidity = float(one_call.current.humidity)
        cursor.execute("insert into weather_data_wind (wind, temperature_id) values (?, ?)", wind_speed, temperature_id)

    logger.debug(MODULE_LOGGER_HEAD + "Current Temps: {:.2f}".format(temperature_sliced))
    logger.debug(MODULE_LOGGER_HEAD + "Current Humidity: {:.2f}".format(humidity))
    logger.debug(MODULE_LOGGER_HEAD + "Current Wind: {:.2f}".format(wind_speed))
    logger.debug(MODULE_LOGGER_HEAD + "Sent data to SQL!")

    cnxn.commit()
    cnxn.close()

    return [{"name": "temperature", "time": datetime.utcnow(), "value": temperature_sliced, "type": "real"},
            {"name": "humidity", "time": datetime.utcnow(), "value": humidity, "type": "real"},
            {"name": "wind", "time": datetime.utcnow(), "value": wind_speed, "type": "real"}]

# --------------------------------------- #
#               classes                   #
# --------------------------------------- #


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
if __name__ == "__main__":
    print(get_weather_data())
