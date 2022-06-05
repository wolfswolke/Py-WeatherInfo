from pyowm.owm import OWM
import pyodbc
import time
from datetime import datetime

# Open Weather API einrichten und default werte setzen.
owm = OWM('813afd14001b1403a2598bc54a94ca7c')
mgr = owm.weather_manager()
one_call = mgr.one_call(lat=47.149719, lon=9.81667)
observation = mgr.weather_at_place('Bludenz, AT')
wind_dict_in_meters_per_sec = observation.weather.wind()
temperature_id = 0

try:
    while True:
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=ZKWOLF-LAPTOP-A\TESTDB1;DATABASE=Test_DB'
            ';Trusted_connection=yes')
        cursor = cnxn.cursor()
        temperature = list(one_call.current.temperature('celsius').items())
        temperature_str = str(temperature)
        temperature_sliced = temperature_str[9:14]
        wind_speed = wind_dict_in_meters_per_sec['speed']
        cursor.execute("insert into weather_data (location, temperature, humidity) values (?, ?, ?)",
                       'bludenz', temperature_sliced, one_call.current.humidity)
        if wind_speed > 1.5:
            print("Wind is Fast! Going over 1.5 m/s!")
            cursor.execute("SELECT IDENT_CURRENT('weather_data')")
            temperature_id = str(cursor.fetchall())
            temperature_id2 = temperature_id.removeprefix('[(Decimal(')
            temperature_id3 = temperature_id2.removesuffix('), )]')
            temperature_id4 = temperature_id3.replace("'", "")
            temperature_id = int(temperature_id4)
            cursor.execute("insert into weather_data_wind (wind, temperature_id) values (?, ?)", wind_speed, temperature_id)

        print('Current Time:')
        print(datetime.now())
        print("Current temperature:")
        print(temperature_sliced)
        print("Current humidity:")
        print(one_call.current.humidity)
        print("Current wind speed in m/s:")
        print(wind_speed)
        print("Data has been sent to SQL Server.")
        print()
        print()
        print()

        cnxn.commit()
        cnxn.close()

        time.sleep(60)
except KeyboardInterrupt:
    print('Keyboard Interrupt')

# Find Lat and Lon for Bludenz.
# reg = owm.city_id_registry()
# list_of_locations = reg.locations_for('bludenz', country='AT')
# bludenz = list_of_locations[0]
# lat = bludenz.lat
# lon = bludenz.lon
# print(lat)
# print(lon)
