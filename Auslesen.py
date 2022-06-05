import pyodbc
import argparse

# Create SQL query
parser = argparse.ArgumentParser()
parser.add_argument("--time", nargs=2)
args = vars(parser.parse_args())
args = {k: v for k, v in args.items() if v is not None}
statement = "SELECT temperature FROM weather_data"
operators = []
if "time" in args:
    operators.append("time BETWEEN {} AND {}".format(*args["time"]))
    del args["time"]
if operators:
    statement = statement + " WHERE " + " and ".join(operators)

# CODE
result_list = []

CONNECTION_STRING = """DRIVER={ODBC Driver 17 for SQL Server};SERVER=ZKWOLF-LAPTOP-A\TESTDB1;DATABASE=Test_DB;Trusted_connection=yes"""
cnxn = pyodbc.connect(CONNECTION_STRING)
if cnxn:
    print("Connection Online")
cursor = cnxn.cursor()
cursor.execute(statement)

columns = [column[0] for column in cursor.description]
results = [columns] + [row for row in cursor.fetchall()]

for result in results:
    if len(result) >= 1:
        result_list.append(str(result[0]))

del result_list[0]
new_list = list(map(lambda x: float(x.replace(",", "")), result_list))
avg = sum(new_list) / len(new_list)

print("Elements Calculated: ", len(new_list))
print("Average of these Values: ", round(avg, 2))
print("MAX of these Values: ", max(new_list))
print("MIN of these Values: ", min(new_list))
print("Connection Closed")
cnxn.close()

input("Press Enter to continue...")