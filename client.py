from lib.mqtt import query_influxdb, migrateSeries
import csv
import pandas as pd
from influxdb import InfluxDBClient

# unixWeek = 604800000
# fromUnixTime = 1602967684458
#
# notnow = True
# i=0
# while notnow:
#     currentUnixStart = fromUnixTime + i*unixWeek
#     query = f"SELECT \"State\" FROM \"train\" WHERE time >= {currentUnixStart}ms and time <= {currentUnixStart+unixWeek}ms GROUP BY \"sensor_id\""
#     result = query_influxdb("db1-dev.cemit.digital", "cargo_net", query, True).raw
#     notnow =  currentUnixStart + unixWeek < 1607009843000
#     for serie in result['series']:
#         df = pd.DataFrame(serie['values'])
#         df.to_csv(r'reformatted.csv')
#         json_body = []
#         with open('reformatted.csv') as csv_file:
#             csv_reader = csv.reader(csv_file, delimiter=',')
#             line_count = 0
#             for row in csv_reader:
#                 if line_count == 0:
#                     print(f'Column names are {", ".join(row)}')
#                     line_count += 1
#                 else:
#                     if not row[1].isdigit():
#                         print(f'time: {row[1]}, state: {float(row[2])}.')
#                         line_count += 1
#                         json_body.append(
#                             {
#                                 "measurement": "train",
#                                 "tags": {
#                                     "sensor_id": "2010-16::BRK-2-X"
#                                 },
#                                 "time": row[1],
#                                 "fields": {
#                                     "Bool": float(row[2])
#                                 }
#                             }
#                         )
#             print(f'Processed {line_count} lines.')
#         client = InfluxDBClient("127.0.0.1", 8086, 'cemit', '3rn3DZKreAQK7AJc', 'cargo_net')
#         client.write_points(json_body)
#     i+=1
# print(i)

# host_from = "db1.cemit.digital"
# ssl_from = True
# database_from = "cargo_net"
# measurement_from = "train"
# tag_from = "sensor_id"
# field_from = "State"
# host_to = "db1.cemit.digital"
# ssl_to = True
# database_to = "cargo_net"
# measurement_to = "train"
# tags_to = {"sensor_id": "2010-16::BRK-2-X"}
# field_to = "Bool"
# conversion = lambda x: float(x)
# migrateSeries(host_from=host_from,
#               ssl_from=ssl_from,
#               database_from=database_from,
#               measurement_from=measurement_from,
#               tag_from=tag_from,
#               field_from=field_from,
#               host_to=host_to,
#               ssl_to=ssl_to,
#               database_to=database_to,
#               measurement_to=measurement_to,
#               tags_to=tags_to,
#               field_to=field_to,
#               conversion=conversion)