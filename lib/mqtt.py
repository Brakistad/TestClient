import json
import isoweek
import maya as maya
import paho.mqtt.client as mqtt
import time as t
from influxdb import InfluxDBClient
import random as rand
import numpy as np
import pandas
from datetime import datetime, timedelta
from paho.mqtt.client import Client
import matplotlib.pyplot as plt
import csv


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(("test", 2))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = str(msg.payload.decode('utf-8'))
    qos = msg.qos
    retain = msg.retain
    topic = msg.topic


def line(_x, _a, _b):
    return _x * _a + _b


def total_ms_now():
    return int(t.time() * 1000)


def gen_data(n, field):
    if field == "gps":
        breviksbanen = [
            "[59.063496, 9.688832]",
            "[59.064116, 9.689688]",
            "[59.065076, 9.689794]",
            "[59.069918, 9.687934]",
            "[59.072009, 9.688212]",
            "[59.075388, 9.688887]",
            "[59.076794, 9.691155]",
            "[59.077544, 9.692953]",
            "[59.079202, 9.694451]",
            "[59.080472, 9.697031]",
            "[59.081187, 9.697524]",
            "[59.091594, 9.692269]",
            "[59.097757, 9.685446]",
            "[59.101612, 9.688964]",
            "[59.103712, 9.695014]",
            "[59.105702, 9.696470]",
            "[59.108464, 9.696414]",
            "[59.113491, 9.700246]",
            "[59.118577, 9.705813]",
            "[59.124413, 9.700805]",
            "[59.121383, 9.690374]",
            "[59.122383, 9.686183]",
            "[59.125672, 9.680783]",
            "[59.126202, 9.677620]",
            "[59.127296, 9.675328]"]
        result = breviksbanen
    else:
        x = np.linspace(0, n, num=n + 1, endpoint=True)
        f_line_rand = line(x, rand.gauss(0.0, 1.0), rand.gauss(0.0, 30.0))
        result = f_line_rand
    return result


# json_body = [
#         {
#             "measurement": "cpu_load_short",
#             "tags": {
#                 "host": "server01",
#                 "region": "us-west"
#             },
#             "time": "2009-11-10T23:00:00Z",
#             "fields": {
#                 "Float_value": 0.64,
#                 "Int_value": 3,
#                 "String_value": "Text",
#                 "Bool_value": True
#             }
#         }
#     ]
def transmit_gen_data(client, database, field, data, time):
    milliseconds_in_day = 3600 * 24 * 1000
    topic = "POINT/PUSH"
    datalist = []
    if field == "temperature":
        sensor_ids = [
            "wheel1_temp1",
            "wheel1_temp2",
            "wheel1_temp3",
            "wheel1_temp4",
            "wheel1_temp5",
            "wheel1_temp6",
            "wheel1_temp7",
            "wheel1_temp8",
            "wheel1_temp_ambient",
            "wheel2_temp1",
            "wheel2_temp2",
            "wheel2_temp3",
            "wheel2_temp4",
            "wheel2_temp5",
            "wheel2_temp6",
            "wheel2_temp7",
            "wheel2_temp8",
            "wheel2_temp_ambient"

        ]
    elif field == "gps":
        sensor_ids = ["gps"]
    else:
        sensor_ids = ["sense_imu",
                      "wheel_imu"]
    time = datetime.utcnow()
    data = {
        "measurement": "train",
        "tags": {
            "sensor_id": "wheeltemp1"
        },
        "time": "%s" % (time.isoformat() + "Z"),
        "fields": {
            "temperature": 0.0
        }
    }
    for sensor_id in sensor_ids:
        i = 0
        y = gen_data(10, field)
        data["tags"].update({
            "sensor_id": sensor_id
        })
        for y_i in y:
            data["fields"].update({
                field: y_i
            })
            data["time"] = "%s" % (time.isoformat() + "Z")
            datalist.append(data)
    payload = json.dumps({"database": database, "data": datalist})
    client.publish(topic, payload, 2)


def transmit(client):
    time = datetime.utcnow()
    database = "testcargo"
    topic = "POINT/PUSH"
    fields = ["Temperature",
              "gps",
              "accx",
              "accy",
              "accz"]
    for field in fields:
        data = {
            "measurement": "train",
            "tags": {},
            "time": "%s" % (time.isoformat() + "Z"),
            "fields": {}
        }
        transmit_gen_data(client, database, field, data, time)


def push_test_data_to_mqtt():
    client = mqtt.Client()
    client.username_pw_set("cemit", "SuperPassord!")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("13.69.250.61", 1883)
    print("Mqtt connected")

    client.loop_start()
    t.sleep(1)
    transmit(client)
    t.sleep(1)
    client.loop_stop()
    client.disconnect()
    t.sleep(4)


# def get_wheel_data_from_week(week, seconds_per_sample):
#     """
#     Gets wheel temperature data within a given week, then calls a group function in order to group the data according to cargonet specs
#     :param week:
#     :return:
#     """
#     influx_client = InfluxDBClient('127.0.0.1', port=8086, database='cargo_net', username='cemit',
#                                    password='3rn3DZKreAQK7AJc')
#     w_stop = isoweek.Week(week.year, week.week + 1)
#     start = int(t.mktime(week.monday().timetuple()) * 1000)
#     # start = 1601006430705
#     stop = int(t.mktime(w_stop.monday().timetuple()) * 1000)
#     print("Start: %s, Stop: %s" % (start, stop))
#     # stop = 1601014100769
#     query = f"SELECT \"Temperature\" FROM \"train\" WHERE time >= {start}ms and time <= {stop}ms GROUP " \
#             f"BY \"sensor_id\" "
#     try:
#         result = influx_client.query(query)
#     except:
#         raise Exception("could not send request to database")
#     series = {}
#     for i in result.raw['series']:
#         id = i['tags']['sensor_id']
#         time, value = map(list, zip(*i['values']))
#         series.update(
#             {'%s' % id: {'time': time, 'value': value}})
#     groups = [("wheel_Ch01", "wheel_Ch06"),
#               ("wheel_Ch02", "wheel_Ch07"),
#               ("wheel_Ch03", "wheel_Ch09"),
#               ("wheel_Ch08",)]
#     final = group(groups, series, seconds_per_sample)
#     return pandas.DataFrame(final)
#
#
# def group(groups, series, seconds_per_sample):
#     final = {}
#     for group in groups:
#         length = len(group)
#         if length == 2:
#             time, value = mean_for_groups(series[group[0]], series[group[1]], seconds_per_sample)
#             final.update({f"{group[0] + group[1]}": {"time": time, "value": value}})
#         else:
#             final.update({f"{group[0]}": {"time": series[group[0]]['time'], "value": series[group[0]]['value']}})
#     return final
#
#
# def mean_for_groups(serie1, serie2, period):
#     time = []
#     value = []
#     i2 = 0
#     mean_n = 0.0
#     mean_flag = False
#     new_mean = True
#     if maya.parse(serie1['time'][0]).datetime() > maya.parse(serie2['time'][0]).datetime():
#         s1 = serie2
#         s2 = serie1
#     else:
#         s1 = serie1
#         s2 = serie2
#     for timestamp1, val in zip(s1['time'], s1['value']):
#         if is_greater(timestamp1, s2['time'][i2]) and within_range(period, timestamp1, s2['time'][i2]):
#             time.append(timestamp1)
#             value.append((val + s2['value'][i2]) / 2.0)
#             i2 += 1
#         elif is_greater(timestamp1, s2['time'][i2]):
#             while is_greater(timestamp1, s2['time'][i2]) and not within_range(period, timestamp1, s2['time'][i2]):
#                 value.append(s2['value'][i2])
#                 time.append(s2['time'][i2])
#                 i2 += 1
#             time.append(timestamp1)
#             value.append((val + s2['value'][i2]) / 2.0)
#             i2 += 1
#         else:
#             time.append(timestamp1)
#             value.append(val)
#     return time, value
#
#
# def is_greater(t1, t2):
#     time1 = maya.parse(t1).datetime()
#     time2 = maya.parse(t2).datetime()
#     return time2 < time1
#
#
# def within_range(period, t1, t2):
#     time1 = maya.parse(t1).datetime()
#     time2 = maya.parse(t2).datetime()
#     return time2 - timedelta(seconds=period / 2.0) < time1 > time2 + timedelta(seconds=period / 2.0)


def connect_influxdb(host='127.0.0.1', db='testcargo', from_time="1d", to_time="now()", field="Temperature"):
    print("connecting to influxdb")
    influx_client = InfluxDBClient(host, port=8086, database=db, username='cemit',
                                   password='3rn3DZKreAQK7AJc', ssl=True, verify_ssl=True)

    print("connected to influxdb")
    query = f"SELECT \"{field}\" FROM \"train\" WHERE time >= now()-{from_time} AND time <= {to_time} GROUP BY \"sensor_id\""
    print("sending query")
    result = influx_client.query(query)
    print("query sent")
    return result

def query_influxdb(host='127.0.0.1', db='testcargo', query=f"SELECT \"Temperature\" FROM \"train\" WHERE time >= now()-14d AND time <= now() GROUP BY \"sensor_id\"", ssl = True):
    print("connecting to influxdb")
    influx_client = InfluxDBClient(host, port=8086, database=db, username='cargonet',
                                   password='CargoNetSuperKul!!2021', ssl=ssl)

    print("connected to influxdb")
    print("sending query")
    result = influx_client.query(query)
    print("query sent")
    return result

def migrateSeries(host_from,
                  ssl_from,
                  database_from,
                  measurement_from,
                  tag_from,
                  field_from,
                  host_to,
                  ssl_to,
                  database_to,
                  measurement_to,
                  tags_to,
                  field_to,
                  conversion):

    import pandas as pd

    unixWeek = 604800000
    fromUnixTime = 1602967684458

    notnow = True
    i = 0
    while notnow:
        currentUnixStart = fromUnixTime + i * unixWeek
        query = f"SELECT \"{field_from}\" FROM \"{measurement_from}\" WHERE time >= {currentUnixStart}ms and time <= {currentUnixStart + unixWeek}ms GROUP BY \"{tag_from}\""
        result = query_influxdb(host_from, database_from, query, ssl_from).raw
        notnow = currentUnixStart + unixWeek < 1607009843000
        for serie in result['series']:
            df = pd.DataFrame(serie['values'])
            df.to_csv(r'reformatted.csv')
            json_body = []
            with open('reformatted.csv') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        print(f'Column names are {", ".join(row)}')
                        line_count += 1
                    else:
                        if not row[1].isdigit():
                            print(f'time: {row[1]}, state: {float(row[2])}.')
                            line_count += 1
                            json_body.append(
                                {
                                    "measurement": measurement_to,
                                    "tags": tags_to,
                                    "time": row[1],
                                    "fields": {
                                        field_to: conversion(row[2])
                                    }
                                }
                            )
                print(f'Processed {line_count} lines.')
            client = InfluxDBClient(host_to, 8086, 'cemit', '3rn3DZKreAQK7AJc', database_to, ssl=ssl_to)
            client.write_points(json_body)
        i += 1
    print(i)
# '2010-16::AX4-E02-T': line['Ch02 (°C)'],
# '2010-16::AX4-E03-T': line['Ch03 (°C)'],
# '2010-16::AX4-E04-T': line['Ch04 (°C)'],
# '2010-16::AX4-E05-T': line['Ch05 (°C)'],
# '2010-16::AX4-E06-T': line['Ch06 (°C)'],
# '2010-16::AX4-E07-T': line['Ch07 (°C)'],
# '2010-16::AX4-E08-T': line['Ch08 (°C)'],
# '2010-16::AX4-E09-T': line['Ch09 (°C)']
def fetch_csv(filename='data'):
    reader = csv.DictReader(open(filename + '.csv', 'rt'))
    dict_list = []
    print('Loading csv ...')
    for line in reader:
        dict_list.extend([
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E01-T'
                },
                'fields': {
                    'Temperature': line['Ch01 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E02-T'
                },
                'fields': {
                    'Temperature': line['Ch02 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E03-T'
                },
                'fields': {
                    'Temperature': line['Ch03 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E04-T'
                },
                'fields': {
                    'Temperature': line['Ch04 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E05-T'
                },
                'fields': {
                    'Temperature': line['Ch05 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E06-T'
                },
                'fields': {
                    'Temperature': line['Ch06 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E07-T'
                },
                'fields': {
                    'Temperature': line['Ch07 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E08-T'
                },
                'fields': {
                    'Temperature': line['Ch08 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            },
            {
                'measurement': 'train',
                'tags': {
                    'sensor_id': '2010-16::AX4-E09-T'
                },
                'fields': {
                    'Temperature': line['Ch09 (°C)']
                },
                'time': datetime.strptime(line['Reading Timestamp (UTC)'], '%m/%d/%Y %I:%M:%S %p').isoformat() + 'Z'
            }
        ])
    print('Finished loading csv')
    return dict_list


# def parse_influxdb(host, db, msgs):
#     print('Connecting to influxdb ...')
#     influx_client = InfluxDBClient(host, port=8086, database=db, username='cemit',
#                                    password='3rn3DZKreAQK7AJc', ssl=True, verify_ssl=True)
#     print('Connection successful!')
#     print('Deleting database ' + db + ' ...')
#     influx_client.drop_database(db)
#     print('Deleted database ' + db + '!')
#     print('Creating database ' + db + ' ...')
#     influx_client.create_database(db)
#     print('Created database ' + db + '!')
#     print('Writing loaded csv series to database ...')
#     yolo = []
#     for msg in msgs:
#         if msg['fields']['Temperature'] == '<SensorError>' or msg['fields']['Temperature'] == '<OutOfRangeHighError>' or \
#                 msg['fields']['Temperature'] == '<OutOfRangeError>' or msg['fields'][
#             'Temperature'] == '<OutOfRangeLowError>':
#             pass
#         else:
#             msg['fields']['Temperature'] = float(msg['fields']['Temperature'])
#             yolo.append(msg)
#     influx_client.write_points(yolo, time_precision='ms')
#     print('Finished writing!')


def test_traffic_mqtt():
    client = Client(client_id="test")
    client.on_connect = on_connect
    client.username_pw_set(username="cemit", password="SuperPassord!")
    client.connect(host="52.164.202.250", port=1883)
    client.loop_start()
    t.sleep(5)

    t.sleep(2)
    client.disconnect()
    t.sleep(2)


def sort_influx_result_to_mqtt(result):
    packet = {'database': 'testcargo', 'data': []}
    for serie in result['series']:
        for value in serie['values']:
            point = {
                'measurement': 'train',
                'tags': {
                    'sensor_id': serie['tags']['sensor_id']
                },
                'fields': {
                    serie['columns'][1]: value[1]
                },
                'time': value[0]
            }
            packet['data'].append(point)
    return packet
def send_to_mqtt(packet, db = "testcargo"):
    packet['database'] = db
    client = Client(client_id="test")
    client.on_connect = on_connect
    client.username_pw_set(username="cemit", password="SuperPassord!")
    client.connect(host="52.164.202.250", port=1883)
    client.loop_start()
    t.sleep(5)
    transmit(client)
    t.sleep(2)
    client.disconnect()
    t.sleep(2)
