import json
import paho.mqtt.client as mqtt
import time as t
import random as rand
import numpy as np
import datetime


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
    time = datetime.datetime.utcnow()
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
            payload = json.dumps({"database": database, "data": data})
            client.publish(topic, payload, 2)
            t.sleep(0.2)


def transmit(client):
    time = datetime.datetime.utcnow()
    database = "testcargo"
    topic = "POINT/PUSH"
    fields = ["temperature",
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


client = mqtt.Client()
client.username_pw_set("cemit", "SuperPassord!")
client.on_connect = on_connect
client.on_message = on_message
client.connect("13.69.250.61", 1883)
print("Mqtt connected")

client.loop_start()
t.sleep(1)
transmit_gen_data(client)
t.sleep(1)
client.loop_stop()
client.disconnect()
t.sleep(4)
