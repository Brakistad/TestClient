import json
import paho.mqtt.client as mqtt
import time as t
import random as rand
import numpy as np


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


def gen_data(n):
    x = np.linspace(0, n, num=n + 1, endpoint=True)
    f_line_rand = line(x, rand.gauss(0.0, 1.0), rand.gauss(0.0, 30.0))
    return f_line_rand


def transmit_gen_data(client, y):
    milliseconds_in_day = 3600 * 24 * 1000
    topic = "test"
    data = {
        'mac': 'dc:a6:32:82:51:59',
        'value': 0.0,
        'sensor_id': 0,
        'client_id': 'testega',
        'group': 'train_af',
        'unit': '°C',
        'timestamp': total_ms_now() - milliseconds_in_day
    }
    for y_i in y:
        data.update({'value': y_i, 'timestamp': total_ms_now()})
        payload = json.dumps(data)
        client.publish(topic, payload, 2)
        t.sleep(0.5)


client = mqtt.Client()
client.username_pw_set("cemit", "SuperPassord!")
client.on_connect = on_connect
client.on_message = on_message
client.connect("13.69.250.61", 1883)
print("Mqtt connected")

client.loop_start()
t.sleep(1)
transmit_gen_data(client, gen_data(10))
t.sleep(1)
client.loop_stop()
client.disconnect()
t.sleep(4)
