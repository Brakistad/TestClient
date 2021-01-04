import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("POINT/PUSH", 2)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    json_body = json.loads(msg.payload)
    try:
        influx_client.write_points(json_body["data"], database=json_body["database"])
    except Exception as e:
        print(e)
        print(json_body)
        influx_client.create_database(json_body["database"])
        influx_client.write_points(json_body["data"], database=json_body["database"])


print("starting ...")
influx_client = InfluxDBClient('influxdb', 8086, database='cargo_net', username='cemit', password='3rn3DZKreAQK7AJc')
print("Connected to influxdb database: cargo_net")

client = mqtt.Client("mqtt_to_influxdb", False)
client.username_pw_set("cemit", "SuperPassord!")
client.on_connect = on_connect
client.on_message = on_message
client.connect("mosquitto", 1883)
print("Mqtt connected")

client.loop_forever()
