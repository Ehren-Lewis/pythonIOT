import paho.mqtt.client as mqtt
import sys
import datetime
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv() 

# accessing and printing value
print(os.getenv("SQL_PASSWORD"))

utc_now = datetime.datetime.now(datetime.timezone.utc)

# def on_connect(client, userdata, flags, reason_code, properties):
#     print(f"Connected with result code {reason_code}")
#     # Subscribing in on_connect() means that if we lose the connection and
#     # reconnect then subscriptions will be renewed.
#     client.subscribe("$SYS/#")

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    print(utc_now)

# def on_subscribe(client, userdata, mid, reason_code_list, properties):
#     # Since we subscribed only for a single channel, reason_code_list contains
#     # a single entry
#     if reason_code_list[0].is_failure:
#         print(f"Broker rejected you subscription: {reason_code_list[0]}")
#     else:
#         print(f"Broker granted the following QoS: {reason_code_list[0].value}")

# def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
#     # Be careful, the reason_code_list is only present in MQTTv5.
#     # In MQTTv3 it will always be empty
#     if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
#         print("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
#     else:
#         print(f"Broker replied with failure: {reason_code_list[0]}")
#     client.disconnect()

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
# mqttc.on_connect = on_connect
mqttc.on_message = on_message
# mqttc.on_subscribe = on_subscribe
# mqttc.on_unsubscribe = on_unsubscribe


mqttc.connect("192.168.12.221")
mqttc.subscribe("sensor/data")

mqttc.user_data_set([])
mqttc.loop_forever()
print(f"Received the following message: {mqttc.user_data_get()}")