import paho.mqtt.client as mqtt
import sys
import datetime
import os
import psycopg2

from dotenv import load_dotenv, dotenv_values 
load_dotenv() 


current_utc_time = datetime.datetime.now(datetime.timezone.utc)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    print(current_utc_time)

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_message = on_message

mqttc.connect("192.168.12.221")
# mqttc.subscribe("sensor/data")
mqttc.subscribe("#")

mqttc.user_data_set([])
mqttc.loop_forever()

conn = psycopg2.connect(database = "iot_data", 
                        user = "datacamp", 
                        host= os.getenv("HOST"),
                        password = os.getenv("SQL_PASSWORD"),
                        port = 5432)