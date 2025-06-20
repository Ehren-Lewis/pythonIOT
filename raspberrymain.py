import paho.mqtt.client as mqtt
import sqlite3
import psycopg2
import json
import time
import os
import threading
from datetime import datetime
from dotenv import load_dotenv, dotenv_values 
load_dotenv() 
# PostgreSQL Config
PG_CONFIG = {
    'host': os.getenv('HOST'),     # your PC's IP
    'database': 'iot_data',
    'user': 'your_pg_user',
    'password': os.getenv('SQL_PASSWORD')
}

# Initialize SQLite backlog
def init_sqlite():
    conn = sqlite3.connect("backlog.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS backlog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# Backup message to SQLite
def backup_to_sqlite(topic, payload):
    conn = sqlite3.connect("backlog.db")
    c = conn.cursor()
    c.execute("INSERT INTO backlog (topic, payload) VALUES (?, ?)", (topic, payload))
    conn.commit()
    conn.close()

# Try pushing to PostgreSQL
def insert_into_postgres(topic, payload):
    try:
        data = json.loads(payload)
        sensor_id = data.get("sensor_id", "unknown")
        temperature = float(data.get("temperature", 0))
        humidity = float(data.get("humidity", 0))
        location = topic.split("/")[-1]  # last part of topic (e.g., bedroom)

        with psycopg2.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        id SERIAL PRIMARY KEY,
                        sensor_id TEXT,
                        location TEXT,
                        temperature REAL,
                        humidity REAL,
                        timestamp TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO sensor_data (sensor_id, location, temperature, humidity)
                    VALUES (%s, %s, %s, %s)
                """, (sensor_id, location, temperature, humidity))
    except Exception as e:
        print(f"[!] Failed to insert into PostgreSQL: {e}")
        raise

# Retry backlog periodically
def flush_backlog():
    while True:
        try:
            conn = sqlite3.connect("backlog.db")
            c = conn.cursor()
            c.execute("SELECT * FROM backlog ORDER BY id ASC LIMIT 50")
            rows = c.fetchall()
            if not rows:
                conn.close()
                time.sleep(10)
                continue

            for row in rows:
                id, topic, payload, _ = row
                try:
                    insert_into_postgres(topic, payload)
                    c.execute("DELETE FROM backlog WHERE id = ?", (id,))
                    conn.commit()
                except Exception:
                    # Leave it in backlog if failed
                    print("[!] Still can't reach PostgreSQL, will retry.")
                    break
            conn.close()
        except Exception as e:
            print(f"[!] Error in backlog flusher: {e}")
        time.sleep(10)

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print(f"[✓] Connected to MQTT broker with code {rc}")
    client.subscribe("sensor/data/#")

def on_message(client, userdata, msg):
    print(f"[MQTT] {msg.topic}: {msg.payload.decode()}")
    try:
        insert_into_postgres(msg.topic, msg.payload.decode())
    except:
        backup_to_sqlite(msg.topic, msg.payload.decode())

# Init SQLite and start background backlog thread
init_sqlite()
threading.Thread(target=flush_backlog, daemon=True).start()

# Start MQTT loop
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
