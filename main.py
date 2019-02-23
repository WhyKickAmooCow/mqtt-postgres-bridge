# /usr/bin/python3

import time
# Postgres library
import psycopg2
# MQTT library
import paho.mqtt.client as mqtt
# TOML for config file
import toml

config_file = open("./config.toml", "r")
config = toml.load(config_file)

mqtt_config = config["mqtt"]
psql_config = config["postgres"]

psql = False


def connect_db():
    try:
        conn = psycopg2.connect(
            f"dbname='{psql_config['db']}' user='{psql_config['user']}' host='{psql_config['host']}' password='{psql_config['password']}'")
        return conn
    except:
        return False


def on_mqtt_message(client, userdata, message):
    print("Received message '" + str(message.payload) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))
    try:
        cur.execute(
            "INSERT INTO readings (time, location, metric, value) VALUES (current_timestamp, %s, %s, %s);", (
                message.topic, message.topic[message.topic.rfind('/')+1:], float(message.payload)))
    except Exception as e:
        print(e)
    psql.commit()


i = 1
while not psql:
    psql = connect_db()
    if psql:
        break

    print(f"database connection attempt {i} failed")

    i += 1

    if i > 5:
        print("giving up connecting to database")
        exit(1)

    time.sleep(1*i)

print("connected to database")
cur = psql.cursor()


client = mqtt.Client(mqtt_config["client_id"])

client.username_pw_set(mqtt_config["user"], mqtt_config["password"])
client.on_message = on_mqtt_message

client.reconnect_delay_set(1, 120)

while True:
    try:
        client.connect(mqtt_config["host"], mqtt_config["port"])
        break
    except Exception as e:
        print(e)
        time.sleep(1)

print("connected to MQTT broker")

# subscribe to all topics
client.subscribe("+/#")
client.loop_forever()
