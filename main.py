# /usr/bin/env python3

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

try:
    conn = psycopg2.connect(
        f'dbname=\'{psql_config["db"]}\' user=\'{psql_config["user"]}\' host=\'{psql_config["host"]}\' password=\'{psql_config["password"]}\'')
except:
    print("Unable to connect to database")
    exit(1)

cur = conn.cursor()


def on_mqtt_message(client, userdata, message):
    print("Received message '" + str(message.payload) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))
    cur.execute(
        "INSERT INTO readings (time, location, metric, value) VALUES (current_timestamp, %s);", (message.topic, message.topic[message.topic.rfind('/'):], message.payload))
    conn.commit()


def main():
    client = mqtt.Client(mqtt_config["client_id"])

    client.username_pw_set(mqtt_config["user"], mqtt_config["password"])
    client.on_message = on_mqtt_message

    client.connect(mqtt_config["host"], mqtt_config["port"])

    # subscribe to all topics
    client.subscribe("+/#")

    client.loop_forever()


if __name__ == "__main__":
    main()
