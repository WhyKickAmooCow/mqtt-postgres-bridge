# /usr/bin/python3

import time

# Postgres library
import psycopg

# MQTT library
import paho.mqtt.client as mqtt

# TOML for config file
import tomllib as toml

import re

from tenacity import retry

from tenacity.wait import wait_random_exponential

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

config_file = open("./config.toml", "rb")
config = toml.load(config_file)

mqtt_config = config["mqtt"]
psql_config = config["postgres"]

mappings = config["mappings"]


class MQTT(object):
    def __init__(self, config, mappings, psql):
        self.config = config
        self.mappings = mappings

        self.psql = psql
        self.cursor = self.psql.cursor()

        self.client = mqtt.Client(
            client_id=self.config["client_id"],
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )

        self.client.username_pw_set(self.config["user"], self.config["password"])
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(1, 120)

    @retry(wait=wait_random_exponential(multiplier=1, max=60))
    def connect(self):
        logger.info("Connecting to broker")
        self.client.connect(self.config["host"], self.config["port"])

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info("Connected to broker")
        for topic in self.config["subscriptions"]:
            logger.info("Subscribing to: %s", topic)
            client.subscribe(topic)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        logger.info("Disconnected with result code: %s", reason_code)

    def mapping_value(mapping, message):
        return

    def on_message(self, client, userdata, message):
        logger.info(
            "Received message '%s' on topic '%s' with QoS %s",
            str(message.payload),
            message.topic,
            str(message.qos),
        )
        try:
            for mapping in self.mappings:
                pattern = re.compile(mapping["topic"])
                if pattern.match(message.topic):
                    self.cursor.execute(
                        mapping["statement"],
                        [
                            eval(value["value"], {}, {"message": message})
                            for value in mapping["values"]
                        ],
                    )
        except Exception as e:
            print(e)
        finally:
            self.psql.commit()

    def loop_forever(self):
        self.client.loop_forever()


# @retry(wait=wait_random_exponential(multiplier=1, max=60))
# def main_loop():
psql_connection_string = f"dbname='{psql_config['db']}' user='{psql_config['user']}' host='{psql_config['host']}' port='{psql_config['port']}' password='{psql_config['password']}'"
logger.info("Connecting to PostgreSQL using conn string: %s", psql_connection_string)
with psycopg.connect(psql_connection_string) as conn:
    logger.info("Connected to PostgreSQL")
    mqtt = MQTT(mqtt_config, mappings, conn)
    mqtt.connect()
    mqtt.loop_forever()
