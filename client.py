from awscrt import mqtt
from awsiot import mqtt_connection_builder

import sqlite3
import time
import os

CLIENT = "PYTHON-47b303a0-b329-4847-a7b4-16eead9d6c69"
ENDPOINT = "a2tt7e6p8l1rsp-ats.iot.eu-west-2.amazonaws.com"
ROOT_CERTIFICATE = r"./secrets/root-CA.crt"

CLIENT_CERTIFICATE = r"./secrets/PYTHON-47b303a0-b329-4847-a7b4-16eead9d6c69.cert.pem"
CLIENT_PRIVATE_KEY = r"./secrets/PYTHON-47b303a0-b329-4847-a7b4-16eead9d6c69.private.key"

TOPICS = [
    "/GM/0bcc878f-61a3-4a15-8d57-b199c4ea3bc5/data",
    "/GM/abf74f71-456e-476e-bb15-69e32c787956/data",
]


class Database(sqlite3.Connection):

    PATH = "./database.sqlite"

    def __init__(self) -> None:
        if not os.path.exists(Database.PATH):
            self.rebuild()

        super().__init__(Database.PATH)
        self.cursor().execute(
            """
            CREATE TABLE IF NOT EXISTS GREENHOUSE_DATA (
                TIMESTAMP INTEGER PRIMARY KEY,
                DEVICE TEXT,
                DATA TEXT
            )
        """
        )

        self.commit()

    def rebuild(self):
        with open(Database.PATH, "w") as file:
            file.write("")


class Client:

    def __init__(self) -> None:
        # fmt: off
        self.client = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT, client_id=CLIENT, cert_filepath=CLIENT_CERTIFICATE,
            pri_key_filepath=CLIENT_PRIVATE_KEY, ca_filepath=ROOT_CERTIFICATE,
        )
        # fmt: on

        self.client.connect().result()
        print("[INFO] - Client Connected")

        subscriptions = map(
            lambda topic: self.client.subscribe(
                topic=topic,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=self.on_message_receive,
            ),
            TOPICS,
        )

        print(
            list(
                map(
                    lambda subscription: "Subscribed to {}".format(
                        str(subscription[0].result()["topic"])
                    ),
                    subscriptions,
                )
            )
        )

    def on_message_receive(self, topic, payload, *args, **kwargs) -> None:
        print("Received message from topic '{}'".format(topic))

        parameters = (int(time.time()), topic.split("/")[2], payload.decode("UTF-8"))

        database = Database()
        database.cursor().execute(
            """
            INSERT INTO GREENHOUSE_DATA 
                (TIMESTAMP, DEVICE, DATA) 
            VALUES
                (?, ?, ?)
        """,
            parameters,
        )
        database.commit()


if __name__ == "__main__":
    client = Client()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
