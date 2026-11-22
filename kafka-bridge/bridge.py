"""Bridge MQTT -> Apache Kafka (UTS TK122 soal 4b).

coldstore/<NIM>/<device>/telemetry -> topic Kafka  coldstore.telemetry  (key = device_id)
coldstore/<NIM>/<device>/agg1m     -> topic Kafka  coldstore.agg1m
Key = device_id memastikan semua data satu perangkat masuk partisi yang sama (urutan terjaga per perangkat).
"""
import logging
import os
import signal
import threading

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

NIM = os.getenv("NIM", "24831094")
MQTT_HOST, MQTT_PORT = os.getenv("MQTT_HOST", "mosquitto"), int(os.getenv("MQTT_PORT", "1883"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MAP = {"telemetry": "coldstore.telemetry", "agg1m": "coldstore.agg1m"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bridge")
producer = Producer({"bootstrap.servers": KAFKA, "client.id": f"bridge-{NIM}", "acks": "all",
                     "enable.idempotence": True, "linger.ms": 20})
stop = threading.Event()
sent = 0


def delivery(err, msg):
    global sent
    if err:
        log.error("gagal kirim ke Kafka: %s", err)
    else:
        sent += 1
        if sent % 50 == 0:
            log.info("terkirim %d pesan (terakhir: %s [p%d] @%d)", sent, msg.topic(), msg.partition(), msg.offset())


def on_connect(c, ud, flags, rc, props=None):
    log.info("MQTT connected rc=%s", rc)
    c.subscribe([(f"coldstore/{NIM}/+/{kind}", 1) for kind in MAP])


def on_message(c, ud, msg):
    parts = msg.topic.split("/")
    if len(parts) != 4 or parts[3] not in MAP:
        return
    producer.produce(MAP[parts[3]], key=parts[2], value=msg.payload, on_delivery=delivery)
    producer.poll(0)


def main():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"kafka-bridge-{NIM}")
    c.on_connect, c.on_message = on_connect, on_message
    c.reconnect_delay_set(1, 10)
    c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    c.loop_start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        producer.poll(0.5)
    c.loop_stop()
    producer.flush(5)


if __name__ == "__main__":
    main()
