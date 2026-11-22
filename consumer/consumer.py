"""Kafka consumer group 'coldstore-analytics' (UTS TK122 soal 4b).
Jalankan 2 instance (consumer-1, consumer-2) -> partisi dibagi otomatis oleh group coordinator.
Offset di-commit manual SETELAH pesan diproses (at-least-once).
"""
import json
import logging
import os
import signal

from confluent_kafka import Consumer

NAME = os.getenv("CONSUMER_NAME", "consumer-?")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "coldstore.telemetry")
GROUP = os.getenv("KAFKA_GROUP", "coldstore-analytics")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(NAME)
running = True
signal.signal(signal.SIGTERM, lambda *_: globals().update(running=False))
signal.signal(signal.SIGINT, lambda *_: globals().update(running=False))


def main():
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": GROUP, "client.id": NAME,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([TOPIC], on_assign=lambda _, p: log.info("ASSIGNED  %s", sorted(x.partition for x in p)),
                on_revoke=lambda _, p: log.info("REVOKED   %s", sorted(x.partition for x in p)))
    stats, n = {}, 0
    while running:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            log.error(m.error())
            continue
        d = json.loads(m.value())
        dev = m.key().decode()
        s = stats.setdefault(dev, {"n": 0, "sum": 0.0, "max": -999.0})
        s["n"] += 1
        s["sum"] += d["temperature"]
        s["max"] = max(s["max"], d["temperature"])
        log.info("p%d@%d %s T=%.2f (avg=%.2f max=%.2f n=%d)", m.partition(), m.offset(), dev, d["temperature"],
                 s["sum"] / s["n"], s["max"], s["n"])
        n += 1
        c.commit(message=m, asynchronous=False)
    c.close()


if __name__ == "__main__":
    main()
