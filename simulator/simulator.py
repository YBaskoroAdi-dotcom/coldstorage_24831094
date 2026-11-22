"""Simulator perangkat Cold Storage UMKM (UTS TK122) - paho-mqtt 2.x.

Tiap "perangkat" (ESP32 virtual) punya client MQTT sendiri supaya punya LWT sendiri.
Publish setiap INTERVAL_S detik (<= 10 s sesuai soal 2d):
  coldstore/<NIM>/<device>/telemetry          (gabungan, QoS 1)
  coldstore/<NIM>/<device>/sensor/{temperature,humidity,door}  (per sensor, door QoS 1)
Subscribe perintah:
  coldstore/<NIM>/<device>/cmd/fan            {"fan": true|false}
"""
import json
import os
import random
import signal
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

NIM = os.getenv("NIM", "24831094")
N_DEVICES = int(os.getenv("N_DEVICES", "3"))
INTERVAL = float(os.getenv("INTERVAL_S", "10"))
HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
PER_SENSOR = os.getenv("PER_SENSOR", "1") == "1"
# Skenario demo alert: perangkat #1 pintu "lupa ditutup" setelah FAULT_AFTER_S selama FAULT_DURATION_S
FAULT_AFTER_S = float(os.getenv("FAULT_AFTER_S", "0"))  # 0 = nonaktif
FAULT_DURATION_S = float(os.getenv("FAULT_DURATION_S", "300"))

stop = threading.Event()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class ColdRoom:
    """Model fisika sederhana ruang pendingin (target 2-8 C)."""

    def __init__(self):
        self.temp = random.uniform(3.5, 5.0)
        self.hum = random.uniform(75, 82)
        self.door_open = False
        self.door_left = 0.0
        self.fan_on = False

    def step(self, dt, fault):
        if fault:
            self.door_open = True
        elif self.door_open:
            self.door_left -= dt
            if self.door_left <= 0:
                self.door_open = False
        elif random.random() < 0.02:  # pintu dibuka sebentar (barang keluar-masuk)
            self.door_open, self.door_left = True, random.uniform(10, 40)

        target, k = (14.0, 0.05) if self.door_open else (4.0, 0.15)
        if self.fan_on:  # kipas/kompresor tambahan
            target, k = target - 3.0, k + 0.08
        self.temp += (target - self.temp) * k + random.gauss(0, 0.08)
        hum_target = 92.0 if self.door_open else 78.0
        self.hum += (hum_target - self.hum) * 0.08 + random.gauss(0, 0.3)
        self.hum = max(40.0, min(99.0, self.hum))


class Device:
    def __init__(self, idx):
        self.id = f"cs-{NIM}-{idx:02d}"
        self.base = f"coldstore/{NIM}/{self.id}"
        self.room = ColdRoom()
        self.fault_capable = idx == 1
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sim-{self.id}")
        # LWT: bila koneksi putus tak wajar, broker menerbitkan "offline" (retained)
        self.c.will_set(f"{self.base}/status", json.dumps({"device_id": self.id, "status": "offline"}),
                        qos=1, retain=True)
        self.c.on_connect = self.on_connect
        self.c.on_message = self.on_message
        self.c.reconnect_delay_set(1, 10)

    def on_connect(self, client, userdata, flags, rc, props=None):
        print(f"[{self.id}] connected rc={rc}", flush=True)
        client.subscribe(f"{self.base}/cmd/fan", qos=1)
        client.publish(f"{self.base}/status", json.dumps({"device_id": self.id, "status": "online"}),
                       qos=1, retain=True)
        self.publish_state()

    def on_message(self, client, userdata, msg):
        try:
            self.room.fan_on = bool(json.loads(msg.payload)["fan"])
        except (ValueError, KeyError, TypeError):
            return
        print(f"[{self.id}] CMD fan -> {self.room.fan_on}", flush=True)
        self.publish_state()

    def publish_state(self):
        self.c.publish(f"{self.base}/state",
                       json.dumps({"device_id": self.id, "fan_on": self.room.fan_on, "ts": now_iso()}),
                       qos=1, retain=True)

    def publish_sample(self):
        r, ts = self.room, now_iso()
        telemetry = {"device_id": self.id, "ts": ts, "temperature": round(r.temp, 2),
                     "humidity": round(r.hum, 1), "door_open": r.door_open, "fan_on": r.fan_on}
        self.c.publish(f"{self.base}/telemetry", json.dumps(telemetry, separators=(",", ":")), qos=1)
        if PER_SENSOR:
            for name, val, unit, qos in (("temperature", telemetry["temperature"], "C", 0),
                                         ("humidity", telemetry["humidity"], "%", 0),
                                         ("door", int(r.door_open), "bool", 1)):
                self.c.publish(f"{self.base}/sensor/{name}",
                               json.dumps({"device_id": self.id, "ts": ts, "value": val, "unit": unit}),
                               qos=qos)
        print(f"[{self.id}] {telemetry}", flush=True)


def main():
    devices = [Device(i) for i in range(1, N_DEVICES + 1)]
    for d in devices:
        d.c.connect_async(HOST, PORT, keepalive=30)
        d.c.loop_start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    t0 = time.time()
    while not stop.is_set():
        elapsed = time.time() - t0
        in_fault = FAULT_AFTER_S > 0 and FAULT_AFTER_S <= elapsed < FAULT_AFTER_S + FAULT_DURATION_S
        for d in devices:
            d.room.step(INTERVAL, in_fault and d.fault_capable)
            if d.c.is_connected():
                d.publish_sample()
        stop.wait(INTERVAL)
    for d in devices:  # shutdown bersih -> kirim status offline manual (LWT hanya untuk putus tak wajar)
        d.c.publish(f"{d.base}/status", json.dumps({"device_id": d.id, "status": "offline"}), qos=1, retain=True)
        d.c.disconnect()
        d.c.loop_stop()


if __name__ == "__main__":
    main()
