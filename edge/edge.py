"""Edge gateway Cold Storage (UTS TK122).

Fungsi:
 1. Subscribe telemetry lokal (Mosquitto site) -> agregasi rata-rata 1 menit  (soal 4a)
 2. Deteksi ambang batas lokal + aksi otonom (kipas) + event alert segera       (soal 4a)
 3. Store & forward: semua kiriman ke cloud lewat Outbox SQLite, dikirim FIFO
    hanya saat koneksi ke ThingsBoard hidup; tidak ada data hilang saat putus    (soal 4a)
 4. Menjadi ThingsBoard *Gateway* (MQTT Gateway API) -> banyak device 1 koneksi   (soal 3a)
 5. Menerima RPC / shared attribute dari ThingsBoard -> meneruskan ke topic cmd   (soal 3c)
"""
import json
import logging
import os
import signal
import threading
import time

import paho.mqtt.client as mqtt

from logic import MinuteAggregator, Outbox, ThresholdDetector, parse_ts

NIM = os.getenv("NIM", "24831094")
N_DEVICES = int(os.getenv("N_DEVICES", "3"))
LOCAL_HOST, LOCAL_PORT = os.getenv("MQTT_HOST", "mosquitto"), int(os.getenv("MQTT_PORT", "1883"))
TB_HOST, TB_PORT = os.getenv("TB_HOST", "thingsboard"), int(os.getenv("TB_PORT", "1883"))
TB_TOKEN = os.getenv("TB_GATEWAY_TOKEN", "")
AUTO_FAN = os.getenv("AUTO_FAN", "1") == "1"
DB_PATH = os.getenv("OUTBOX_DB", "/data/outbox.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("edge")

DEVICES = [f"cs-{NIM}-{i:02d}" for i in range(1, N_DEVICES + 1)]
BASE = f"coldstore/{NIM}"

agg = MinuteAggregator()
det = ThresholdDetector(temp_max=float(os.getenv("TEMP_MAX", "8")), temp_min=float(os.getenv("TEMP_MIN", "0")),
                        hum_max=float(os.getenv("HUM_MAX", "85")),
                        door_max_open_s=float(os.getenv("DOOR_MAX_OPEN_S", "120")))
outbox = Outbox(DB_PATH)
cloud_up = threading.Event()
pending_rpc = {}   # device -> request id (menunggu konfirmasi state dari perangkat)
last_fan = {}      # device -> status kipas terakhir
stop = threading.Event()

local = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"edge-{NIM}")
cloud = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"edge-gw-{NIM}")


def to_cloud(topic, obj):
    """Semua pengiriman ke cloud lewat outbox (durable) -> dikirim oleh drainer."""
    outbox.push(topic, json.dumps(obj, separators=(",", ":")))


def send_cmd(dev, fan, source):
    local.publish(f"{BASE}/{dev}/cmd/fan", json.dumps({"fan": bool(fan), "source": source}), qos=1)
    log.info("CMD %s fan=%s (source=%s)", dev, fan, source)


# ---------------------------------------------------------------- site broker
def local_on_connect(c, ud, flags, rc, props=None):
    log.info("local broker connected rc=%s", rc)
    c.subscribe([(f"{BASE}/+/telemetry", 1), (f"{BASE}/+/state", 1)])


def emit_agg(a):
    dev = a["device"]
    values = {k: v for k, v in a.items() if k not in ("device", "window_start")}
    local.publish(f"{BASE}/{dev}/agg1m", json.dumps({"device_id": dev, "window_start": a["window_start"], **values}),
                  qos=1)
    to_cloud("v1/gateway/telemetry", {dev: [{"ts": a["window_start"] * 1000, "values": values}]})
    log.info("AGG1m %s T=%.2f H=%.1f n=%d (outbox=%d)", dev, a["temperature_avg"], a["humidity_avg"],
             a["samples"], outbox.size())


def emit_alert(e):
    dev = e["device"]
    local.publish(f"{BASE}/{dev}/alert", json.dumps(e), qos=1)
    to_cloud("v1/gateway/telemetry", {dev: [{"ts": int(e["ts"] * 1000), "values": {
        "edge_alert": e["code"] if e["state"] == "raised" else "NONE",
        "edge_alert_state": e["state"], "edge_alert_value": e["value"]}}]})
    log.warning("ALERT %s %s %s value=%s thr=%s", e["state"].upper(), dev, e["code"], e["value"], e["threshold"])
    if AUTO_FAN and e["code"] == "TEMP_HIGH":  # aksi otonom lokal, tidak menunggu cloud
        send_cmd(dev, e["state"] == "raised", "edge-auto")


def local_on_message(c, ud, msg):
    parts = msg.topic.split("/")
    if len(parts) != 4:
        return
    dev, kind = parts[2], parts[3]
    try:
        p = json.loads(msg.payload)
        if kind == "telemetry":
            ts = parse_ts(p["ts"])
            for a in agg.add(dev, ts, p["temperature"], p["humidity"], p["door_open"], p.get("fan_on")):
                emit_agg(a)
            for e in det.update(dev, ts, p["temperature"], p["humidity"], p["door_open"]):
                emit_alert(e)
        elif kind == "state":
            last_fan[dev] = p["fan_on"]
            if dev in pending_rpc:  # balas RPC dua arah dari ThingsBoard
                to_cloud("v1/gateway/rpc", {"device": dev, "id": pending_rpc.pop(dev), "data": {"fan_on": p["fan_on"]}})
    except (ValueError, KeyError, TypeError) as exc:
        log.error("payload tidak valid di %s: %s", msg.topic, exc)


# ---------------------------------------------------------------- ThingsBoard
def cloud_on_connect(c, ud, flags, rc, props=None):
    if rc != 0:
        log.error("cloud connect gagal rc=%s", rc)
        return
    log.info("cloud (ThingsBoard) connected; outbox=%d", outbox.size())
    c.subscribe([("v1/gateway/rpc", 1), ("v1/gateway/attributes", 1)])
    for dev in DEVICES:  # daftarkan/aktifkan device di bawah gateway
        c.publish("v1/gateway/connect", json.dumps({"device": dev}), qos=1)
    cloud_up.set()


def cloud_on_disconnect(c, ud, flags, rc, props=None):
    cloud_up.clear()
    log.warning("cloud terputus (rc=%s) -> store & forward aktif, outbox=%d", rc, outbox.size())


def cloud_on_message(c, ud, msg):
    try:
        j = json.loads(msg.payload)
        dev, data = j["device"], j["data"]
        if msg.topic == "v1/gateway/rpc":  # RPC: {"id":1,"method":"setFan","params":true}
            if data.get("method") in ("setFan", "fan"):
                prm = data.get("params")
                send_cmd(dev, prm.get("enabled") if isinstance(prm, dict) else prm, "thingsboard-rpc")
                if "id" in data:
                    pending_rpc[dev] = data["id"]
        elif msg.topic == "v1/gateway/attributes" and "fan" in data:  # shared attribute "fan"
            send_cmd(dev, data["fan"], "thingsboard-attr")
    except (ValueError, KeyError, TypeError) as exc:
        log.error("pesan cloud tidak valid: %s", exc)


def drainer():
    """Kirim isi outbox FIFO ke cloud saat online; baris dihapus hanya setelah PUBACK."""
    while not stop.is_set():
        if not cloud_up.is_set():
            stop.wait(1)
            continue
        rows = outbox.peek(50)
        if not rows:
            stop.wait(0.5)
            continue
        for row_id, topic, payload in rows:
            info = cloud.publish(topic, payload, qos=1)
            try:
                info.wait_for_publish(timeout=5)
            except (RuntimeError, ValueError):
                break
            if not info.is_published():
                break  # koneksi bermasalah -> coba lagi nanti, urutan tetap terjaga
            outbox.ack(row_id)


def flusher():
    while not stop.is_set():
        for a in agg.flush_due(time.time()):
            emit_agg(a)
        stop.wait(5)


def main():
    local.on_connect, local.on_message = local_on_connect, local_on_message
    local.reconnect_delay_set(1, 10)
    local.connect_async(LOCAL_HOST, LOCAL_PORT, keepalive=30)
    local.loop_start()

    if TB_TOKEN:
        cloud.username_pw_set(TB_TOKEN)
        cloud.on_connect, cloud.on_disconnect, cloud.on_message = cloud_on_connect, cloud_on_disconnect, cloud_on_message
        cloud.reconnect_delay_set(1, 15)
        cloud.connect_async(TB_HOST, TB_PORT, keepalive=20)
        cloud.loop_start()
    else:
        log.warning("TB_GATEWAY_TOKEN kosong -> mode offline, semua data menumpuk di outbox")

    for fn in (drainer, flusher):
        threading.Thread(target=fn, daemon=True).start()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    while not stop.is_set():
        stop.wait(30)
        log.info("heartbeat cloud_up=%s outbox=%d", cloud_up.is_set(), outbox.size())
    local.loop_stop()
    cloud.loop_stop()


if __name__ == "__main__":
    main()
