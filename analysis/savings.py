"""Perhitungan penghematan data akibat agregasi edge (soal 5b) + estimasi biaya (soal 5c).
Jalankan:  python analysis/savings.py   (tanpa dependensi)
Ukuran payload DIUKUR dari contoh JSON nyata (bukan ditebak)."""
import json

NIM = "24831094"
DEVICES, INTERVAL_S, AGG_S = 3, 10, 60
DAY = 86400

raw = {"device_id": f"cs-{NIM}-01", "ts": "2026-11-20T07:35:00Z", "temperature": 4.72,
       "humidity": 78.4, "door_open": False, "fan_on": False}
agg = {"device_id": f"cs-{NIM}-01", "window_start": 1795160100, "samples": 6, "temperature_avg": 4.72,
       "temperature_min": 4.55, "temperature_max": 4.91, "humidity_avg": 78.4, "humidity_max": 79.1,
       "door_open_ratio": 0.0, "fan_on": 0}
topic_raw, topic_agg = f"coldstore/{NIM}/cs-{NIM}-01/telemetry", f"coldstore/{NIM}/cs-{NIM}-01/agg1m"
enc = lambda o: len(json.dumps(o, separators=(",", ":")).encode())
mqtt_hdr = lambda t: 1 + 1 + 2 + len(t) + 2  # fixed hdr + remaining len + topic len + topic + packet id (QoS1)


def compute(devices=DEVICES):
    raw_msgs = devices * DAY // INTERVAL_S
    agg_msgs = devices * DAY // AGG_S
    b_raw, b_agg = enc(raw), enc(agg)
    return {
        "devices": devices, "raw_payload_B": b_raw, "agg_payload_B": b_agg,
        "raw_msgs_day": raw_msgs, "agg_msgs_day": agg_msgs,
        "raw_bytes_day": raw_msgs * b_raw, "agg_bytes_day": agg_msgs * b_agg,
        "raw_wire_day": raw_msgs * (b_raw + mqtt_hdr(topic_raw)),
        "agg_wire_day": agg_msgs * (b_agg + mqtt_hdr(topic_agg)),
        "msg_saving_pct": 100 * (1 - agg_msgs / raw_msgs),
        "byte_saving_pct": 100 * (1 - agg_msgs * b_agg / (raw_msgs * b_raw)),
    }


# harga AWS IoT Core (halaman aws.amazon.com/iot-core/pricing, contoh): $1.00 / juta pesan (blok 5 KB),
# $0.08 / juta menit koneksi. Pesan < 5 KB dihitung 1 pesan.
def aws_month(devices, msgs_per_day):
    msgs = msgs_per_day * 30
    return msgs / 1e6 * 1.00 + devices * 43200 / 1e6 * 0.08


if __name__ == "__main__":
    for n in (DEVICES, 1000):
        c = compute(n)
        print(f"\n== {n} perangkat ==")
        for k, v in c.items():
            print(f"{k:>16}: {v:,.2f}" if isinstance(v, float) else f"{k:>16}: {v:,}")
        print(f"AWS tanpa edge : ${aws_month(n, c['raw_msgs_day']):,.2f}/bln")
        print(f"AWS dgn edge   : ${aws_month(n, c['agg_msgs_day']):,.2f}/bln")
