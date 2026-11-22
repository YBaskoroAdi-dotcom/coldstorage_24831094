"""Logika edge murni (tanpa dependensi MQTT) agar mudah diuji unit-test."""
import sqlite3
import threading
from datetime import datetime, timezone


def parse_ts(s):
    """ISO-8601 'YYYY-MM-DDTHH:MM:SSZ' -> epoch detik (float)."""
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


class MinuteAggregator:
    """Rata-rata/min/maks per perangkat per jendela 1 menit (bucket = epoch // 60)."""

    def __init__(self, grace_s=5.0):
        self.b = {}
        self.grace = grace_s

    def add(self, dev, ts, temp, hum, door_open, fan_on=None):
        bucket, out = int(ts // 60), []
        cur = self.b.get(dev)
        if cur and bucket > cur["bucket"]:
            out.append(self._finish(dev, cur))
            cur = None
        if cur is None:
            cur = self.b[dev] = {"bucket": bucket, "t": [], "h": [], "d": [], "fan": None}
        if bucket < cur["bucket"]:  # sampel terlambat dari jendela lama -> diabaikan
            return out
        cur["t"].append(temp)
        cur["h"].append(hum)
        cur["d"].append(1 if door_open else 0)
        if fan_on is not None:
            cur["fan"] = int(bool(fan_on))
        return out

    def flush_due(self, now):
        limit = int((now - self.grace) // 60)
        out = []
        for dev, cur in list(self.b.items()):
            if cur["bucket"] < limit:
                out.append(self._finish(dev, cur))
                del self.b[dev]
        return out

    @staticmethod
    def _finish(dev, cur):
        t, h, d = cur["t"], cur["h"], cur["d"]
        return {
            "device": dev, "window_start": cur["bucket"] * 60, "samples": len(t),
            "temperature_avg": round(sum(t) / len(t), 2), "temperature_min": round(min(t), 2),
            "temperature_max": round(max(t), 2), "humidity_avg": round(sum(h) / len(h), 1),
            "humidity_max": round(max(h), 1), "door_open_ratio": round(sum(d) / len(d), 2),
            "fan_on": cur["fan"] if cur["fan"] is not None else 0,
        }


class ThresholdDetector:
    """Deteksi ambang batas lokal dengan debounce (consecutive) dan histeresis."""

    def __init__(self, temp_max=8.0, temp_min=0.0, hum_max=85.0, door_max_open_s=120.0,
                 consecutive=2, hysteresis=0.5):
        self.temp_max, self.temp_min, self.hum_max = temp_max, temp_min, hum_max
        self.door_max = door_max_open_s
        self.n, self.hys = consecutive, hysteresis
        self.state = {}      # (dev, code) -> {"active": bool, "count": int}
        self.door_since = {}  # dev -> ts mulai pintu terbuka

    def update(self, dev, ts, temp, hum, door_open):
        if door_open:
            self.door_since.setdefault(dev, ts)
        else:
            self.door_since.pop(dev, None)
        open_for = ts - self.door_since[dev] if dev in self.door_since else 0.0
        rules = (  # code, melanggar?, aman kembali?, nilai, ambang, severity
            ("TEMP_HIGH", temp > self.temp_max, temp <= self.temp_max - self.hys, temp, self.temp_max, "CRITICAL"),
            ("TEMP_LOW", temp < self.temp_min, temp >= self.temp_min + self.hys, temp, self.temp_min, "MAJOR"),
            ("HUM_HIGH", hum > self.hum_max, hum <= self.hum_max - 2.0, hum, self.hum_max, "WARNING"),
            ("DOOR_OPEN_LONG", open_for > self.door_max, not door_open, round(open_for, 1), self.door_max, "MAJOR"),
        )
        events = []
        for code, bad, ok, val, thr, sev in rules:
            st = self.state.setdefault((dev, code), {"active": False, "count": 0})
            if not st["active"]:
                st["count"] = st["count"] + 1 if bad else 0
                if st["count"] >= self.n:
                    st["active"] = True
                    events.append({"device": dev, "code": code, "state": "raised", "value": val,
                                   "threshold": thr, "severity": sev, "ts": ts})
            elif ok:
                st["active"], st["count"] = False, 0
                events.append({"device": dev, "code": code, "state": "cleared", "value": val,
                               "threshold": thr, "severity": sev, "ts": ts})
        return events


class Outbox:
    """Antrian durable (SQLite) untuk store & forward: urut FIFO, bertahan walau container restart."""

    def __init__(self, path=":memory:", max_rows=200_000):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS outbox("
                        "id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, payload TEXT NOT NULL)")
        self.db.commit()
        self.lock, self.max_rows = threading.Lock(), max_rows

    def push(self, topic, payload):
        with self.lock:
            self.db.execute("INSERT INTO outbox(topic,payload) VALUES(?,?)", (topic, payload))
            self.db.execute("DELETE FROM outbox WHERE id <= (SELECT MAX(id) FROM outbox) - ?", (self.max_rows,))
            self.db.commit()

    def peek(self, n=50):
        with self.lock:
            return self.db.execute("SELECT id,topic,payload FROM outbox ORDER BY id LIMIT ?", (n,)).fetchall()

    def ack(self, row_id):
        with self.lock:
            self.db.execute("DELETE FROM outbox WHERE id=?", (row_id,))
            self.db.commit()

    def size(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
