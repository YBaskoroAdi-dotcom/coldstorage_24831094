import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "edge"))
from logic import MinuteAggregator, Outbox, ThresholdDetector, parse_ts  # noqa: E402


class AggTest(unittest.TestCase):
    def test_one_minute_avg(self):
        a, out = MinuteAggregator(), []
        base = 1_700_000_040  # kelipatan 60
        for i in range(6):  # 6 sampel @10s di menit pertama
            out += a.add("d1", base + i * 10, 4.0 + i, 80.0, i >= 4)
        self.assertEqual(out, [])
        out += a.add("d1", base + 60, 5.0, 80.0, False)  # sampel menit berikut -> flush menit lama
        self.assertEqual(len(out), 1)
        r = out[0]
        self.assertEqual(r["samples"], 6)
        self.assertEqual(r["temperature_avg"], 6.5)
        self.assertEqual((r["temperature_min"], r["temperature_max"]), (4.0, 9.0))
        self.assertAlmostEqual(r["door_open_ratio"], 0.33, places=2)

    def test_flush_due_timer(self):
        a = MinuteAggregator(grace_s=5)
        a.add("d1", 1_700_000_040, 4, 80, False)
        self.assertEqual(a.flush_due(1_700_000_100 + 1), [])  # masih dalam grace
        self.assertEqual(len(a.flush_due(1_700_000_100 + 6)), 1)

    def test_parse_ts(self):
        self.assertEqual(parse_ts("2026-11-20T07:35:00Z"), 1795160100.0)


class ThresholdTest(unittest.TestCase):
    def test_debounce_raise_and_clear(self):
        d = ThresholdDetector(temp_max=8, consecutive=2, hysteresis=0.5)
        self.assertEqual(d.update("d", 0, 8.5, 70, False), [])          # 1x melanggar -> belum
        ev = d.update("d", 10, 8.6, 70, False)                          # 2x berturut -> raised
        self.assertEqual([(e["code"], e["state"]) for e in ev], [("TEMP_HIGH", "raised")])
        self.assertEqual(d.update("d", 20, 8.7, 70, False), [])         # tidak spam
        self.assertEqual(d.update("d", 30, 7.8, 70, False), [])         # belum lewat histeresis
        ev = d.update("d", 40, 7.4, 70, False)
        self.assertEqual([(e["code"], e["state"]) for e in ev], [("TEMP_HIGH", "cleared")])

    def test_door_open_long(self):
        d = ThresholdDetector(door_max_open_s=60, consecutive=1)
        evs = []
        for t in range(0, 100, 10):
            evs += d.update("d", t, 4, 70, True)
        self.assertEqual([e["code"] for e in evs], ["DOOR_OPEN_LONG"])
        self.assertEqual(d.update("d", 100, 4, 70, False)[0]["state"], "cleared")


class OutboxTest(unittest.TestCase):
    def test_fifo_and_cap(self):
        o = Outbox(max_rows=3)
        for i in range(5):
            o.push("t", str(i))
        self.assertEqual([r[2] for r in o.peek(10)], ["2", "3", "4"])  # yang tertua dibuang
        o.ack(o.peek(1)[0][0])
        self.assertEqual(o.size(), 2)

    def test_persistent(self):
        import tempfile
        p = os.path.join(tempfile.mkdtemp(), "b.db")
        Outbox(p).push("t", "x")
        self.assertEqual(Outbox(p).size(), 1)  # tetap ada setelah "restart"


if __name__ == "__main__":
    unittest.main()
