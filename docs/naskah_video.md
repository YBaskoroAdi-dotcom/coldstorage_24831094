# Naskah Video Demo (maks 5 menit) — bicara sendiri dengan suaramu

Layout layar: kiri terminal (`docker compose logs -f edge simulator`), kanan browser. Pastikan NIM terlihat (Client ID / nama device / judul dashboard).

| Waktu | Tampilkan | Ucapkan (garis besar) |
|---|---|---|
| 0:00–0:25 | Slide/diagram arsitektur 4 layer | Perkenalan: nama, NIM, studi kasus Cold Storage UMKM; 4 layer Edge–Network–Platform–Application |
| 0:25–1:00 | `docker compose ps` | Semua container jalan: mosquitto, simulator, edge, thingsboard, kafka, bridge, 2 consumer |
| 1:00–1:45 | MQTTX subscribe `coldstore/24831094/#` | Simulator → broker; tunjukkan topic hierarki, payload JSON, LWT/status retained |
| 1:45–2:30 | Log edge `AGG1m ...` | Edge hitung rata² 1 menit, deteksi ambang lokal; 3 sampel/10 dtk → 1 pesan/menit |
| 2:30–3:15 | Dashboard ThingsBoard | Grafik time-series + gauge; klik Switch kipas (RPC) → log simulator `CMD fan -> True` |
| 3:15–3:50 | Set `FAULT_AFTER_S`/tunggu alarm → Telegram di HP | Suhu >8 °C → alarm TB + notifikasi Telegram |
| 3:50–4:25 | `docker compose stop thingsboard` ~1 menit lalu `start` | Store & forward: outbox bertambah, lalu terkuras setelah cloud kembali |
| 4:25–5:00 | `kafka-consumer-groups.sh --describe` | 3 partisi dibagi ke 2 consumer, lag ≈ 0; ringkas penghematan 83% pesan & 72% byte |
