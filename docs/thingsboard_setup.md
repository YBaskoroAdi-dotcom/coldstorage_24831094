# Setup ThingsBoard (Soal 3) — langkah UI

Prasyarat: `docker compose up -d thingsboard` → tunggu ±2–3 menit → buka http://localhost:8080
(login awal tenant `tenant@thingsboard.org` / `tenant`; **ganti password**, lalu ubah judul dashboard/nama device memuat NIM).

## 3a. Device & dashboard
1. `python scripts/provision_thingsboard.py` → membuat device gateway `cs-gw-24831094` dan menulis token ke `.env`.
2. `docker compose up -d edge` → edge terhubung sebagai gateway; device `cs-24831094-01..03` **otomatis terdaftar**
   (Entities → Devices). Telemetry muncul tiap menit (agregasi 1 menit) di tab *Latest telemetry*.
3. Dashboards → Add → judul **`Cold Storage 24831094`** → Add widget:
   - *Charts → Timeseries - Line chart*: `temperature_avg`, `humidity_avg`
   - *Analog gauges → Radial gauge* (atau *Cards → Simple card*): `temperature_avg` (rentang -5…20 °C, zona merah >8)
   - *Alarm widgets → Alarms table* (untuk 3b)
   - *Control widgets → Switch control (RPC)* → method `setFan`, params on = `true`, off = `false` (untuk 3c)

## 3b. Rule alert otomatis
Rule chains → **Root Rule Chain** → buka editor, tambahkan node (cabang `Post telemetry` dari *Message Type Switch*):

| Node | Konfigurasi |
|---|---|
| **script** (filter) | `return (msg.temperature_avg !== undefined && msg.temperature_avg > 8.0) \|\| (msg.humidity_avg !== undefined && msg.humidity_avg > 85.0) \|\| (msg.edge_alert !== undefined && msg.edge_alert !== 'NONE');` |
| **create alarm** | type `COLD_STORAGE_ALERT 24831094`, severity `CRITICAL`, clear condition otomatis oleh node *clear alarm* pada cabang False |
| **deduplication** (opsional) | interval 300 s agar Telegram tidak banjir |
| **script** (transform) | `var t='🚨 ALERT Cold Storage '+metadata.deviceName+'\nSuhu rata² '+msg.temperature_avg+' °C (batas 8)\nKelembapan '+msg.humidity_avg+' %\nNIM 24831094'; return {msg:{chat_id:'<CHAT_ID>',text:t},metadata:metadata,msgType:msgType};` |
| **rest api call** | `POST https://api.telegram.org/bot<BOT_TOKEN>/sendMessage`, header `Content-Type: application/json` |

Buat bot lewat @BotFather, ambil `chat_id` lewat `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates`.
**Token bot hanya ditaruh di konfigurasi node TB — jangan di repo/laporan (blur di screenshot).**
Alternatif email: node *send email* + SMTP di System Settings → Mail server.

Uji: jalankan dengan `FAULT_AFTER_S=120` di `.env` → pintu device 01 "lupa ditutup" → suhu naik >8 °C → alert Telegram.

## 3c. Perintah turun (server → perangkat)
- **RPC**: Gunakan widget *Switch control* di dashboard (atau *Rule chain → rpc call*) → method `setFan`, params `true`.
  Jalur: TB → `v1/gateway/rpc` → edge → `coldstore/24831094/cs-24831094-01/cmd/fan` → simulator (log `CMD fan -> True`)
  → `state` retained → edge membalas RPC.
- **Shared attribute**: Device → Attributes → *Shared* → tambah `fan = true` → jalur `v1/gateway/attributes` → idem.
- Bukti: log `docker compose logs -f simulator edge` + MQTTX subscribe `coldstore/24831094/#`.
