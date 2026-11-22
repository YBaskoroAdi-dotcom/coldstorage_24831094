# UTS TK122 — IoT Platform & Cloud Integration · Cold Storage UMKM

Studi kasus digit NIM 4/5: **Cold storage UMKM** (suhu, kelembapan, status pintu). Individu — NIM: `24.83.1094` · Nama: Yudhistira Baskoro Adi Admojo.

```
Sensor (simulator ×3) ──MQTT──▶ Mosquitto (site) ──▶ Edge (agregasi 1 mnt · threshold · store&forward)
                                      │                         │ MQTT Gateway API (QoS1)
                                      │                         ▼
                                      │                  ThingsBoard CE ──▶ Dashboard · Alarm · Telegram
                                      │                         ▲ RPC / shared attribute (perintah kipas)
                                      └──▶ kafka-bridge ──▶ Kafka (3 partisi) ──▶ consumer group (2 consumer)
```

## Jalankan
```bash
cp .env.example .env            # isi NIM lengkap
docker compose up -d --build
# ThingsBoard butuh ±3 menit pada start pertama: http://localhost:8080
python scripts/provision_thingsboard.py     # buat gateway device + token -> .env
docker compose up -d edge                    # restart edge agar memakai token
```
Langkah dashboard/rule/RPC: [`docs/thingsboard_setup.md`](docs/thingsboard_setup.md) · Naskah video: [`docs/naskah_video.md`](docs/naskah_video.md)

## Verifikasi cepat
| Tujuan | Perintah |
|---|---|
| Lihat semua topic (MQTTX / CLI) | `mosquitto_sub -h localhost -t 'coldstore/24831094/#' -v` |
| Wildcard `+` | `mosquitto_sub -h localhost -t 'coldstore/24831094/+/sensor/temperature' -v` |
| Wildcard `#` | `mosquitto_sub -h localhost -t 'coldstore/24831094/cs-24831094-01/#' -v` |
| Kirim perintah kipas manual | `mosquitto_pub -h localhost -q 1 -t 'coldstore/24831094/cs-24831094-01/cmd/fan' -m '{"fan":true}'` |
| Partisi & consumer lag | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group coldstore-analytics` |
| Detail partisi topic | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic coldstore.telemetry` |
| Uji store & forward | `docker compose stop thingsboard` → tunggu 2–3 menit (`docker compose logs -f edge` → outbox naik) → `docker compose start thingsboard` |
| Demo alert | set `FAULT_AFTER_S=120` di `.env` → `docker compose up -d simulator` |
| Unit test logika edge | `python -m unittest discover -s tests` |
| Hitung penghematan data | `python analysis/savings.py` |

## Struktur
`simulator/` perangkat virtual · `edge/` logika edge (`logic.py` murni, teruji) · `kafka-bridge/` MQTT→Kafka · `consumer/` consumer group ·
`mosquitto/` konfigurasi · `scripts/` provisioning TB · `analysis/` perhitungan Soal 5b · `tests/` unit test.

## Keamanan repo
Tidak ada password/token/API key di repo. `.env` ada di `.gitignore`. Token bot Telegram hanya di konfigurasi rule node ThingsBoard.
Broker lokal anonymous hanya untuk lab (keamanan perangkat di luar cakupan UTS).

## Sumber
Slide P01–P07 · [ThingsBoard Gateway MQTT API](https://thingsboard.io/docs/reference/gateway-mqtt-api/) · [ThingsBoard MQTT Device API](https://thingsboard.io/docs/reference/mqtt-api/) ·
[Eclipse Mosquitto](https://mosquitto.org/documentation/) · [paho-mqtt](https://eclipse.dev/paho/files/paho.mqtt.python/html/) ·
[Apache Kafka docs](https://kafka.apache.org/documentation/) · [AWS IoT Core pricing](https://aws.amazon.com/iot-core/pricing/)
"# coldstorage_24831094" 
