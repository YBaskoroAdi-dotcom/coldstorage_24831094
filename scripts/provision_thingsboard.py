"""Membuat device Gateway di ThingsBoard CE via REST API (soal 3a).
Pakai:  python scripts/provision_thingsboard.py            (TB di http://localhost:8080)
Login default tenant: tenant@thingsboard.org / tenant  (GANTI password setelah login pertama).
Token dibuat dari NIM -> tidak ada rahasia yang masuk repo; hasilnya ditulis ke .env (yang di-.gitignore).
"""
import os
import secrets
import sys

import requests

TB = os.getenv("TB_URL", "http://localhost:8080")
USER = os.getenv("TB_USER", "tenant@thingsboard.org")
PASS = os.getenv("TB_PASS", "tenant")
NIM = os.getenv("NIM", "24831094")
GW = f"cs-gw-{NIM}"

r = requests.post(f"{TB}/api/auth/login", json={"username": USER, "password": PASS}, timeout=15)
r.raise_for_status()
h = {"X-Authorization": f"Bearer {r.json()['token']}"}

token = secrets.token_urlsafe(16)
dev = requests.post(f"{TB}/api/device", params={"accessToken": token}, headers=h, timeout=15, json={
    "name": GW, "type": "cold-storage-gateway", "additionalInfo": {"gateway": True,
                                                                     "description": f"Edge gateway Cold Storage {NIM}"}})
if dev.status_code == 400 and "already exists" in dev.text:
    sys.exit(f"Device {GW} sudah ada. Ambil token-nya dari UI (Devices > {GW} > Copy access token) lalu isi .env")
dev.raise_for_status()

with open(".env", "a") as f:
    f.write(f"\nTB_GATEWAY_TOKEN={token}\n")
print(f"OK device '{GW}' dibuat; TB_GATEWAY_TOKEN ditulis ke .env. Jalankan: docker compose up -d edge")
