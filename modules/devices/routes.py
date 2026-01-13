from flask import Blueprint, render_template, request, redirect, url_for
from datetime import datetime

from modules.db_indflow import get_db

devices_bp = Blueprint("devices", __name__, template_folder="templates")


def _ensure_devices_table(conn):
    # Tabela mínima para cadastro/vínculo de devices (MAC = device_id)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            machine_id TEXT,
            alias TEXT,
            last_seen TEXT
        )
    """)
    conn.commit()


def _norm_device_id(v: str) -> str:
    # Aceita MAC com ":" e "-" e normaliza (mantém hex + sem separadores)
    s = (v or "").strip().upper()
    s = s.replace(":", "").replace("-", "")
    return s


def _norm_machine_id(v: str) -> str:
    return (v or "").strip().lower()


@devices_bp.route("/", methods=["GET"])
def home():
    db = get_db()
    _ensure_devices_table(db)

    cur = db.execute("""
        SELECT device_id, machine_id, alias, last_seen
        FROM devices
        ORDER BY (machine_id IS NULL) DESC, last_seen DESC
    """)
    rows = cur.fetchall()

    devices = []
    for r in rows:
        # sqlite3.Row ou tuple
        try:
            device_id = r["device_id"]
            machine_id = r["machine_id"]
            alias = r["alias"]
            last_seen = r["last_seen"]
        except Exception:
            device_id, machine_id, alias, last_seen = r

        devices.append({
            "device_id": device_id,
            "machine_id": machine_id,
            "alias": alias,
            "last_seen": last_seen,
        })

    return render_template("devices_home.html", devices=devices)


@devices_bp.route("/link", methods=["POST"])
def link_device():
    device_id = _norm_device_id(request.form.get("device_id"))
    machine_id = _norm_machine_id(request.form.get("machine_id"))

    if not device_id or not machine_id:
        # sem flash pra não exigir SECRET_KEY; só volta
        return redirect(url_for("devices.home"))

    db = get_db()
    _ensure_devices_table(db)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Upsert simples: cria se não existir, senão atualiza machine_id
    cur = db.execute("SELECT device_id FROM devices WHERE device_id = ?", (device_id,))
    exists = cur.fetchone() is not None

    if not exists:
        db.execute(
            "INSERT INTO devices (device_id, machine_id, alias, last_seen) VALUES (?, ?, ?, ?)",
            (device_id, machine_id, None, now),
        )
    else:
        db.execute(
            "UPDATE devices SET machine_id = ? WHERE device_id = ?",
            (machine_id, device_id),
        )

    db.commit()
    return redirect(url_for("devices.home"))
