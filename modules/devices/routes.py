from flask import Blueprint, render_template, request, redirect, url_for
from datetime import datetime
import re

from modules.db_indflow import get_db

devices_bp = Blueprint("devices", __name__, template_folder="templates")


# ============================================================
# HELPERS
# ============================================================

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
    """
    Normaliza MAC:
    - remove ':' e '-'
    - uppercase
    """
    s = (v or "").strip().upper()
    s = s.replace(":", "").replace("-", "")
    return s


def _is_valid_mac(v: str) -> bool:
    """
    MAC válido = exatamente 12 caracteres hexadecimais
    """
    return bool(re.fullmatch(r"[0-9A-F]{12}", (v or "")))


def _norm_machine_id(v: str) -> str:
    return (v or "").strip().lower()


def _norm_alias(v: str) -> str:
    s = (v or "").strip()
    if len(s) > 32:
        s = s[:32]
    return s


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# ROUTES
# ============================================================

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
            "is_valid_mac": _is_valid_mac(device_id),
        })

    return render_template("devices_home.html", devices=devices)


@devices_bp.route("/link", methods=["POST"])
def link_device():
    device_id = _norm_device_id(request.form.get("device_id"))
    machine_id = _norm_machine_id(request.form.get("machine_id"))

    # REGRA ESTRUTURAL: DEVICE PRECISA SER MAC VÁLIDO
    if not device_id or not _is_valid_mac(device_id):
        return redirect(url_for("devices.home"))

    if not machine_id:
        return redirect(url_for("devices.home"))

    db = get_db()
    _ensure_devices_table(db)

    now = _now_str()

    cur = db.execute("SELECT device_id FROM devices WHERE device_id = ?", (device_id,))
    exists = cur.fetchone() is not None

    if not exists:
        db.execute(
            "INSERT INTO devices (device_id, machine_id, alias, last_seen) VALUES (?, ?, ?, ?)",
            (device_id, machine_id, None, now),
        )
    else:
        db.execute(
            "UPDATE devices SET machine_id = ?, last_seen = ? WHERE device_id = ?",
            (machine_id, now, device_id),
        )

    db.commit()
    return redirect(url_for("devices.home"))


@devices_bp.route("/unlink", methods=["POST"])
def unlink_device():
    device_id = _norm_device_id(request.form.get("device_id"))

    # Desvincular só faz sentido pro MAC real
    if not device_id or not _is_valid_mac(device_id):
        return redirect(url_for("devices.home"))

    db = get_db()
    _ensure_devices_table(db)

    db.execute(
        "UPDATE devices SET machine_id = NULL, last_seen = ? WHERE device_id = ?",
        (_now_str(), device_id),
    )
    db.commit()
    return redirect(url_for("devices.home"))


@devices_bp.route("/delete", methods=["POST"])
def delete_device():
    """
    Excluir deve permitir remover “fantasmas” também (MAQUINA01, etc),
    então NÃO exigimos MAC válido aqui.
    """
    raw = (request.form.get("device_id") or "").strip()
    if not raw:
        return redirect(url_for("devices.home"))

    # Se vier com separador, normaliza. Se for string livre, mantém "como está" (upper).
    if ":" in raw or "-" in raw:
        device_id = _norm_device_id(raw)
    else:
        device_id = raw.upper()

    db = get_db()
    _ensure_devices_table(db)

    db.execute("DELETE FROM devices WHERE device_id = ?", (device_id,))
    db.commit()
    return redirect(url_for("devices.home"))


@devices_bp.route("/alias", methods=["POST"])
def set_alias():
    device_id = _norm_device_id(request.form.get("device_id"))
    alias = _norm_alias(request.form.get("alias"))

    # Alias só pro MAC real
    if not device_id or not _is_valid_mac(device_id):
        return redirect(url_for("devices.home"))

    db = get_db()
    _ensure_devices_table(db)

    cur = db.execute("SELECT device_id FROM devices WHERE device_id = ?", (device_id,))
    exists = cur.fetchone() is not None
    if not exists:
        return redirect(url_for("devices.home"))

    db.execute(
        "UPDATE devices SET alias = ?, last_seen = ? WHERE device_id = ?",
        (alias if alias else None, _now_str(), device_id),
    )
    db.commit()
    return redirect(url_for("devices.home"))


@devices_bp.route("/cleanup-invalid", methods=["POST"])
def cleanup_invalid():
    """
    Remove com segurança qualquer registro cujo device_id NÃO seja MAC válido.
    Ex.: MAQUINA01, MAQUINA01_DEV, etc.
    """
    db = get_db()
    _ensure_devices_table(db)

    cur = db.execute("SELECT device_id FROM devices")
    rows = cur.fetchall()

    to_delete = []
    for r in rows:
        try:
            device_id = r["device_id"]
        except Exception:
            device_id = r[0]
        if not _is_valid_mac(device_id):
            to_delete.append(device_id)

    for device_id in to_delete:
        db.execute("DELETE FROM devices WHERE device_id = ?", (device_id,))

    db.commit()
    return redirect(url_for("devices.home"))
