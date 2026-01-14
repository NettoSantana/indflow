# modules/machine/device_helpers.py
import re
from typing import Optional

from modules.db_indflow import get_db
from modules.machine_calc import now_bahia


# ============================================================
# DEVICES (ESP) — MAC = CPF
# ============================================================

def norm_device_id(v: str) -> str:
    """
    Normaliza MAC de forma ÚNICA no sistema:
    - extrai exatamente 12 caracteres HEX (0-9A-F)
    - retorna "" se não achar
    Aceita formatos: AA:BB:CC:DD:EE:FF, AA-BB-..., AABBCCDDEEFF
    e também strings “sujas” (a função caça o trecho hex).
    """
    s = (v or "").strip().upper()
    if not s:
        return ""

    # remove separadores comuns para facilitar
    s2 = s.replace(":", "").replace("-", "").replace(" ", "")

    # procura um bloco de 12 hex em qualquer lugar
    m = re.search(r"([0-9A-F]{12})", s2)
    return m.group(1) if m else ""


def ensure_devices_table(conn) -> None:
    # Segurança extra: mesmo que init_db não tenha rodado ainda
    conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            machine_id TEXT,
            alias TEXT,
            created_at TEXT,
            last_seen TEXT
        )
    """)
    conn.commit()


def touch_device_seen(device_id: str) -> None:
    """
    Registra 'last_seen' do device SEM NUNCA apagar:
    - machine_id (vínculo)
    - alias (apelido)

    Usa UPSERT para ser robusto.
    """
    if not device_id:
        return

    conn = get_db()
    try:
        ensure_devices_table(conn)

        now_iso = now_bahia().strftime("%Y-%m-%d %H:%M:%S")

        # UPSERT:
        # - se não existir: cria com machine_id/alias NULL
        # - se existir: atualiza apenas last_seen (preserva machine_id/alias/created_at)
        conn.execute("""
            INSERT INTO devices (device_id, machine_id, alias, created_at, last_seen)
            VALUES (?, NULL, NULL, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
              last_seen = excluded.last_seen
        """, (device_id, now_iso, now_iso))

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_machine_from_device(device_id: str) -> Optional[str]:
    """
    Retorna machine_id vinculada ao device (se existir).
    Normaliza para lower() na saída.
    """
    if not device_id:
        return None

    conn = get_db()
    try:
        ensure_devices_table(conn)
        cur = conn.execute(
            "SELECT machine_id FROM devices WHERE device_id = ?",
            (device_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        try:
            mid = row["machine_id"]
        except Exception:
            mid = row[0]

        mid = (mid or "").strip().lower()
        return mid or None
    finally:
        try:
            conn.close()
        except Exception:
            pass
