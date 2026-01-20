# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_calc_nao_programado.py
# Último recode: 2026-01-20 20:15 (America/Bahia)
# Motivo: Corrigir "hora extra" por hora: calcular e persistir np_producao_hora (zera na virada da hora) evitando usar np_producao (acumulado do dia) como produção da hora.

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from modules.db_indflow import get_db

TZ_BAHIA = ZoneInfo("America/Bahia")

# Dia operacional vira às 23:59 (igual ao resto do sistema)
DIA_OPERACIONAL_VIRA = time(23, 59)


def now_bahia() -> datetime:
    return datetime.now(TZ_BAHIA)


def _dia_operacional_ref_str(agora: datetime) -> str:
    """
    Dia operacional:
      - de 23:59 até 23:58 do dia seguinte.
    """
    if agora.time() >= DIA_OPERACIONAL_VIRA:
        return agora.date().isoformat()
    return (agora.date() - timedelta(days=1)).isoformat()


def _get_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "on")


def _safe_int(v, default=0) -> int:
    try:
        return int(v or 0)
    except Exception:
        return default


def _delta_non_negative(curr: int, prev: int) -> int:
    d = curr - prev
    if d < 0:
        return 0
    return int(d)


def _norm_machine_id_from_m(m: dict) -> str:
    for k in ("nome", "machine_id", "id", "alias"):
        v = (m.get(k) if isinstance(m, dict) else None)
        if v:
            s = str(v).strip().lower()
            if s:
                return s
    return "maquina01"


def _scoped_machine_id(m: dict, raw_mid: str) -> str:
    mid = (raw_mid or "").strip().lower()
    if not mid:
        mid = "maquina01"
    cid = (m.get("cliente_id") or "").strip() if isinstance(m, dict) else ""
    if cid:
        return f"{cid}::{mid}"
    return mid


def _ensure_np_table(conn) -> None:
    """
    Tabela simples para persistir hora extra / fora do planejado.
    ✅ Agora também persiste controle de hora (np_hour_ref / np_hour_baseline),
    para que np_producao_hora sobreviva a restart.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nao_programado_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,
            np_producao INTEGER NOT NULL DEFAULT 0,
            np_minutos INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)

    # migração leve (bancos antigos)
    try:
        conn.execute("ALTER TABLE nao_programado_diario ADD COLUMN np_hour_ref INTEGER")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE nao_programado_diario ADD COLUMN np_hour_baseline INTEGER")
    except Exception:
        pass

    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_np_diario ON nao_programado_diario(machine_id, data_ref)")
    except Exception:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_np_diario_data ON nao_programado_diario(data_ref)")
    except Exception:
        pass
    conn.commit()


def _load_np_from_db(machine_id: str, data_ref: str) -> dict | None:
    conn = get_db()
    try:
        _ensure_np_table(conn)
        cur = conn.execute(
            """
            SELECT np_producao, np_minutos, np_hour_ref, np_hour_baseline
              FROM nao_programado_diario
             WHERE machine_id = ?
               AND data_ref = ?
             LIMIT 1
            """,
            (machine_id, data_ref),
        )
        row = cur.fetchone()
        if not row:
            return None

        # row pode ser sqlite Row ou tupla
        try:
            return {
                "np_producao": int(row["np_producao"] or 0),
                "np_minutos": int(row["np_minutos"] or 0),
                "np_hour_ref": (int(row["np_hour_ref"]) if row["np_hour_ref"] is not None else None),
                "np_hour_baseline": (int(row["np_hour_baseline"]) if row["np_hour_baseline"] is not None else None),
            }
        except Exception:
            return {
                "np_producao": int(row[0] or 0),
                "np_minutos": int(row[1] or 0),
                "np_hour_ref": (int(row[2]) if row[2] is not None else None),
                "np_hour_baseline": (int(row[3]) if row[3] is not None else None),
            }
    finally:
        conn.close()


def _upsert_np_to_db(machine_id: str, data_ref: str, np_producao: int, np_minutos: int, updated_at: str,
                     np_hour_ref: int | None, np_hour_baseline: int | None) -> None:
    conn = get_db()
    try:
        _ensure_np_table(conn)
        conn.execute(
            """
            INSERT INTO nao_programado_diario (machine_id, data_ref, np_producao, np_minutos, updated_at, np_hour_ref, np_hour_baseline)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref) DO UPDATE SET
                np_producao = excluded.np_producao,
                np_minutos  = excluded.np_minutos,
                updated_at  = excluded.updated_at,
                np_hour_ref = excluded.np_hour_ref,
                np_hour_baseline = excluded.np_hour_baseline
            """,
            (machine_id, data_ref, int(np_producao), int(np_minutos), updated_at,
             (int(np_hour_ref) if np_hour_ref is not None else None),
             (int(np_hour_baseline) if np_hour_baseline is not None else None)),
        )
        conn.commit()
    finally:
        conn.close()


def update_nao_programado(m: dict, dentro_turno: bool, agora: datetime | None = None) -> None:
    """
    Acumula produção e tempo "não programados" (fora do turno) + PERSISTE em DB.

    ✅ Agora calcula também:
      - np_producao_hora (zera na virada da hora)
    """
    if agora is None:
        agora = now_bahia()

    data_ref = _dia_operacional_ref_str(agora)
    raw_mid = _norm_machine_id_from_m(m)
    scoped_mid = _scoped_machine_id(m, raw_mid)

    # init acumuladores em memória
    if "np_producao" not in m:
        m["np_producao"] = 0
    if "np_minutos" not in m:
        m["np_minutos"] = 0
    if "_np_secs" not in m:
        m["_np_secs"] = 0

    # ✅ controle "por hora" (fora do turno)
    if "np_hour_ref" not in m:
        m["np_hour_ref"] = None
    if "np_hour_baseline" not in m:
        m["np_hour_baseline"] = None
    if "np_producao_hora" not in m:
        m["np_producao_hora"] = 0

    # reset por dia operacional + recarrega do DB (se existir)
    prev_data_ref = str(m.get("_np_data_ref") or "").strip()
    if prev_data_ref != data_ref:
        m["_np_data_ref"] = data_ref
        m["_np_last_ts"] = None
        m["_np_last_esp"] = None
        m["_np_active"] = False
        m["_np_secs"] = 0

        # reset hora-extra por hora
        m["np_hour_ref"] = None
        m["np_hour_baseline"] = None
        m["np_producao_hora"] = 0

        loaded = _load_np_from_db(scoped_mid, data_ref)
        if loaded:
            m["np_producao"] = _safe_int(loaded.get("np_producao", 0), 0)
            m["np_minutos"] = _safe_int(loaded.get("np_minutos", 0), 0)
            m["np_hour_ref"] = loaded.get("np_hour_ref", None)
            m["np_hour_baseline"] = loaded.get("np_hour_baseline", None)
        else:
            m["np_producao"] = _safe_int(m.get("np_producao", 0), 0)
            m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0)

    curr_esp = _safe_int(m.get("esp_absoluto", 0), 0)

    prev_esp = m.get("_np_last_esp")
    if prev_esp is None:
        prev_esp = curr_esp
    prev_esp = _safe_int(prev_esp, curr_esp)

    delta = _delta_non_negative(curr_esp, prev_esp)

    run_flag = (
        _get_bool(m.get("run"))
        or _get_bool(m.get("rodando"))
        or _get_bool(m.get("sinal_run"))
    )

    # dentro do turno: fecha janela NP e atualiza marcadores (não conta hora extra por hora aqui)
    if dentro_turno:
        m["_np_active"] = False
        m["_np_last_ts"] = agora.isoformat()
        m["_np_last_esp"] = curr_esp
        m["_np_secs"] = 0
        m["np_producao_hora"] = 0  # quando volta pro turno, hora extra "por hora" não é relevante

        try:
            _upsert_np_to_db(
                machine_id=scoped_mid,
                data_ref=data_ref,
                np_producao=_safe_int(m.get("np_producao", 0), 0),
                np_minutos=_safe_int(m.get("np_minutos", 0), 0),
                updated_at=agora.isoformat(),
                np_hour_ref=m.get("np_hour_ref"),
                np_hour_baseline=m.get("np_hour_baseline"),
            )
        except Exception:
            pass
        return

    # =========================
    # FORA DO TURNO: calcula "por hora"
    # =========================
    hora_ref = int(agora.hour)

    # se hora mudou, ancora baseline da np_producao atual
    try:
        if m.get("np_hour_ref") is None or int(m.get("np_hour_ref")) != hora_ref:
            m["np_hour_ref"] = hora_ref
            m["np_hour_baseline"] = int(_safe_int(m.get("np_producao", 0), 0))
            m["np_producao_hora"] = 0
    except Exception:
        m["np_hour_ref"] = hora_ref
        m["np_hour_baseline"] = int(_safe_int(m.get("np_producao", 0), 0))
        m["np_producao_hora"] = 0

    was_active = _get_bool(m.get("_np_active"))
    activity_this_interval = bool(run_flag or (delta > 0) or was_active)

    # tempo (segundos -> minutos)
    last_ts_raw = m.get("_np_last_ts")
    if activity_this_interval and last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_ts_raw))
            dt_s = int((agora - last_ts).total_seconds())
            if dt_s > 0:
                m["_np_secs"] = _safe_int(m.get("_np_secs", 0), 0) + dt_s
                if m["_np_secs"] >= 60:
                    add_min = m["_np_secs"] // 60
                    m["_np_secs"] = m["_np_secs"] % 60
                    m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0) + int(add_min)
        except Exception:
            pass

    # produção NP (dia)
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # produção NP (hora): dia - baseline da hora
    try:
        base = _safe_int(m.get("np_hour_baseline", 0), 0)
        cur_np = _safe_int(m.get("np_producao", 0), 0)
        m["np_producao_hora"] = max(0, int(cur_np - base))
    except Exception:
        m["np_producao_hora"] = 0

    m["_np_active"] = bool(run_flag or (delta > 0))
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp

    try:
        _upsert_np_to_db(
            machine_id=scoped_mid,
            data_ref=data_ref,
            np_producao=_safe_int(m.get("np_producao", 0), 0),
            np_minutos=_safe_int(m.get("np_minutos", 0), 0),
            updated_at=agora.isoformat(),
            np_hour_ref=m.get("np_hour_ref"),
            np_hour_baseline=m.get("np_hour_baseline"),
        )
    except Exception:
        pass
