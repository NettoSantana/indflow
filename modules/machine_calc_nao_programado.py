# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_calc_nao_programado.py
# Último recode: 2026-01-21 22:55 (America/Bahia)
# Motivo: Corrigir baseline do NP por hora: np_hour_baseline deve ancorar no esp_absoluto (contador real), não no acumulado np_producao. Evita explosão (baseline pequeno tipo 3071) e garante np_producao_hora consistente.

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


# ============================================================
# PERSISTÊNCIA - DIA (já existia)
# ============================================================
def _ensure_np_table(conn) -> None:
    """
    Tabela diária para persistir hora extra / fora do planejado.
    Mantém np_producao (dia) e np_minutos (dia) + controle de hora atual (baseline).
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


def _upsert_np_diario_to_db(machine_id: str, data_ref: str, np_producao: int, np_minutos: int, updated_at: str,
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


# ============================================================
# ✅ NOVO: PERSISTÊNCIA - HORA (para "não sumir" ao virar a hora)
# ============================================================
def _ensure_np_horaria_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nao_programado_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,
            hora_dia INTEGER NOT NULL,
            produzido INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_np_horaria
            ON nao_programado_horaria(machine_id, data_ref, hora_dia)
        """)
    except Exception:
        pass
    try:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS ix_np_horaria_data
            ON nao_programado_horaria(data_ref)
        """)
    except Exception:
        pass
    conn.commit()


def upsert_np_horaria(machine_id: str, data_ref: str, hora_dia: int, produzido: int, updated_at: str) -> None:
    """
    Grava a produção NÃO programada por hora do dia (0..23).
    """
    conn = get_db()
    try:
        _ensure_np_horaria_table(conn)
        conn.execute(
            """
            INSERT INTO nao_programado_horaria (machine_id, data_ref, hora_dia, produzido, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref, hora_dia) DO UPDATE SET
                produzido = excluded.produzido,
                updated_at = excluded.updated_at
            """,
            (machine_id, (data_ref or "").strip(), int(hora_dia), int(produzido), updated_at),
        )
        conn.commit()
    finally:
        conn.close()


def update_nao_programado(m: dict, dentro_turno: bool, agora: datetime | None = None) -> None:
    """
    Acumula produção e tempo "não programados" (fora do turno) + PERSISTE em DB.

    ✅ Agora também persiste por hora em nao_programado_horaria:
      - np_producao_hora é gravado em (data_ref, hora_dia)
      - assim o valor fica "na tabela" e não some ao virar a hora

    🔥 RECODE (2026-01-21):
      - np_hour_baseline passa a ser ancorado no esp_absoluto (contador real) ao virar a hora,
        e np_producao_hora vira (esp_absoluto - np_hour_baseline).
      - Isso elimina explosões quando baseline vem pequeno (ex: 3071).
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

    # controle por hora
    if "np_hour_ref" not in m:
        m["np_hour_ref"] = None
    if "np_hour_baseline" not in m:
        m["np_hour_baseline"] = None
    if "np_producao_hora" not in m:
        m["np_producao_hora"] = 0

    # reset por dia operacional + recarrega do DB
    prev_data_ref = str(m.get("_np_data_ref") or "").strip()
    if prev_data_ref != data_ref:
        m["_np_data_ref"] = data_ref
        m["_np_last_ts"] = None
        m["_np_last_esp"] = None
        m["_np_active"] = False
        m["_np_secs"] = 0

        m["np_hour_ref"] = None
        m["np_hour_baseline"] = None
        m["np_producao_hora"] = 0

        loaded = _load_np_from_db(scoped_mid, data_ref)
        if loaded:
            m["np_producao"] = _safe_int(loaded.get("np_producao", 0), 0)
            m["np_minutos"] = _safe_int(loaded.get("np_minutos", 0), 0)
            m["np_hour_ref"] = loaded.get("np_hour_ref", None)
            m["np_hour_baseline"] = loaded.get("np_hour_baseline", None)

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

    # dentro do turno: só salva o diário (horária não é usada)
    if dentro_turno:
        m["_np_active"] = False
        m["_np_last_ts"] = agora.isoformat()
        m["_np_last_esp"] = curr_esp
        m["_np_secs"] = 0
        m["np_producao_hora"] = 0

        try:
            _upsert_np_diario_to_db(
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
    # FORA DO TURNO: por hora do dia
    # =========================
    hora_ref = int(agora.hour)

    # se hora mudou, ancora baseline no contador real (esp_absoluto) e zera hora
    if m.get("np_hour_ref") is None or _safe_int(m.get("np_hour_ref"), -1) != hora_ref:
        m["np_hour_ref"] = hora_ref
        m["np_hour_baseline"] = int(curr_esp)  # ✅ RECODE: baseline por hora = esp_absoluto atual
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

    # produção NP (dia): soma deltas fora do turno
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # ✅ produção NP (hora): esp_absoluto - baseline_esp da hora
    base_esp = m.get("np_hour_baseline")
    if base_esp is None:
        base_esp = curr_esp
        m["np_hour_baseline"] = int(base_esp)
    base_esp = _safe_int(base_esp, curr_esp)
    m["np_producao_hora"] = max(0, int(curr_esp - base_esp))

    m["_np_active"] = bool(run_flag or (delta > 0))
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp

    # ✅ persiste: diário + horária (a cada atualização)
    try:
        _upsert_np_diario_to_db(
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

    try:
        upsert_np_horaria(
            machine_id=scoped_mid,
            data_ref=data_ref,
            hora_dia=hora_ref,
            produzido=_safe_int(m.get("np_producao_hora", 0), 0),
            updated_at=agora.isoformat(),
        )
    except Exception:
        pass
