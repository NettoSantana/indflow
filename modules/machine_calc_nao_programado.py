# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_calc_nao_programado.py
# Último recode: 2026-01-20 18:05 (America/Bahia)
# Motivo: Persistir produção/tempo "fora do planejado" (hora extra) no SQLite e recarregar após restart; multi-tenant via cliente_id::machine_id.

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
    # aceita 1/0, "1"/"0", True/False, "true"/"false"
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
    """
    Melhor esforço (sem depender de outro arquivo):
    - preferimos "nome" porque é o que o sistema já usa como ID interno
    - fallback: machine_id / id / alias
    """
    for k in ("nome", "machine_id", "id", "alias"):
        v = (m.get(k) if isinstance(m, dict) else None)
        if v:
            s = str(v).strip().lower()
            if s:
                return s
    return "maquina01"


def _scoped_machine_id(m: dict, raw_mid: str) -> str:
    """
    Multi-tenant compatível com o resto do projeto:
      <cliente_id>::<machine_id>
    """
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
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_np_diario ON nao_programado_diario(machine_id, data_ref)")
    except Exception:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_np_diario_data ON nao_programado_diario(data_ref)")
    except Exception:
        pass
    conn.commit()


def _load_np_from_db(machine_id: str, data_ref: str) -> tuple[int, int] | None:
    conn = get_db()
    try:
        _ensure_np_table(conn)
        cur = conn.execute(
            """
            SELECT np_producao, np_minutos
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
            return int(row["np_producao"] or 0), int(row["np_minutos"] or 0)
        except Exception:
            # fallback tuple
            return int(row[0] or 0), int(row[1] or 0)
    finally:
        conn.close()


def _upsert_np_to_db(machine_id: str, data_ref: str, np_producao: int, np_minutos: int, updated_at: str) -> None:
    conn = get_db()
    try:
        _ensure_np_table(conn)
        conn.execute(
            """
            INSERT INTO nao_programado_diario (machine_id, data_ref, np_producao, np_minutos, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref) DO UPDATE SET
                np_producao = excluded.np_producao,
                np_minutos  = excluded.np_minutos,
                updated_at  = excluded.updated_at
            """,
            (machine_id, data_ref, int(np_producao), int(np_minutos), updated_at),
        )
        conn.commit()
    finally:
        conn.close()


def update_nao_programado(m: dict, dentro_turno: bool, agora: datetime | None = None) -> None:
    """
    Acumula produção e tempo "não programados" (fora do turno) + PERSISTE em DB.

    ✅ Ajuste importante:
      - Heartbeat a cada ~5s NÃO pode usar round(dt/60), senão nunca vira 1 minuto.
      - Acumulamos segundos em m["_np_secs"] e convertemos quando bater >= 60s.

    ✅ NOVO:
      - Persiste em SQLite (nao_programado_diario) por machine_id + data_ref (dia operacional)
      - Recarrega do banco na primeira chamada do dia, evitando "sumir" após restart.

    Regras:
      - Dentro do turno: fecha janela não-programada e atualiza marcadores.
      - Fora do turno:
          * Produção: soma delta do esp_absoluto (se delta>0)
          * Tempo: soma enquanto houver atividade no intervalo:
              - run==1 (preferencial) OU
              - delta>0 (houve produção) OU
              - já estava ativo (was_active)

    Campos em m:
      - np_producao: int
      - np_minutos: int
      - _np_secs: int (acumulador de segundos fora do turno)
      - _np_last_ts: iso str
      - _np_last_esp: int
      - _np_active: bool
      - _np_data_ref: str (dia operacional atual que estamos acumulando)
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

    # ✅ Se mudou o dia operacional, zera o "contexto" em memória e recarrega do DB (se existir)
    prev_data_ref = str(m.get("_np_data_ref") or "").strip()
    if prev_data_ref != data_ref:
        m["_np_data_ref"] = data_ref
        m["_np_last_ts"] = None
        m["_np_last_esp"] = None
        m["_np_active"] = False
        m["_np_secs"] = 0

        # tenta recarregar o que já foi salvo (se o servidor reiniciou no meio do dia)
        loaded = _load_np_from_db(scoped_mid, data_ref)
        if loaded:
            m["np_producao"], m["np_minutos"] = loaded
        else:
            m["np_producao"] = _safe_int(m.get("np_producao", 0), 0)
            m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0)

    curr_esp = _safe_int(m.get("esp_absoluto", 0), 0)

    # baseline local para delta (para não perder produção fora do turno)
    prev_esp = m.get("_np_last_esp")
    if prev_esp is None:
        prev_esp = curr_esp
    prev_esp = _safe_int(prev_esp, curr_esp)

    delta = _delta_non_negative(curr_esp, prev_esp)

    # sinal de RUN vindo do ESP (m["run"] existe no backend)
    run_flag = (
        _get_bool(m.get("run"))
        or _get_bool(m.get("rodando"))
        or _get_bool(m.get("sinal_run"))
    )

    # dentro do turno: fecha janela e atualiza baseline/ts
    if dentro_turno:
        m["_np_active"] = False
        m["_np_last_ts"] = agora.isoformat()
        m["_np_last_esp"] = curr_esp
        m["_np_secs"] = 0  # zera acumulador de segundos ao voltar pro turno

        # ✅ persiste também (mantém DB coerente)
        try:
            _upsert_np_to_db(
                machine_id=scoped_mid,
                data_ref=data_ref,
                np_producao=_safe_int(m.get("np_producao", 0), 0),
                np_minutos=_safe_int(m.get("np_minutos", 0), 0),
                updated_at=agora.isoformat(),
            )
        except Exception:
            pass
        return

    was_active = _get_bool(m.get("_np_active"))

    # atividade no intervalo: RUN ou produção ou já estava ativo
    activity_this_interval = bool(run_flag or (delta > 0) or was_active)

    # soma tempo (em segundos) desde último update
    last_ts_raw = m.get("_np_last_ts")
    if activity_this_interval and last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_ts_raw))
            dt_s = int((agora - last_ts).total_seconds())
            if dt_s > 0:
                # acumula segundos
                m["_np_secs"] = _safe_int(m.get("_np_secs", 0), 0) + dt_s

                # converte para minutos quando completar 60s
                if m["_np_secs"] >= 60:
                    add_min = m["_np_secs"] // 60
                    m["_np_secs"] = m["_np_secs"] % 60
                    m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0) + int(add_min)
        except Exception:
            pass

    # soma produção fora do turno
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # janela ativa fora do turno (preferencialmente por RUN)
    m["_np_active"] = bool(run_flag or (delta > 0))

    # atualiza marcadores
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp

    # ✅ persiste sempre que tiver atualização (mantém simples e confiável)
    try:
        _upsert_np_to_db(
            machine_id=scoped_mid,
            data_ref=data_ref,
            np_producao=_safe_int(m.get("np_producao", 0), 0),
            np_minutos=_safe_int(m.get("np_minutos", 0), 0),
            updated_at=agora.isoformat(),
        )
    except Exception:
        pass
