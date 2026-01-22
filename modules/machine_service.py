# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_service.py
# Ultimo recode: 2026-01-22 07:35 (America/Bahia)
# Motivo: Debug NP - expor estado do repo e erros de persistencia no estado m (para aparecer no /machine/status). Mantem OPCAO A.

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List

from modules.db_indflow import get_db

# Repo NP (persistencia)
try:
    from modules.repos.nao_programado_horaria_repo import (
        ensure_table as np_ensure_table,
        upsert_delta as np_upsert_delta,
        load_np_por_hora_24 as np_load_np_por_hora_24,
    )
except Exception:
    try:
        from .repos.nao_programado_horaria_repo import (  # type: ignore
            ensure_table as np_ensure_table,
            upsert_delta as np_upsert_delta,
            load_np_por_hora_24 as np_load_np_por_hora_24,
        )
    except Exception:
        np_ensure_table = None  # type: ignore
        np_upsert_delta = None  # type: ignore
        np_load_np_por_hora_24 = None  # type: ignore

# Calc NP (regras puras) - best-effort
try:
    from modules import machine_calc_nao_programado as np_calc  # type: ignore
except Exception:
    try:
        from . import machine_calc_nao_programado as np_calc  # type: ignore
    except Exception:
        np_calc = None  # type: ignore


UNIDADES_VALIDAS = {"pcs", "m", "m2"}

# ============================================================
# FUSO / DIA OPERACIONAL
# ============================================================
TZ_BAHIA = ZoneInfo("America/Bahia")
DIA_OPERACIONAL_VIRA = time(23, 59)  # vira as 23:59


def now_bahia() -> datetime:
    return datetime.now(TZ_BAHIA)


def dia_operacional_ref_str(agora: Optional[datetime] = None) -> str:
    """
    Dia operacional:
      - inicia as 23:59 e vai ate 23:58 do dia seguinte.
    Referencia (data_ref) e o dia em que comecou (YYYY-MM-DD).
    """
    a = agora or now_bahia()
    if a.time() >= DIA_OPERACIONAL_VIRA:
        return a.date().isoformat()
    return (a.date() - timedelta(days=1)).isoformat()


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _fmt_updated_at(agora: Optional[datetime] = None) -> str:
    a = agora or now_bahia()
    return a.strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# UNIDADES
# ============================================================
def normalizar_unidade(v):
    if v is None:
        return None
    v = str(v).strip().lower()
    if v == "" or v == "none":
        return None
    return v if v in UNIDADES_VALIDAS else None


def aplicar_unidades(m, unidade_1, unidade_2):
    u1 = normalizar_unidade(unidade_1)
    u2 = normalizar_unidade(unidade_2)

    if u1 and u2 and u1 == u2:
        u2 = None

    m["unidade_1"] = u1
    m["unidade_2"] = u2


# ============================================================
# RESET DIARIO (mantido, mas com fuso Bahia e dia operacional)
# ============================================================
def reset_contexto(m, machine_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            machine_id,
            m["ultimo_dia"].isoformat() if hasattr(m.get("ultimo_dia"), "isoformat") else str(m.get("ultimo_dia")),
            m.get("producao_turno", 0),
            m.get("meta_turno", 0),
            m.get("percentual_turno", 0),
        ),
    )

    conn.commit()
    conn.close()

    m["baseline_diario"] = m.get("esp_absoluto", 0)
    m["producao_turno"] = 0
    m["producao_turno_anterior"] = 0
    m["producao_hora"] = 0
    m["percentual_hora"] = 0
    m["percentual_turno"] = 0
    m["tempo_medio_min_por_peca"] = None
    m["ultima_hora"] = None
    m["ultimo_dia"] = now_bahia().date()
    m["reset_executado_hoje"] = True

    # zera NP do estado
    m["_np_active"] = False
    m["_np_secs"] = 0
    m["_np_data_ref"] = dia_operacional_ref_str()
    m["_np_last_esp"] = m.get("esp_absoluto", 0)
    m["_np_last_ts"] = now_bahia().isoformat()
    m["_np_first_ts"] = None
    m["np_hour_ref"] = None
    m["np_hour_baseline"] = None
    m["np_producao"] = 0
    m["np_producao_hora"] = 0
    m["np_minutos"] = 0
    m["np_por_hora_24"] = [0] * 24


def verificar_reset_diario(m, machine_id):
    agora = now_bahia()
    horario_reset = time(23, 59)

    if agora.time() >= horario_reset and not m.get("reset_executado_hoje", False):
        reset_contexto(m, machine_id)

    if m.get("ultimo_dia") and agora.date() != m["ultimo_dia"]:
        m["reset_executado_hoje"] = False


# ============================================================
# TURNO / METAS
# ============================================================
def calcular_horas_turno(inicio_str, fim_str):
    inicio = datetime.strptime(inicio_str, "%H:%M")
    fim = datetime.strptime(fim_str, "%H:%M")

    if fim <= inicio:
        fim += timedelta(days=1)

    horas = []
    atual = inicio
    while atual < fim:
        proxima = atual + timedelta(hours=1)
        horas.append(f"{atual.strftime('%H:%M')} - {proxima.strftime('%H:%M')}")
        atual = proxima

    return horas, inicio, fim


def calcular_metas_por_hora(meta_turno, horas, rampa_percentual):
    qtd_horas = len(horas)
    if qtd_horas <= 0:
        return []

    meta_base = meta_turno / qtd_horas
    meta_primeira = round(meta_base * (rampa_percentual / 100))
    restante = meta_turno - meta_primeira
    horas_restantes = qtd_horas - 1

    metas = [meta_primeira]

    if horas_restantes > 0:
        meta_restante_base = restante // horas_restantes
        sobra = restante % horas_restantes

        for i in range(horas_restantes):
            valor = meta_restante_base + (1 if i < sobra else 0)
            metas.append(valor)

    return metas


def calcular_tempo_medio_turno_min_por_peca(m):
    try:
        produzido = int(m.get("producao_turno", 0) or 0)
        inicio_str = m.get("turno_inicio")

        if produzido > 0 and inicio_str:
            agora = now_bahia()

            inicio_dt = datetime.strptime(inicio_str, "%H:%M")
            inicio_dt = inicio_dt.replace(year=agora.year, month=agora.month, day=agora.day, tzinfo=TZ_BAHIA)

            if agora < inicio_dt:
                inicio_dt -= timedelta(days=1)

            minutos = (agora - inicio_dt).total_seconds() / 60
            minutos = max(minutos, 1)

            return round(minutos / produzido, 2)

        return None
    except Exception:
        return None


# ============================================================
# HISTORICO
# ============================================================
def buscar_historico(machine_id=None, inicio=None, fim=None):
    query = """
        SELECT machine_id, data, produzido, meta, percentual
        FROM producao_diaria
        WHERE 1=1
    """
    params = []

    if machine_id:
        query += " AND machine_id = ?"
        params.append(machine_id)

    if inicio:
        query += " AND data >= ?"
        params.append(inicio)

    if fim:
        query += " AND data <= ?"
        params.append(fim)

    query += " ORDER BY data DESC"

    conn = get_db()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return [dict(r) for r in rows]


# ============================================================
# NAO PROGRAMADO (HORA EXTRA) - ORQUESTRACAO
# ============================================================
def _parse_turno_range(agora: datetime, inicio_str: str, fim_str: str):
    ini = datetime.strptime(inicio_str, "%H:%M").time()
    fim = datetime.strptime(fim_str, "%H:%M").time()

    inicio_dt = agora.replace(hour=ini.hour, minute=ini.minute, second=0, microsecond=0)
    fim_dt = agora.replace(hour=fim.hour, minute=fim.minute, second=0, microsecond=0)

    if fim_dt <= inicio_dt:
        fim_dt += timedelta(days=1)

    if agora < inicio_dt and fim_dt.date() != inicio_dt.date():
        inicio_dt -= timedelta(days=1)
        fim_dt -= timedelta(days=1)

    return inicio_dt, fim_dt


def is_fora_do_turno(m: dict, agora: Optional[datetime] = None) -> bool:
    a = agora or now_bahia()

    if np_calc is not None:
        for fname in ("is_fora_do_turno", "fora_do_turno"):
            fn = getattr(np_calc, fname, None)
            if callable(fn):
                try:
                    return bool(fn(m, a))
                except Exception:
                    pass

    inicio = (m.get("turno_inicio") or "").strip()
    fim = (m.get("turno_fim") or "").strip()
    if not inicio or not fim:
        return True

    try:
        inicio_dt, fim_dt = _parse_turno_range(a, inicio, fim)
        return not (inicio_dt <= a < fim_dt)
    except Exception:
        return True


def _machine_id_scoped(cliente_id: Optional[str], machine_id: str) -> str:
    if cliente_id:
        return f"{cliente_id}::{machine_id}"
    return machine_id


def processar_nao_programado(
    m: dict,
    machine_id: str,
    cliente_id: Optional[str],
    esp_absoluto: int,
    agora: Optional[datetime] = None,
) -> None:
    a = agora or now_bahia()
    data_ref = dia_operacional_ref_str(a)
    hora_dia = int(a.hour)
    updated_at = _fmt_updated_at(a)

    mid = _machine_id_scoped(cliente_id, machine_id)
    esp = _safe_int(esp_absoluto, 0)

    fora_turno = is_fora_do_turno(m, a)

    was_active = bool(m.get("_np_active", False))

    # Debug_toggle: expor estado do repo e ultimo erro de persistencia (visivel no /machine/status)
    m["_np_repo_ok"] = bool(np_ensure_table and np_upsert_delta and np_load_np_por_hora_24)
    m["_np_persist_error"] = None
    m["_np_persist_stage"] = None

    if "np_por_hora_24" not in m or not isinstance(m.get("np_por_hora_24"), list) or len(m.get("np_por_hora_24")) != 24:
        m["np_por_hora_24"] = [0] * 24

    if not fora_turno:
        m["_np_active"] = False
        m["_np_secs"] = 0
        m["_np_data_ref"] = data_ref
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()

        m["np_hour_ref"] = None
        m["np_hour_baseline"] = None
        m["np_producao"] = 0
        m["np_producao_hora"] = 0
        m["np_minutos"] = 0
        return

    # Fora do turno => NP ativo

    # OPCAO A: ao entrar em NP (fora do turno), reancorar baseline da hora quando
    # o estado anterior estiver inativo ou inconsistente. Isso evita travar em valor
    # antigo quando perder updates/bootstraps e o baseline ficar "fantasma".
    entering_np = (not was_active) or (m.get("np_hour_ref") is None) or (m.get("np_hour_baseline") is None)
    if entering_np:
        m["_np_data_ref"] = data_ref
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()
        if not m.get("_np_first_ts"):
            m["_np_first_ts"] = a.isoformat()

        m["np_hour_ref"] = hora_dia
        m["np_hour_baseline"] = esp
        m["np_producao_hora"] = 0
        m["np_producao"] = 0
        m["np_minutos"] = 0

        try:
            if np_load_np_por_hora_24 is not None:
                m["_np_persist_stage"] = "load_on_enter"
                conn = get_db()
                try:
                    m["np_por_hora_24"] = np_load_np_por_hora_24(conn, mid, data_ref)
                finally:
                    conn.close()
        except Exception as e:
            m["_np_persist_error"] = repr(e)
            pass

    m["_np_active"] = True

    np_data_ref = (m.get("_np_data_ref") or "").strip()
    np_last_esp = _safe_int(m.get("_np_last_esp", esp), esp)

    # Mudou dia operacional => reseta trackers e ancora sem gravar
    if np_data_ref != data_ref:
        m["_np_data_ref"] = data_ref
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()
        m["_np_first_ts"] = a.isoformat()

        m["np_hour_ref"] = hora_dia
        m["np_hour_baseline"] = esp
        m["np_producao_hora"] = 0
        m["np_producao"] = 0
        m["np_minutos"] = 0

        try:
            if np_load_np_por_hora_24 is not None:
                m["_np_persist_stage"] = "load_on_day_change"
                conn = get_db()
                try:
                    m["np_por_hora_24"] = np_load_np_por_hora_24(conn, mid, data_ref)
                finally:
                    conn.close()
        except Exception as e:
            m["_np_persist_error"] = repr(e)
            pass
        return

    # Se mudou a hora, reinicia baseline da hora (apenas metrica)
    prev_hour_ref = m.get("np_hour_ref")
    prev_hour_ref_int = _safe_int(prev_hour_ref, -1) if prev_hour_ref is not None else -1
    if prev_hour_ref_int != hora_dia:
        m["np_hour_ref"] = hora_dia
        m["np_hour_baseline"] = esp
        m["np_producao_hora"] = 0

    # Delta por update (NUNCA acumulado diario)
    delta = esp - np_last_esp

    # Contador voltou => so sincroniza
    if delta < 0:
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()
        m["np_producao_hora"] = 0
        m["np_producao"] = 0
        return

    # =====================================================
    # BOOTSTRAP: se _np_last_esp esta 0 (ou delta gigante),
    # ancorar e NAO gravar neste update.
    # =====================================================
    if np_last_esp <= 0 and esp > 0:
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()
        if not m.get("_np_first_ts"):
            m["_np_first_ts"] = a.isoformat()
        return

    # delta absurdo (protecao): provavelmente _np_last_esp inconsistente
    # Ex.: primeiro update apos deploy/reset com contador alto.
    if delta > 200000:  # limiar simples e seguro para evitar "explodir" o banco
        m["_np_last_esp"] = esp
        m["_np_last_ts"] = a.isoformat()
        if not m.get("_np_first_ts"):
            m["_np_first_ts"] = a.isoformat()
        return

    # =====================================================
    # CATCH-UP: total real da hora (esp - baseline_hora)
    # e grava no DB a diferenca para nao "travar" em valor antigo.
    # =====================================================
    base_hora = _safe_int(m.get("np_hour_baseline", esp), esp)
    np_hora_total = max(0, esp - base_hora)

    # valor ja conhecido do DB (ou do ultimo load)
    try:
        db_total_hora = _safe_int((m.get("np_por_hora_24") or [0] * 24)[hora_dia], 0)
    except Exception:
        db_total_hora = 0

    # delta que precisamos gravar para o DB alcancar o total real da hora
    catchup_delta = max(0, int(np_hora_total - db_total_hora))

    # Persistencia (delta real + catch-up)
    delta_to_persist = 0
    if delta > 0:
        delta_to_persist += int(delta)
    if catchup_delta > 0:
        # evita dupla contagem: se delta ja cobre parte, mantem o maior ganho
        # (na pratica, catchup ja considera o db_total; somar delta pode duplicar)
        # entao aqui escolhemos persistir o MAIOR entre delta e catchup, nao a soma.
        delta_to_persist = max(int(delta), int(catchup_delta))

    if delta_to_persist > 0:
        if not (np_ensure_table and np_upsert_delta and np_load_np_por_hora_24):
            m["_np_persist_stage"] = "repo_missing"
            m["_np_persist_error"] = "np_repo_missing"
        else:
            try:
                if np_upsert_delta is not None and np_ensure_table is not None:
                    conn = get_db()
                    try:
                        m["_np_persist_stage"] = "ensure_table"
                        np_ensure_table(conn)
                        m["_np_persist_stage"] = "upsert_delta"
                        np_upsert_delta(conn, mid, data_ref, hora_dia, int(delta_to_persist), updated_at)
                        if np_load_np_por_hora_24 is not None:
                            m["_np_persist_stage"] = "load_24_after_upsert"
                            m["np_por_hora_24"] = np_load_np_por_hora_24(conn, mid, data_ref)
                    finally:
                        conn.close()
            except Exception as e:
                m["_np_persist_stage"] = "persist_exception"
                m["_np_persist_error"] = repr(e)
                pass

    # Atualiza trackers
    m["_np_last_esp"] = esp
    m["_np_last_ts"] = a.isoformat()
    if not m.get("_np_first_ts"):
        m["_np_first_ts"] = a.isoformat()

    # Metrica: NP da hora atual = esp - baseline da hora
    m["np_producao_hora"] = np_hora_total
    m["np_producao"] = np_hora_total

    try:
        first = datetime.fromisoformat(m["_np_first_ts"])
        m["np_minutos"] = int(max(0, (a - first).total_seconds() // 60))
    except Exception:
        m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0)
