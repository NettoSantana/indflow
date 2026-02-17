# PATH: indflow/modules/producao/routes.py
# LAST_RECODE: 2026-02-17 12:47 America/Bahia
# MOTIVO: Corrigir encerramento de OP/bobina: usar esp_last mais recente (max baseline_diario x producao_horaria), remover fechamento indevido no /op/status e fechar ultima bobina no /op/encerrar.

from flask import Blueprint, render_template, redirect, request, jsonify
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import sqlite3
import os
from pathlib import Path
from threading import Lock

# =====================================================
# AUTH
# =====================================================
from modules.admin.routes import login_required

# =====================================================
# DATA (SQLite) - historico diario existente
# =====================================================
# Observacao: este modulo existe em modules/producao/data.py
# e contem init_db, salvar_producao_diaria e listar_historico.
try:
    from modules.producao.data import init_db, salvar_producao_diaria, listar_historico
except Exception:
    # fallback caso o Python esteja resolvendo pacotes de forma diferente
    from .data import init_db, salvar_producao_diaria, listar_historico

# Inicializa o banco do historico ao carregar o modulo
try:
    init_db()
except Exception:
    # Se falhar, a API ainda sobe; mas o historico nao vai persistir.
    pass

# =====================================================
# BLUEPRINT
# =====================================================
producao_bp = Blueprint("producao", __name__, template_folder="templates")

# ------------------------------------------------------------
# TIMEZONE
# ------------------------------------------------------------
_TZ_CACHE = None

def _get_tz():
    """
    Retorna o fuso horario usado no backend.
    Padrao: America/Bahia (Horario da Bahia/Brasil).
    Pode ser sobrescrito por env TZ (ex: America/Bahia).
    """
    global _TZ_CACHE
    if _TZ_CACHE is not None:
        return _TZ_CACHE
    tz_name = (os.getenv("TZ") or "America/Bahia").strip() or "America/Bahia"
    try:
        _TZ_CACHE = ZoneInfo(tz_name)
    except Exception:
        _TZ_CACHE = ZoneInfo("America/Bahia")
    return _TZ_CACHE

def _now_local():
    """Agora no fuso local."""
    return datetime.now(_get_tz())


# =====================================================
# CONTEXTO EM MEMORIA (MESMO PADRAO DO SERVER)
# =====================================================
machine_data = {}



def _get_current_esp_abs(conn: sqlite3.Connection, machine_id: str) -> int:
    """Retorna o ultimo valor absoluto (esp_last) conhecido para a maquina.

    Problema observado:
    - baseline_diario pode ficar atrasado (ex.: ESP sem alimentar, queda de internet)
      e, quando isso acontece, o calculo de pcs/metros por OP/bobina fica errado.

    Regra aplicada:
    - Busca esp_last em baseline_diario e em producao_horaria e usa o MAIOR valor valido.
      Assim, se uma das fontes estiver atrasada, ainda usamos o contador mais recente.
    """
    try:
        cur = conn.cursor()

        esp_bd = None
        esp_ph = None

        try:
            row = cur.execute(
                """
                SELECT esp_last
                FROM baseline_diario
                WHERE lower(machine_id)=lower(?)
                ORDER BY dia_ref DESC, updated_at DESC, id DESC
                LIMIT 1
                """,
                (machine_id,),
            ).fetchone()
            if row and row[0] is not None:
                esp_bd = int(row[0])
        except Exception:
            esp_bd = None

        try:
            row = cur.execute(
                """
                SELECT esp_last
                FROM producao_horaria
                WHERE lower(machine_id)=lower(?)
                ORDER BY data_ref DESC, hora_idx DESC, updated_at DESC, id DESC
                LIMIT 1
                """,
                (machine_id,),
            ).fetchone()
            if row and row[0] is not None:
                esp_ph = int(row[0])
        except Exception:
            esp_ph = None

        candidatos = []
        if isinstance(esp_bd, int) and esp_bd >= 0:
            candidatos.append(esp_bd)
        if isinstance(esp_ph, int) and esp_ph >= 0:
            candidatos.append(esp_ph)

        return max(candidatos) if candidatos else 0
    except Exception:
        return 0

def get_machine(machine_id: str):
    if machine_id not in machine_data:
        machine_data[machine_id] = {
            "machine_id": machine_id,
            "meta_turno": 0,
            "hora_inicio": None,
            "hora_fim": None,
            "rampa_percentual": 0,
            "horas_turno": [],
            "meta_por_hora": [],
        }
    return machine_data[machine_id]


# =====================================================
# OP (ORDEM DE PRODUCAO) - SQLITE + MEMORIA
# =====================================================
DB_PATH = Path(__import__("os").environ.get("INDFLOW_DB_PATH", "indflow.db"))
# =====================================================
# HISTORICO DIARIO - GARANTIR DIA ATUAL (OPCAO 3)
#   Objetivo: o Historico deve sempre conter o dia corrente,
#   mesmo com producao zero, para permitir listar OPs do dia.
# =====================================================
def _to_bahia_iso(iso_str: str) -> str:
    """Converte uma string ISO (com ou sem TZ) para ISO com TZ America/Bahia.
    Regra:
      - Se vier sem timezone (naive), assume UTC (pois no servidor costuma vir UTC).
      - Converte para America/Bahia e retorna com offset (-03:00).
    """
    s = (iso_str or "").strip()
    if not s:
        return s
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # tenta padrao com 'Z'
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
            else:
                return s
        except Exception:
            return s

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    try:
        bahia = ZoneInfo("America/Bahia")
        dt2 = dt.astimezone(bahia)
        return dt2.isoformat(timespec="seconds")
    except Exception:
        return dt.isoformat(timespec="seconds")

def _sum_ops_pcs(ops_list) -> int:
    try:
        return int(sum(int((o or {}).get("op_pcs") or 0) for o in (ops_list or [])))
    except Exception:
        return 0

def _hoje_iso():
    return datetime.now().date().isoformat()

def _last_n_days_iso(n: int):
    """Retorna lista de datas YYYY-MM-DD dos ultimos n dias (inclui hoje), em ordem decrescente."""
    try:
        n = int(n or 0)
    except Exception:
        n = 0
    if n <= 0:
        n = 30
    if n > 365:
        n = 365

    hoje = datetime.now().date()
    out = []
    for i in range(n):
        out.append((hoje - timedelta(days=i)).isoformat())
    return out


def _ensure_range_rows(machine_id: str, days_desc: list[str]):
    """Garante que exista 1 linha em producao_diaria para cada dia do intervalo (insert se faltar)."""
    mid = (machine_id or "").strip()
    if not mid:
        return
    if not isinstance(days_desc, list) or not days_desc:
        return

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        meta_default = _buscar_meta_mais_recente(conn, mid)

        placeholders = ",".join(["?"] * len(days_desc))
        cur.execute(
            f"""
            SELECT data
            FROM producao_diaria
            WHERE machine_id = ?
              AND data IN ({placeholders})
            """,
            [mid] + list(days_desc),
        )
        existing = set([r[0] for r in (cur.fetchall() or []) if r and r[0]])

        for d in days_desc:
            if d in existing:
                continue
            try:
                cur.execute(
                    """
                    INSERT INTO producao_diaria (machine_id, data, produzido, meta)
                    VALUES (?, ?, ?, ?)
                    """,
                    (mid, d, 0, meta_default),
                )
            except Exception:
                continue

        conn.commit()
    except Exception:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _fetch_producao_diaria_range(machine_id: str, days_desc: list[str]):
    """Busca producao_diaria para os dias informados (retorna lista na mesma ordem days_desc)."""
    mid = (machine_id or "").strip()
    if not mid:
        return []
    if not isinstance(days_desc, list) or not days_desc:
        return []

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        placeholders = ",".join(["?"] * len(days_desc))
        cur.execute(
            f"""
            SELECT data, produzido, meta
            FROM producao_diaria
            WHERE machine_id = ?
              AND data IN ({placeholders})
            """,
            [mid] + list(days_desc),
        )
        rows = cur.fetchall() or []
        by_day = {}
        for r in rows:
            if not r:
                continue
            d = r[0]
            by_day[d] = {
                "machine_id": mid,
                "data": d,
                "produzido": int(r[1] or 0),
                "meta": int(r[2] or 0),
            }

        out = []
        for d in days_desc:
            out.append(by_day.get(d, {"machine_id": mid, "data": d, "produzido": 0, "meta": 0}))
        return out
    except Exception:
        return []
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass



def _sum_producao_horaria_pcs(conn, machine_id: str, dia_iso: str) -> int:
    """
    Soma a producao (pcs) registrada na tabela producao_horaria para um dia.
    Isso permite que o Historico reflita a contagem "ao vivo" (por hora),
    sem depender do fechamento do dia.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(COALESCE(produzido, 0)), 0)
            FROM producao_horaria
            WHERE machine_id = ? AND data_ref = ?
            """,
            (machine_id, dia_iso),
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0

def _sync_producao_diaria_from_horaria_range(machine_id: str, days_desc: list[str]):
    """
    Para cada dia em days_desc, faz UPSERT em producao_diaria usando a soma de producao_horaria.
    Mantem a meta existente quando ja cadastrada (>0).
    """
    mid = (machine_id or "").strip()
    if not mid or not days_desc:
        return

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        meta_default = _buscar_meta_mais_recente(conn, mid)

        for dia in days_desc:
            pcs = _sum_producao_horaria_pcs(conn, mid, dia)

            # UPSERT: produzido = pcs (valor absoluto do dia), preservando meta se ja existir.
            cur.execute(
                """
                INSERT INTO producao_diaria (machine_id, data, produzido, meta)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(machine_id, data) DO UPDATE SET
                    produzido = excluded.produzido,
                    meta = CASE
                        WHEN COALESCE(producao_diaria.meta, 0) > 0 THEN producao_diaria.meta
                        ELSE excluded.meta
                    END
                """,
                (mid, dia, int(pcs or 0), int(meta_default or 0)),
            )

        conn.commit()
    except Exception:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def _buscar_meta_mais_recente(conn, machine_id: str) -> int:
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT meta
            FROM producao_diaria
            WHERE machine_id = ?
            ORDER BY data DESC
            LIMIT 1
            """,
            (machine_id,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return 0

def _garantir_dia_atual_no_historico(machine_id: str):
    """Cria linha em producao_diaria para hoje (produzido=0) se nao existir."""
    mid = (machine_id or "").strip()
    if not mid:
        return

    hoje = _hoje_iso()
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM producao_diaria
            WHERE machine_id = ? AND data = ?
            LIMIT 1
            """,
            (mid, hoje),
        )
        exists = cur.fetchone() is not None
        if exists:
            return

        meta = _buscar_meta_mais_recente(conn, mid)
        cur.execute(
            """
            INSERT INTO producao_diaria (machine_id, data, produzido, meta)
            VALUES (?, ?, ?, ?)
            """,
            (mid, hoje, 0, meta),
        )
        conn.commit()
    except Exception:
        # Nao derrubar a pagina por conta do historico
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def _garantir_dia_atual_para_todas_maquinas():
    """Cria linha diaria para hoje (0) para todas as maquinas ja existentes no banco."""
    hoje = _hoje_iso()
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT machine_id
            FROM producao_diaria
            """
        )
        mids = [r[0] for r in (cur.fetchall() or []) if r and r[0]]
        for mid in mids:
            # para evitar abrir/fechar varias conexoes, reutiliza a mesma
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM producao_diaria
                    WHERE machine_id = ? AND data = ?
                    LIMIT 1
                    """,
                    (mid, hoje),
                )
                exists = cur.fetchone() is not None
                if exists:
                    continue
                meta = _buscar_meta_mais_recente(conn, mid)
                cur.execute(
                    """
                    INSERT INTO producao_diaria (machine_id, data, produzido, meta)
                    VALUES (?, ?, ?, ?)
                    """,
                    (mid, hoje, 0, meta),
                )
            except Exception:
                continue
        conn.commit()
    except Exception:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
_op_lock = Lock()

# Uma OP ativa por maquina (em memoria):
# op_active[machine_id] = { ... }
op_active = {}


def _get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_op_db():
    """
    Cria tabela de OP se nao existir.
    Mantem tudo simples e compativel com SQLite.
    """
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ordens_producao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,

            os TEXT NOT NULL,
            lote TEXT NOT NULL,
            operador TEXT NOT NULL,

            bobina TEXT,
            gr_fio TEXT,
            observacoes TEXT,

            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,

            baseline_pcs INTEGER NOT NULL DEFAULT 0,
            baseline_u1 REAL NOT NULL DEFAULT 0,
            baseline_u2 REAL NOT NULL DEFAULT 0,

            op_metros INTEGER NOT NULL DEFAULT 0,
            op_pcs INTEGER NOT NULL DEFAULT 0,
            op_conv_m_por_pcs REAL NOT NULL DEFAULT 0,

            unidade_1 TEXT,
            unidade_2 TEXT
        )
        """
    )

    # Migracoes simples: adicionar colunas novas se a tabela ja existia.
    for sql in [
        "ALTER TABLE ordens_producao ADD COLUMN op_metros INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN op_pcs INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN op_conv_m_por_pcs REAL NOT NULL DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN qtd_mat_bom INTEGER DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN qtd_cost_elas INTEGER DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN refugo INTEGER DEFAULT 0",
        "ALTER TABLE ordens_producao ADD COLUMN qtd_saco_caixa INTEGER DEFAULT 0",
    ]:
        try:
            cur.execute(sql)
        except Exception:
            pass

    # -------------------------------------------------
    # TABELA: FECHAMENTO POR BOBINA (1 OP pode ter N bobinas)
    # -------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ordens_producao_bobinas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            op_id INTEGER NOT NULL,
            idx INTEGER NOT NULL DEFAULT 0,

            comprimento_m INTEGER NOT NULL DEFAULT 0,
            pcs_total INTEGER NOT NULL DEFAULT 0,
            metro_consumido REAL NOT NULL DEFAULT 0,

            qtd_cost_elas INTEGER NOT NULL DEFAULT 0,
            refugo INTEGER NOT NULL DEFAULT 0,
            qtd_saco_caixa INTEGER NOT NULL DEFAULT 0,
            qtd_mat_bom INTEGER NOT NULL DEFAULT 0,

            updated_at TEXT,

            UNIQUE(op_id, idx)
        )
        """
    )

    # Migracao defensiva para colunas novas (caso tabela exista em formato antigo)
    for col, ddl in [
        ("comprimento_m", "INTEGER NOT NULL DEFAULT 0"),
        ("pcs_total", "INTEGER NOT NULL DEFAULT 0"),
        ("metro_consumido", "REAL NOT NULL DEFAULT 0"),
        ("qtd_cost_elas", "INTEGER NOT NULL DEFAULT 0"),
        ("refugo", "INTEGER NOT NULL DEFAULT 0"),
        ("qtd_saco_caixa", "INTEGER NOT NULL DEFAULT 0"),
        ("qtd_mat_bom", "INTEGER NOT NULL DEFAULT 0"),
        ("updated_at", "TEXT"),
    ]:
        try:
            cur.execute(f"ALTER TABLE ordens_producao_bobinas ADD COLUMN {col} {ddl}")
        except Exception:
            pass


    # -------------------------------------------------
    # TABELA: EVENTOS DE BOBINA (TROCA POR TIMESTAMP)
    #   Logica: uma bobina "vale" ate a proxima ser inserida.
    #   Guardamos:
    #     - started_at / ended_at (ISO)
    #     - start_abs_pcs / end_abs_pcs (contador absoluto do ESP)
    #   Assim calculamos pcs_total por bobina = end_abs_pcs - start_abs_pcs.
    # -------------------------------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ordens_producao_bobina_eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            op_id INTEGER NOT NULL,
            seq INTEGER NOT NULL DEFAULT 0,

            comprimento_m INTEGER NOT NULL DEFAULT 0,

            started_at TEXT NOT NULL,
            ended_at TEXT,
            start_abs_pcs INTEGER NOT NULL DEFAULT 0,
            end_abs_pcs INTEGER,

            created_at TEXT,
            updated_at TEXT,

            UNIQUE(op_id, seq)
        )
        """
    )

    # Migracao defensiva para colunas novas (caso tabela exista em formato antigo)
    for col, ddl in [
        ("comprimento_m", "INTEGER NOT NULL DEFAULT 0"),
        ("started_at", "TEXT NOT NULL DEFAULT ''"),
        ("ended_at", "TEXT"),
        ("start_abs_pcs", "INTEGER NOT NULL DEFAULT 0"),
        ("end_abs_pcs", "INTEGER"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
    ]:
        try:
            cur.execute(f"ALTER TABLE ordens_producao_bobina_eventos ADD COLUMN {col} {ddl}")
        except Exception:
            pass

    conn.commit()
    conn.close()


try:
    init_op_db()
except Exception:
    # Nao derrubar o app caso falhe criar tabela em runtime
    pass


def _now_iso():
    tz = _get_tz()
    if tz is None:
        return datetime.now().isoformat(timespec="seconds")
    return datetime.now(tz).isoformat(timespec="seconds")


def _sanitize_mid(v: str) -> str:
    s = (v or "").strip()
    # Mantem simples: permite letras/numeros/_/-
    out = []
    for ch in s:
        if ch.isalnum() or ch in ("_", "-"):
            out.append(ch)
    return "".join(out)


def _as_str(v) -> str:
    return ("" if v is None else str(v)).strip()


def _parse_bobinas_from_str(bobina_str: str):
    s = _as_str(bobina_str)
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    out = []
    for p in parts:
        if not p:
            continue
        if not p.isdigit():
            return None
        out.append(int(p))
    return out


def _normalize_bobinas(data: dict):
    """Retorna (bobinas_list_int, bobina_str). Se invalido, retorna (None, None)."""
    bobinas_in = data.get("bobinas")
    if bobinas_in is None:
        b = _as_str(data.get("bobina"))
        if not b:
            return [], ""
        if not b.isdigit():
            return None, None
        v = int(b)
        return [v], str(v)

    if bobinas_in == "":
        return [], ""

    if not isinstance(bobinas_in, list):
        return None, None

    out = []
    for it in bobinas_in:
        if it is None:
            continue
        s = str(it).strip()
        if s == "":
            continue
        if not s.isdigit():
            return None, None
        out.append(int(s))

    bobina_str = ",".join(str(x) for x in out) if out else ""
    return out, bobina_str



def _get_conv_m_por_pcs(machine_id: str) -> float:
    """Busca conversao (1 pcs = X metros) da maquina. Tenta tabelas comuns."""
    mid = _sanitize_mid(_as_str(machine_id))
    if not mid:
        return 0.0

    # Tabelas/colunas candidatas (compatibilidade entre modulos)
    candidates = [
        ("machine_config", "conv_m_por_pcs"),
        ("maquinas", "conv_m_por_pcs"),
        ("machines", "conv_m_por_pcs"),
        ("machine_settings", "conv_m_por_pcs"),
        ("config_maquina", "conv_m_por_pcs"),
    ]

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        for table, col in candidates:
            try:
                cur.execute(f"SELECT {col} FROM {table} WHERE machine_id = ? LIMIT 1", (mid,))
                row = cur.fetchone()
                if row and row[0] is not None:
                    try:
                        v = float(row[0])
                    except Exception:
                        v = 0.0
                    if v > 0:
                        return v
            except Exception:
                continue
    except Exception:
        pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return 0.0


def _calc_pcs_from_metros(metros: int, conv_m_por_pcs: float) -> int:
    """Retorna floor(metros / conv). Se conv invalida, retorna 0."""
    try:
        m = int(metros or 0)
    except Exception:
        m = 0
    try:
        conv = float(conv_m_por_pcs or 0)
    except Exception:
        conv = 0.0
    if m <= 0 or conv <= 0:
        return 0
    # floor (arredondado pra menos)
    return int(m // conv)


def _insert_op_row(payload: dict) -> int:
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO ordens_producao (
            machine_id, os, lote, operador, bobina, gr_fio, observacoes,
            started_at, ended_at, status,
            baseline_pcs, baseline_u1, baseline_u2,
            op_metros, op_pcs, op_conv_m_por_pcs,
            unidade_1, unidade_2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("machine_id"),
            payload.get("os"),
            payload.get("lote"),
            payload.get("operador"),
            payload.get("bobina"),
            payload.get("gr_fio"),
            payload.get("observacoes"),
            payload.get("started_at"),
            payload.get("ended_at"),
            payload.get("status"),
            int(payload.get("baseline_pcs") or 0),
            float(payload.get("baseline_u1") or 0),
            float(payload.get("baseline_u2") or 0),
            int(payload.get("op_metros") or 0),
            int(payload.get("op_pcs") or 0),
            float(payload.get("op_conv_m_por_pcs") or 0),
            payload.get("unidade_1"),
            payload.get("unidade_2"),
        ),
    )

    conn.commit()
    op_id = int(cur.lastrowid)
    conn.close()
    return op_id


def _close_op_row_v2(op_id: int, ended_at: str, op_metros: int, op_pcs: int, op_conv_m_por_pcs: float):
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE ordens_producao
        SET ended_at = ?, status = ?, op_metros = ?, op_pcs = ?, op_conv_m_por_pcs = ?
        WHERE id = ?
        """,
        (ended_at, "ENCERRADA", int(op_metros or 0), int(op_pcs or 0), float(op_conv_m_por_pcs or 0), int(op_id)),
    )

    conn.commit()
    conn.close()


def _close_op_row(op_id: int, ended_at: str):
    # Wrapper para manter compatibilidade com chamadas antigas
    return _close_op_row_v2(op_id, ended_at, 0, 0, 0.0)



def _update_op_row(op_id: int, payload: dict):
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE ordens_producao
        SET os = ?,
            lote = ?,
            operador = ?,
            bobina = ?,
            gr_fio = ?,
            observacoes = ?
        WHERE id = ?
          AND status = ?
        """,
        (
            payload.get("os"),
            payload.get("lote"),
            payload.get("operador"),
            payload.get("bobina"),
            payload.get("gr_fio"),
            payload.get("observacoes"),
            int(op_id),
            "ATIVA",
        ),
    )

    conn.commit()
    conn.close()



# =====================================================
# OP -> HISTORICO: montar lista de OPs por dia
# =====================================================
def _safe_date_only(dt_str: str):
    s = _as_str(dt_str)
    if not s:
        return None
    # ISO: YYYY-MM-DDTHH:MM:SS
    return s[:10] if len(s) >= 10 else None


def _iter_days_inclusive(start_day: str, end_day: str, max_days: int = 40):
    """Gera dias YYYY-MM-DD do intervalo [start_day, end_day]."""
    try:
        d0 = datetime.fromisoformat(start_day).date()
        d1 = datetime.fromisoformat(end_day).date()
    except Exception:
        return []

    if d1 < d0:
        d0, d1 = d1, d0

    out = []
    cur = d0
    steps = 0
    while cur <= d1 and steps < max_days:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
        steps += 1

    # Se estourou o limite, devolve pelo menos inicio e fim
    if steps >= max_days and out:
        last = d1.isoformat()
        if out[-1] != last:
            out.append(last)
    return out



def _parse_bobinas_csv(csv: str) -> list[int]:
    s = (csv or "").strip()
    if not s:
        return []
    out: list[int] = []
    for part in s.split(","):
        p = part.strip()
        if not p:
            continue
        if p.isdigit():
            out.append(int(p))
    return out


def _alloc_pcs_by_bobinas(op_pcs_total: int, bobinas_m: list[int], conv_m_por_pcs: float) -> list[int]:
    """
    Aloca pcs_total da OP entre bobinas, de forma sequencial (simples e deterministica).
    - capacidade_pcs_bobina = floor(comprimento_m / conv)
    - preenche bobina 1, depois 2, etc.
    - se sobrar pcs alem da capacidade total, joga o restante na ultima bobina.
    """
    try:
        total = int(op_pcs_total or 0)
    except Exception:
        total = 0

    if total <= 0:
        return [0 for _ in bobinas_m] if bobinas_m else [0]

    conv = float(conv_m_por_pcs or 0.0)
    if conv <= 0:
        return [total] + [0 for _ in bobinas_m[1:]] if bobinas_m else [total]

    if not bobinas_m:
        return [total]

    caps: list[int] = []
    for m in bobinas_m:
        try:
            mm = int(m or 0)
        except Exception:
            mm = 0
        if mm <= 0:
            caps.append(0)
        else:
            caps.append(int(mm // conv))

    remaining = total
    alloc: list[int] = []
    for cap in caps:
        take = cap if remaining >= cap else remaining
        if take < 0:
            take = 0
        alloc.append(take)
        remaining -= take

    if remaining > 0 and alloc:
        alloc[-1] += remaining

    return alloc


def _fetch_bobinas_fechamento(op_id: int) -> dict[int, dict]:
    out: dict[int, dict] = {}
    try:
        oid = int(op_id or 0)
    except Exception:
        return out
    if oid <= 0:
        return out

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT idx, comprimento_m, pcs_total, metro_consumido,
                   qtd_cost_elas, refugo, qtd_saco_caixa, qtd_mat_bom
            FROM ordens_producao_bobinas
            WHERE op_id = ?
            ORDER BY idx ASC
            """,
            (oid,),
        )
        for r in cur.fetchall():
            try:
                idx = int(r[0] or 0)
            except Exception:
                idx = 0
            out[idx] = {
                "idx": idx,
                "comprimento_m": int(r[1] or 0),
                "pcs_total": int(r[2] or 0),
                "metro_consumido": float(r[3] or 0.0),
                "qtd_cost_elas": int(r[4] or 0),
                "refugo": int(r[5] or 0),
                "qtd_saco_caixa": int(r[6] or 0),
                "qtd_mat_bom": int(r[7] or 0),
            }
    except Exception:
        out = {}
    finally:
        if conn:
            conn.close()
    return out




def _safe_parse_iso(dt_str: str):
    s = _as_str(dt_str)
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # tentativa com Z
        try:
            if s.endswith("Z"):
                return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _minutes_between_iso(start_iso: str, end_iso: str) -> int:
    a = _safe_parse_iso(start_iso)
    b = _safe_parse_iso(end_iso)
    if not a or not b:
        return 0
    try:
        delta = (b - a).total_seconds()
        if delta < 0:
            delta = 0
        return int(delta // 60)
    except Exception:
        return 0


def _fetch_bobina_eventos(op_id: int) -> list[dict]:
    out: list[dict] = []
    try:
        oid = int(op_id or 0)
    except Exception:
        return out
    if oid <= 0:
        return out

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT seq, comprimento_m, started_at, ended_at, start_abs_pcs, end_abs_pcs
            FROM ordens_producao_bobina_eventos
            WHERE op_id = ?
            ORDER BY seq ASC
            """,
            (oid,),
        )
        for r in (cur.fetchall() or []):
            out.append(
                {
                    "seq": int(r[0] or 0),
                    "comprimento_m": int(r[1] or 0),
                    "started_at": r[2] or "",
                    "ended_at": r[3] or "",
                    "start_abs_pcs": int(r[4] or 0),
                    "end_abs_pcs": (int(r[5]) if r[5] is not None else None),
                }
            )
    except Exception:
        return []
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return out


def _upsert_bobina_event_start(op_id: int, seq: int, comprimento_m: int, started_at: str, start_abs_pcs: int):
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        now_iso = _now_iso()
        cur.execute(
            """
            INSERT INTO ordens_producao_bobina_eventos
                (op_id, seq, comprimento_m, started_at, ended_at, start_abs_pcs, end_abs_pcs, created_at, updated_at)
            VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?)
            ON CONFLICT(op_id, seq) DO UPDATE SET
                comprimento_m = excluded.comprimento_m,
                started_at = excluded.started_at,
                start_abs_pcs = excluded.start_abs_pcs,
                updated_at = excluded.updated_at
            """,
            (
                int(op_id),
                int(seq),
                int(comprimento_m or 0),
                _as_str(started_at),
                int(start_abs_pcs or 0),
                now_iso,
                now_iso,
            ),
        )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def _close_last_bobina_event(op_id: int, ended_at: str, end_abs_pcs: int):
    """Fecha a ultima bobina aberta (ended_at NULL)."""
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        now_iso = _now_iso()
        cur.execute(
            """
            SELECT seq
            FROM ordens_producao_bobina_eventos
            WHERE op_id = ?
              AND (ended_at IS NULL OR ended_at = '')
            ORDER BY seq DESC
            LIMIT 1
            """,
            (int(op_id),),
        )
        row = cur.fetchone()
        if not row:
            return
        seq = int(row[0] or 0)

        cur.execute(
            """
            UPDATE ordens_producao_bobina_eventos
            SET ended_at = ?,
                end_abs_pcs = ?,
                updated_at = ?
            WHERE op_id = ? AND seq = ?
            """,
            (_as_str(ended_at), int(end_abs_pcs or 0), now_iso, int(op_id), int(seq)),
        )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
def _fetch_ops_for_range(machine_id: str | None, day_min: str, day_max: str):
    """
    Busca OPs que cruzam o intervalo [day_min, day_max].
    start_day <= day_max AND (end_day >= day_min OR end_day IS NULL).

    Retorna tambem:
      - bobinas: lista de comprimentos (metros) cadastrada na OP
      - bobinas_itens: lista por bobina com pcs_total/metro_consumido + campos de fechamento
    """
    conn = _get_conn()
    cur = conn.cursor()

    if machine_id:
        cur.execute(
            """
            SELECT id, machine_id, os, lote, operador, bobina, gr_fio, observacoes, started_at, ended_at, status, op_metros, op_pcs, op_conv_m_por_pcs,
                   qtd_mat_bom, qtd_cost_elas, refugo, qtd_saco_caixa
            FROM ordens_producao
            WHERE machine_id = ?
              AND substr(started_at, 1, 10) <= ?
              AND (ended_at IS NULL OR substr(ended_at, 1, 10) >= ?)
            ORDER BY started_at DESC
            """,
            (machine_id, day_max, day_min),
        )
    else:
        cur.execute(
            """
            SELECT id, machine_id, os, lote, operador, bobina, gr_fio, observacoes, started_at, ended_at, status, op_metros, op_pcs, op_conv_m_por_pcs,
                   qtd_mat_bom, qtd_cost_elas, refugo, qtd_saco_caixa
            FROM ordens_producao
            WHERE substr(started_at, 1, 10) <= ?
              AND (ended_at IS NULL OR substr(ended_at, 1, 10) >= ?)
            ORDER BY started_at DESC
            """,
            (day_max, day_min),
        )

    rows = cur.fetchall()
    conn.close()

    ops = []
    for r in rows:
        op_id = int(r[0] or 0)
        bobina_csv = r[5] or ""
        conv = float(r[13] or 0.0)
        op_pcs = int(r[12] or 0)

        # bobinas cadastradas na OP (em metros)
        bobinas_m = _parse_bobinas_csv(bobina_csv)

        # fechamento por bobina (se existir)
        fechamento_map = _fetch_bobinas_fechamento(op_id)

        # Eventos de bobina (preferencial): por troca/timestamp
        eventos = _fetch_bobina_eventos(op_id)

        # Fallback (antigo): alocacao deterministica por capacidade
        alloc_pcs = _alloc_pcs_by_bobinas(op_pcs, bobinas_m, conv)

        bobinas_itens = []
        if eventos:
            # Preferir eventos (troca de bobina por timestamp/baseline)
            # pcs_total = end_abs_pcs - start_abs_pcs
            # tempo_consumo_min = diff(started_at, ended_at)
            if status := (r[10] or ""):
                pass
            # Se OP esta ativa, usamos esp atual como "fim" do ultimo evento em aberto.
            esp_atual_abs = None
            if (r[10] or "") == "ATIVA":
                try:
                    with _get_conn() as conn2:
                        esp_atual_abs = _get_current_esp_abs(conn2, r[1] or "")
                except Exception:
                    esp_atual_abs = None

            for ev in eventos:
                seq = int(ev.get("seq", 0) or 0)
                comprimento_m = int(ev.get("comprimento_m", 0) or 0)

                ev_start = _as_str(ev.get("started_at"))
                ev_end = _as_str(ev.get("ended_at"))

                start_abs = int(ev.get("start_abs_pcs", 0) or 0)
                end_abs = ev.get("end_abs_pcs", None)

                if end_abs is None:
                    # Evento em aberto: se OP ativa, fecha virtualmente com esp atual.
                    if esp_atual_abs is not None:
                        end_abs = int(esp_atual_abs)
                        if not ev_end:
                            ev_end = _now_iso()
                    else:
                        end_abs = start_abs
                        if not ev_end:
                            ev_end = ev_start

                pcs_total = int(end_abs) - int(start_abs)
                if pcs_total < 0:
                    pcs_total = 0

                metro_consumido = float(pcs_total) * conv if conv > 0 else 0.0
                tempo_consumo_min = _minutes_between_iso(ev_start, ev_end) if (ev_start and ev_end) else 0

                row_f = fechamento_map.get(seq, {})
                qtd_cost_elas = int(row_f.get("qtd_cost_elas", 0) or 0)
                refugo = int(row_f.get("refugo", 0) or 0)
                qtd_saco_caixa = int(row_f.get("qtd_saco_caixa", 0) or 0)

                qtd_mat_bom = int(pcs_total - (qtd_cost_elas + refugo + qtd_saco_caixa))
                if qtd_mat_bom < 0:
                    qtd_mat_bom = 0

                bobinas_itens.append(
                    {
                        "idx": seq,  # mantido por compatibilidade (no front vira tempo_consumo depois)
                        "comprimento_m": int(comprimento_m or 0),
                        "pcs_total": int(pcs_total or 0),
                        "metro_consumido": float(metro_consumido or 0.0),
                        "tempo_consumo_min": int(tempo_consumo_min or 0),
                        "started_at": ev_start,
                        "ended_at": ev_end,
                        "qtd_cost_elas": int(qtd_cost_elas or 0),
                        "refugo": int(refugo or 0),
                        "qtd_saco_caixa": int(qtd_saco_caixa or 0),
                        "qtd_mat_bom": int(qtd_mat_bom or 0),
                    }
                )

        elif bobinas_m:
            # Fallback (antigo): alocacao por capacidade (deterministica)
            for idx, comprimento_m in enumerate(bobinas_m):
                pcs_total = alloc_pcs[idx] if idx < len(alloc_pcs) else 0
                metro_consumido = float(pcs_total) * conv if conv > 0 else 0.0

                row_f = fechamento_map.get(idx, {})
                qtd_cost_elas = int(row_f.get("qtd_cost_elas", 0) or 0)
                refugo = int(row_f.get("refugo", 0) or 0)
                qtd_saco_caixa = int(row_f.get("qtd_saco_caixa", 0) or 0)

                qtd_mat_bom = int(pcs_total - (qtd_cost_elas + refugo + qtd_saco_caixa))
                if qtd_mat_bom < 0:
                    qtd_mat_bom = 0

                bobinas_itens.append(
                    {
                        "idx": idx,
                        "comprimento_m": int(comprimento_m or 0),
                        "pcs_total": int(pcs_total or 0),
                        "metro_consumido": float(metro_consumido or 0.0),
                        "qtd_cost_elas": int(qtd_cost_elas or 0),
                        "refugo": int(refugo or 0),
                        "qtd_saco_caixa": int(qtd_saco_caixa or 0),
                        "qtd_mat_bom": int(qtd_mat_bom or 0),
                    }
                )
        else:
            # Sem bobinas: mantem compatibilidade (usa fechamento da OP inteira como pseudo-bobina idx=0)
            try:
                legacy_mat_bom = int(r[14] or 0)
            except Exception:
                legacy_mat_bom = 0
            try:
                legacy_cost = int(r[15] or 0)
            except Exception:
                legacy_cost = 0
            try:
                legacy_refugo = int(r[16] or 0)
            except Exception:
                legacy_refugo = 0
            try:
                legacy_saco = int(r[17] or 0)
            except Exception:
                legacy_saco = 0

            bobinas_itens.append(
                {
                    "idx": 0,
                    "comprimento_m": 0,
                    "pcs_total": int(op_pcs or 0),
                    "metro_consumido": float(op_pcs) * conv if conv > 0 else 0.0,
                    "qtd_cost_elas": legacy_cost,
                    "refugo": legacy_refugo,
                    "qtd_saco_caixa": legacy_saco,
                    "qtd_mat_bom": legacy_mat_bom,
                }
            )

        ops.append(
            {
                "op_id": op_id,
                "machine_id": r[1] or "",
                "os": r[2] or "",
                "lote": r[3] or "",
                "operador": r[4] or "",
                "bobina": bobina_csv,
                "bobinas": bobinas_m,
                "bobinas_itens": bobinas_itens,
                "gr_fio": r[6] or "",
                "observacoes": r[7] or "",
                "started_at": r[8] or "",
                "ended_at": r[9] or "",
                "status": r[10] or "",
                "op_metros": int(r[11] or 0),
                "op_pcs": op_pcs,
                "op_conv_m_por_pcs": conv,
            }
        )
    return ops



# =====================================================
# REDIRECIONAR /producao PARA /
# =====================================================
@producao_bp.route("/")
@login_required
def home():
    return redirect("/")


# =====================================================
# PAGINA DE HISTORICO
# =====================================================
@producao_bp.route("/historico")
@login_required
def historico_page():
    # O template historico.html usa querystring machine_id (?machine_id=xxx)
    return render_template("historico.html")


# =====================================================
# API - HISTORICO (JSON)
# =====================================================
@producao_bp.route("/api/producao/historico", methods=["GET"])
@login_required
def api_historico():
    """
    Retorna historico para a tela /producao/historico (templates/historico.html).
    A tela espera campos:
      - data (YYYY-MM-DD)
      - produzido
      - pecas_boas
      - refugo_total (ou refugo)

    No SQLite atual, a tabela guarda:
      - machine_id, data, produzido, meta
    Entao aqui fazemos um "adapter" simples:
      pecas_boas = produzido
      refugo_total = 0
    """
    machine_id = (request.args.get("machine_id") or "").strip() or None

    try:
        limit = int(request.args.get("limit", 30))
    except Exception:
        limit = 30

    if limit <= 0:
        limit = 30
    if limit > 365:
        limit = 365


    # -------------------------------------------------
    # OPCAO 3: garantir que o dia de hoje exista no historico
    # (mesmo com producao zero), para permitir anexar OPs.
    # -------------------------------------------------
    try:
        if machine_id:
            _garantir_dia_atual_no_historico(machine_id)
        else:
            _garantir_dia_atual_para_todas_maquinas()
    except Exception:
        pass
    # Historico por UPSERT: sempre retornar os ultimos N dias, mesmo com produzido=0.
    # Garante 1 linha por dia na tabela producao_diaria.
    if not machine_id:
        # Sem machine_id, mantemos comportamento antigo (lista resumida).
        try:
            rows = listar_historico(machine_id=machine_id, limit=limit)
        except Exception:
            rows = []
    else:
        days_desc = _last_n_days_iso(limit)
        try:
            _ensure_range_rows(machine_id, days_desc)
        except Exception:
            pass


        # Sincronizar historico diario com a contagem "ao vivo" (producao_horaria).
        # Assim, a tabela do Historico nao fica zerada enquanto a maquina esta produzindo.
        try:
            _sync_producao_diaria_from_horaria_range(machine_id, days_desc)
        except Exception:
            pass

        rows = _fetch_producao_diaria_range(machine_id, days_desc)

    # -------------------------------------------------
    # Anexar OPs (ordens_producao) por dia no historico
    # -------------------------------------------------
    
    ops_map = {}
    try:
        days = [str(r.get("data", "") or "").strip() for r in rows if str(r.get("data", "") or "").strip()]
        if days:
            day_min = min(days)
            day_max = max(days)

            ops = _fetch_ops_for_range(machine_id=machine_id, day_min=day_min, day_max=day_max)

            for op in ops:
                mid = str(op.get("machine_id") or "").strip()
                sd = _safe_date_only(op.get("started_at"))
                ed = _safe_date_only(op.get("ended_at")) or sd
                if not mid or not sd:
                    continue

                for d in _iter_days_inclusive(sd, ed, max_days=40):
                    if d < day_min or d > day_max:
                        continue
                    ops_map.setdefault((mid, d), []).append(op)
    except Exception:
        ops_map = {}

    out = []
    for r in rows:
        produzido = int(r.get("produzido", 0) or 0)
        mid = str(r.get("machine_id", "") or "").strip()
        dia = str(r.get("data", "") or "").strip()
        ops_do_dia = ops_map.get((mid, dia), []) if (mid and dia) else []

        out.append(
            {
                "machine_id": r.get("machine_id", ""),
                "data": r.get("data", ""),
                "produzido": produzido,
                "pecas_boas": produzido,
                "refugo_total": 0,
                "meta": int(r.get("meta", 0) or 0),
                "percentual": (int((produzido * 100) / int(r.get("meta", 0) or 0)) if int(r.get("meta", 0) or 0) > 0 else 0),
                "ops": ops_do_dia,
            }
        )

    return jsonify(out)


def _incrementar_producao_diaria_por_op(machine_id: str, dia_iso: str, delta_pcs: int):
    """
    Soma delta_pcs na producao_diaria do dia (UPSERT incremental).
    - Se nao existir linha no dia, cria com meta mais recente e produzido=delta_pcs.
    - Se existir, faz produzido = produzido + delta_pcs.
    """
    mid = (machine_id or "").strip()
    dia = (dia_iso or "").strip()
    if not mid or not dia:
        return

    try:
        delta = int(delta_pcs or 0)
    except Exception:
        delta = 0

    if delta <= 0:
        return

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        meta = _buscar_meta_mais_recente(conn, mid)

        # UPSERT incremental
        cur.execute(
            """
            INSERT INTO producao_diaria (machine_id, data, produzido, meta)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(machine_id, data) DO UPDATE SET
                produzido = COALESCE(producao_diaria.produzido, 0) + excluded.produzido,
                meta = CASE
                    WHEN COALESCE(producao_diaria.meta, 0) > 0 THEN producao_diaria.meta
                    ELSE excluded.meta
                END
            """,
            (mid, dia, delta, meta),
        )

        conn.commit()
    except Exception:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# =====================================================
# API - SALVAR PRODUCAO DIARIA (JSON)
# =====================================================
@producao_bp.route("/api/producao/salvar_diaria", methods=["POST"])
@login_required
def api_salvar_diaria():
    """
    Endpoint simples para persistir a producao do dia no SQLite.
    Body JSON esperado:
      {
        "machine_id": "maq1",
        "produzido": 1234,
        "meta": 2000
      }
    """
    data = request.get_json(silent=True) or {}

    machine_id = str(data.get("machine_id", "")).strip()
    if not machine_id:
        return jsonify({"error": "machine_id obrigatorio"}), 400

    try:
        produzido = int(data.get("produzido", 0))
    except Exception:
        produzido = 0

    try:
        meta = int(data.get("meta", 0))
    except Exception:
        meta = 0

    if produzido < 0:
        produzido = 0
    if meta < 0:
        meta = 0

    try:
        salvar_producao_diaria(machine_id=machine_id, produzido=produzido, meta=meta)
    except Exception:
        return jsonify({"error": "falha ao salvar no banco"}), 500

    return jsonify({"status": "ok", "machine_id": machine_id})


# =====================================================
# PAGINA DE CONFIGURACAO
# =====================================================
@producao_bp.route("/config/<machine_id>")
@login_required
def config_machine(machine_id):
    return render_template("config_maquina.html", machine_id=machine_id)


# =====================================================
# SALVAR CONFIGURACAO DA MAQUINA
# =====================================================
@producao_bp.route("/config/<machine_id>", methods=["POST"])
@login_required
def salvar_config(machine_id):
    data = request.get_json()

    meta_turno = int(data.get("meta_turno", 0))
    hora_inicio = data.get("hora_inicio")  # "08:00"
    hora_fim = data.get("hora_fim")  # "18:00"
    rampa = int(data.get("rampa_percentual", 0))

    if meta_turno <= 0 or not hora_inicio or not hora_fim:
        return jsonify({"error": "Dados invalidos"}), 400

    fmt = "%H:%M"
    inicio = datetime.strptime(hora_inicio, fmt)
    fim = datetime.strptime(hora_fim, fmt)

    if fim <= inicio:
        return jsonify({"error": "Hora fim deve ser maior que inicio"}), 400

    horas_totais = int((fim - inicio).total_seconds() / 3600)

    if horas_totais <= 0:
        return jsonify({"error": "Turno invalido"}), 400

    meta_base = meta_turno / horas_totais

    horas_turno = []
    meta_por_hora = []

    hora_atual = inicio

    for i in range(horas_totais):
        horas_turno.append(hora_atual.strftime("%H:%M"))

        if i == 0 and rampa > 0:
            meta_hora = round(meta_base * (rampa / 100))
        else:
            meta_hora = round(meta_base)

        meta_por_hora.append(meta_hora)
        hora_atual += timedelta(hours=1)

    m = get_machine(machine_id)
    m["meta_turno"] = meta_turno
    m["hora_inicio"] = hora_inicio
    m["hora_fim"] = hora_fim
    m["rampa_percentual"] = rampa
    m["horas_turno"] = horas_turno
    m["meta_por_hora"] = meta_por_hora

    return jsonify(
        {
            "status": "ok",
            "machine_id": machine_id,
            "horas_turno": horas_turno,
            "meta_por_hora": meta_por_hora,
        }
    )


# =====================================================
# OP - STATUS (JSON)
# GET /producao/op/status?machine_id=corpo
# =====================================================
@producao_bp.route("/op/status", methods=["GET"])
@login_required
def op_status():
    machine_id = _sanitize_mid(request.args.get("machine_id", ""))
    if not machine_id:
        return jsonify({"active": False})

    with _op_lock:
        op = op_active.get(machine_id)

    if not op:
        return jsonify({"active": False})


    # Producao atual da OP = (esp_atual - baseline_pcs)
    with _get_conn() as conn:
        esp_atual = _get_current_esp_abs(conn, machine_id)


    baseline_pcs = int(((op.get("baseline") or {}).get("pcs")) or 0)
    op_pcs_live = max(0, int(esp_atual) - int(baseline_pcs))
    return jsonify(
        {
            "active": True,
            "op_id": op.get("op_id"),
            "machine_id": machine_id,
            "esp_atual": int(esp_atual),
            "baseline_pcs": int(baseline_pcs),
            "op_pcs": int(op_pcs_live),
            "os": op.get("os"),
            "lote": op.get("lote"),
            "operador": op.get("operador"),
            "bobina": op.get("bobina") or "",
            "bobinas": op.get("bobinas") or _parse_bobinas_from_str(op.get("bobina") or "") or [],
            "gr_fio": op.get("gr_fio") or "",
            "observacoes": op.get("observacoes") or "",
            "started_at": op.get("started_at"),
            "baseline": op.get("baseline") or {},
            "unidade_1": op.get("unidade_1") or "",
            "unidade_2": op.get("unidade_2") or "",
            "op_conv_m_por_pcs": op.get("op_conv_m_por_pcs") or 0,
        }
    )


# =====================================================
# OP - INICIAR (JSON)
# POST /producao/op/iniciar
# Body:
# {
#   "machine_id": "corpo",
#   "os": "98668",
#   "lote": "126012560",
#   "operador": "Ricardo",
#   "bobina": "",
#   "gr_fio": "",
#   "observacoes": "",
#   "unidade_1": "m",
#   "unidade_2": "pcs",
#   "baseline": { "pcs": 123, "u1": 10.5, "u2": 123 }
# }
# Nota: baseline chega no proximo passo (front). Por enquanto default 0.
# =====================================================
@producao_bp.route("/op/iniciar", methods=["POST"])
@login_required
def op_iniciar():
    data = request.get_json(silent=True) or {}

    machine_id = _sanitize_mid(_as_str(data.get("machine_id")))
    os_ = _as_str(data.get("os"))
    lote = _as_str(data.get("lote"))
    operador = _as_str(data.get("operador"))

    if not machine_id:
        return jsonify({"error": "machine_id obrigatorio"}), 400
    if not os_ or not lote or not operador:
        return jsonify({"error": "OS, Lote e Operador sao obrigatorios"}), 400

    with _op_lock:
        if machine_id in op_active:
            return jsonify({"error": "Ja existe uma OP ativa para esta maquina"}), 409

    bobinas_list, bobina = _normalize_bobinas(data)
    if bobinas_list is None:
        return jsonify({"error": "Bobinas devem ser numeros (metros)"}), 400
    gr_fio = _as_str(data.get("gr_fio"))
    observacoes = _as_str(data.get("observacoes"))

    unidade_1 = _as_str(data.get("unidade_1"))
    unidade_2 = _as_str(data.get("unidade_2"))

    # Baseline da OP: valor absoluto atual do ESP. A OP sempre inicia com 0 (esp_atual - baseline).
    with _get_conn() as conn:
        esp_atual = _get_current_esp_abs(conn, machine_id)
    baseline_pcs = int(esp_atual)
    baseline_u1 = float(esp_atual)
    baseline_u2 = 0.0
    started_at = _now_iso()

    row_payload = {
        "machine_id": machine_id,
        "os": os_,
        "lote": lote,
        "operador": operador,
        "bobina": bobina,
        "bobinas": bobinas_list,
        "gr_fio": gr_fio,
        "observacoes": observacoes,
        "started_at": started_at,
        "ended_at": None,
        "status": "ATIVA",
        "baseline_pcs": baseline_pcs,
        "baseline_u1": baseline_u1,
        "baseline_u2": baseline_u2,
        "op_metros": 0,
        "op_pcs": 0,
        "op_conv_m_por_pcs": _get_conv_m_por_pcs(machine_id),
        "unidade_1": unidade_1,
        "unidade_2": unidade_2,
    }

    try:
        op_id = _insert_op_row(row_payload)
    except Exception:
        return jsonify({"error": "Falha ao salvar OP no banco"}), 500

    # Registrar evento da primeira bobina (se houver) com timestamp de inicio e baseline absoluto
    # Regra: a bobina atual continua ate a proxima bobina ser inserida (novo evento).
    try:
        if bobinas_list:
            _upsert_bobina_event_start(
                op_id=op_id,
                seq=0,
                comprimento_m=int(bobinas_list[0] or 0),
                started_at=started_at,
                start_abs_pcs=int(baseline_pcs),
            )
    except Exception:
        # Nao bloquear inicio da OP por falha de evento; historico cai em fallback.
        pass


    op_mem = {
        "op_id": op_id,
        "machine_id": machine_id,
        "os": os_,
        "lote": lote,
        "operador": operador,
        "bobina": bobina,
        "bobinas": bobinas_list,
        "gr_fio": gr_fio,
        "observacoes": observacoes,
        "started_at": started_at,
        "baseline": {"pcs": baseline_pcs, "u1": baseline_u1, "u2": baseline_u2},
        "unidade_1": unidade_1,
        "unidade_2": unidade_2,
        "op_conv_m_por_pcs": row_payload.get("op_conv_m_por_pcs") or 0,
    }

    with _op_lock:
        op_active[machine_id] = op_mem

    return jsonify({"status": "ok", "active": True, "op_id": op_id, "machine_id": machine_id})




# =====================================================
# OP - EDITAR (JSON)
# POST /producao/op/editar
# Body:
# {
#   "machine_id": "corpo",
#   "os": "98668",
#   "lote": "126012560",
#   "operador": "Ricardo",
#   "bobinas": [1200, 800],
#   "gr_fio": "",
#   "observacoes": ""
# }
# =====================================================
@producao_bp.route("/op/editar", methods=["POST"])
@login_required
def op_editar():
    data = request.get_json(silent=True) or {}

    machine_id = _sanitize_mid(_as_str(data.get("machine_id")))
    os_ = _as_str(data.get("os"))
    lote = _as_str(data.get("lote"))
    operador = _as_str(data.get("operador"))

    if not machine_id:
        return jsonify({"error": "machine_id obrigatorio"}), 400
    if not os_ or not lote or not operador:
        return jsonify({"error": "OS, Lote e Operador sao obrigatorios"}), 400

    bobinas_list, bobina = _normalize_bobinas(data)
    if bobinas_list is None:
        return jsonify({"error": "Bobinas devem ser numeros (metros)"}), 400

    gr_fio = _as_str(data.get("gr_fio"))
    observacoes = _as_str(data.get("observacoes"))

    with _op_lock:
        op = op_active.get(machine_id)

    if not op:
        return jsonify({"error": "Nao existe OP ativa para esta maquina"}), 404

    op_id = int(op.get("op_id") or 0)
    if op_id <= 0:
        return jsonify({"error": "OP ativa invalida"}), 500

    # Se aumentou a lista de bobinas durante a OP ativa, tratamos como "troca de bobina".
    # Regra: bobina anterior conta ate o momento em que a nova bobina e inserida.
    try:
        prev_list = op.get("bobinas") or _parse_bobinas_from_str(op.get("bobina") or "") or []
        new_list = bobinas_list or []
        if len(new_list) > len(prev_list) and len(new_list) >= 1:
            # Considera apenas adicionados no fim (comportamento do front: "Adicionar bobina")
            added = new_list[len(prev_list):]
            if added:
                with _get_conn() as conn:
                    esp_atual = _get_current_esp_abs(conn, machine_id)
                ts_now = _now_iso()

                # Fecha a bobina atual (ultima aberta)
                try:
                    _close_last_bobina_event(op_id=op_id, ended_at=ts_now, end_abs_pcs=int(esp_atual))
                except Exception:
                    pass

                # Cria eventos para cada bobina adicionada (sequencia crescente)
                base_seq = max(0, len(prev_list))
                for offset, comp in enumerate(added):
                    try:
                        _upsert_bobina_event_start(
                            op_id=op_id,
                            seq=int(base_seq + offset),
                            comprimento_m=int(comp or 0),
                            started_at=ts_now,
                            start_abs_pcs=int(esp_atual),
                        )
                    except Exception:
                        continue
    except Exception:
        pass


    payload = {
        "os": os_,
        "lote": lote,
        "operador": operador,
        "bobina": bobina,
        "gr_fio": gr_fio,
        "observacoes": observacoes,
    }

    try:
        _update_op_row(op_id, payload)
    except Exception:
        return jsonify({"error": "Falha ao atualizar OP no banco"}), 500

    with _op_lock:
        op["os"] = os_
        op["lote"] = lote
        op["operador"] = operador
        op["bobina"] = bobina
        op["bobinas"] = bobinas_list
        op["gr_fio"] = gr_fio
        op["observacoes"] = observacoes
        op_active[machine_id] = op

    return jsonify({"status": "ok", "active": True, "op_id": op_id, "machine_id": machine_id})

# =====================================================
# OP - ENCERRAR (JSON)
# POST /producao/op/encerrar
# Body: { "machine_id": "corpo" }
# =====================================================
@producao_bp.route("/op/encerrar", methods=["POST"])
@login_required
def op_encerrar():
    data = request.get_json(silent=True) or {}
    machine_id = _sanitize_mid(_as_str(data.get("machine_id")))

    if not machine_id:
        return jsonify({"error": "machine_id obrigatorio"}), 400

    with _op_lock:
        op = op_active.get(machine_id)

    if not op:
        return jsonify({"error": "Nao existe OP ativa para esta maquina"}), 404

    ended_at = _now_iso()
    op_id = int(op.get("op_id") or 0)

    # Calculo da OP:
    # - metros = soma das bobinas informadas (metros)
    # - pcs = diferenca do contador absoluto do ESP (esp_atual - baseline_pcs)
    bobinas = op.get("bobinas") or _parse_bobinas_from_str(op.get("bobina") or "") or []
    op_metros = 0
    for b in bobinas:
        try:
            op_metros += int(float(str(b).strip()))
        except Exception:
            continue

    with _get_conn() as conn:
        esp_atual = _get_current_esp_abs(conn, machine_id)

    baseline_pcs = int(((op.get("baseline") or {}).get("pcs")) or 0)
    op_pcs = max(0, int(esp_atual) - int(baseline_pcs))
    # Fecha o evento da ultima bobina no encerramento (fim = encerramento da OP)
    # Isso garante que pcs_total/metro_consumido/tempo fiquem consistentes no historico.
    try:
        if op_id > 0:
            _close_last_bobina_event(op_id=op_id, ended_at=ended_at, end_abs_pcs=int(esp_atual))
    except Exception:
        # Nao bloquear encerramento por falha no evento
        pass

    try:
        conv = float(op.get("op_conv_m_por_pcs") or 0)
    except Exception:
        conv = 0.0
    if conv <= 0:
        conv = _get_conv_m_por_pcs(machine_id)

    try:
        if op_id > 0:
            _close_op_row_v2(op_id, ended_at, op_metros, op_pcs, conv)
    except Exception:
        return jsonify({"error": "Falha ao encerrar OP no banco"}), 500

    # Atualiza historico diario: produzido += op_pcs no dia do encerramento
    try:
        dia_enc = _safe_date_only(ended_at) or _hoje_iso()
        _incrementar_producao_diaria_por_op(machine_id, dia_enc, op_pcs)
    except Exception:
        pass

    with _op_lock:
        op_active.pop(machine_id, None)

    return jsonify(
        {
            "status": "ok",
            "active": False,
            "machine_id": machine_id,
            "ended_at": ended_at,
            "op_metros": op_metros,
            "op_pcs": op_pcs,
            "op_conv_m_por_pcs": conv,
        }
    )



# =====================================================
# OP - SALVAR FECHAMENTO MANUAL (JSON)
# POST /producao/op/salvar
# Body:
# {
#   "op_id": 1,
#   "qtd_mat_bom": 0,
#   "qtd_cost_elas": 0,
#   "refugo": 0,
#   "qtd_saco_caixa": 0,
#   "observacoes": ""
# }
# =====================================================
@producao_bp.route("/op/salvar", methods=["POST"])
@login_required
def op_salvar():
    data = request.get_json(silent=True) or {}

    try:
        op_id = int(data.get("op_id", 0))
    except Exception:
        op_id = 0

    if op_id <= 0:
        return jsonify({"error": "op_id invalido"}), 400

    def _int(v):
        try:
            return int(v)
        except Exception:
            return 0

    observacoes = (data.get("observacoes") or "").strip()

    # Novo formato: salvar por bobina
    bobinas_payload = data.get("bobinas")
    if isinstance(bobinas_payload, list):
        conn = None
        try:
            conn = _get_conn()
            cur = conn.cursor()

            # Garantir colunas legacy na OP (compatibilidade)
            for col, ddl in [
                ("qtd_mat_bom", "INTEGER DEFAULT 0"),
                ("qtd_cost_elas", "INTEGER DEFAULT 0"),
                ("refugo", "INTEGER DEFAULT 0"),
                ("qtd_saco_caixa", "INTEGER DEFAULT 0"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE ordens_producao ADD COLUMN {col} {ddl}")
                except Exception:
                    pass

            # Buscar dados base da OP (bobinas/csv, op_pcs, conv)
            cur.execute(
                """
                SELECT bobina, op_pcs, op_conv_m_por_pcs
                FROM ordens_producao
                WHERE id = ?
                """,
                (op_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "OP nao encontrada"}), 404

            bobina_csv = row[0] or ""
            op_pcs_total = int(row[1] or 0)
            conv = float(row[2] or 0.0)

            bobinas_m = _parse_bobinas_csv(bobina_csv)
            alloc_pcs = _alloc_pcs_by_bobinas(op_pcs_total, bobinas_m, conv)

            # UPSERT por idx
            now_iso = _now_iso()
            sum_mat_bom = 0
            sum_cost = 0
            sum_refugo = 0
            sum_saco = 0

            for item in bobinas_payload:
                if not isinstance(item, dict):
                    continue
                idx = _int(item.get("idx"))
                qtd_cost_elas = _int(item.get("qtd_cost_elas"))
                refugo = _int(item.get("refugo"))
                qtd_saco_caixa = _int(item.get("qtd_saco_caixa"))

                # comprimento e pcs_total derivados da OP (fonte unica)
                comprimento_m = bobinas_m[idx] if idx >= 0 and idx < len(bobinas_m) else 0
                pcs_total = alloc_pcs[idx] if idx >= 0 and idx < len(alloc_pcs) else 0
                metro_consumido = float(pcs_total) * conv if conv > 0 else 0.0

                qtd_mat_bom = int(pcs_total - (qtd_cost_elas + refugo + qtd_saco_caixa))
                if qtd_mat_bom < 0:
                    qtd_mat_bom = 0

                cur.execute(
                    """
                    INSERT INTO ordens_producao_bobinas
                        (op_id, idx, comprimento_m, pcs_total, metro_consumido,
                         qtd_cost_elas, refugo, qtd_saco_caixa, qtd_mat_bom, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(op_id, idx) DO UPDATE SET
                        comprimento_m = excluded.comprimento_m,
                        pcs_total = excluded.pcs_total,
                        metro_consumido = excluded.metro_consumido,
                        qtd_cost_elas = excluded.qtd_cost_elas,
                        refugo = excluded.refugo,
                        qtd_saco_caixa = excluded.qtd_saco_caixa,
                        qtd_mat_bom = excluded.qtd_mat_bom,
                        updated_at = excluded.updated_at
                    """,
                    (
                        op_id,
                        idx,
                        int(comprimento_m or 0),
                        int(pcs_total or 0),
                        float(metro_consumido or 0.0),
                        int(qtd_cost_elas or 0),
                        int(refugo or 0),
                        int(qtd_saco_caixa or 0),
                        int(qtd_mat_bom or 0),
                        now_iso,
                    ),
                )

                sum_mat_bom += int(qtd_mat_bom or 0)
                sum_cost += int(qtd_cost_elas or 0)
                sum_refugo += int(refugo or 0)
                sum_saco += int(qtd_saco_caixa or 0)

            # Atualizar resumo legacy na OP (somas)
            cur.execute(
                """
                UPDATE ordens_producao
                SET qtd_mat_bom = ?,
                    qtd_cost_elas = ?,
                    refugo = ?,
                    qtd_saco_caixa = ?,
                    observacoes = ?
                WHERE id = ?
                """,
                (sum_mat_bom, sum_cost, sum_refugo, sum_saco, observacoes, op_id),
            )

            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            return jsonify({"error": "Falha ao salvar fechamento por bobina"}), 500
        finally:
            if conn:
                conn.close()

        return jsonify({"status": "ok", "op_id": op_id, "mode": "bobinas"})

    # Formato antigo (compatibilidade): salvar direto na OP
    qtd_mat_bom = _int(data.get("qtd_mat_bom"))
    qtd_cost_elas = _int(data.get("qtd_cost_elas"))
    refugo = _int(data.get("refugo"))
    qtd_saco_caixa = _int(data.get("qtd_saco_caixa"))

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # migracao defensiva
        for col, ddl in [
            ("qtd_mat_bom", "INTEGER DEFAULT 0"),
            ("qtd_cost_elas", "INTEGER DEFAULT 0"),
            ("refugo", "INTEGER DEFAULT 0"),
            ("qtd_saco_caixa", "INTEGER DEFAULT 0"),
        ]:
            try:
                cur.execute(f"ALTER TABLE ordens_producao ADD COLUMN {col} {ddl}")
            except Exception:
                pass

        cur.execute(
            """
            UPDATE ordens_producao
            SET qtd_mat_bom = ?,
                qtd_cost_elas = ?,
                refugo = ?,
                qtd_saco_caixa = ?,
                observacoes = ?
            WHERE id = ?
            """,
            (
                qtd_mat_bom,
                qtd_cost_elas,
                refugo,
                qtd_saco_caixa,
                observacoes,
                op_id,
            ),
        )

        if cur.rowcount == 0:
            return jsonify({"error": "OP nao encontrada"}), 404

        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        return jsonify({"error": "Falha ao salvar fechamento da OP"}), 500
    finally:
        if conn:
            conn.close()

    return jsonify({"status": "ok", "op_id": op_id, "mode": "legacy"})

