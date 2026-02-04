# PATH: modules/producao/routes.py
# LAST_RECODE: 2026-02-04 10:05 America/Bahia
# MOTIVO: Fazer /producao/api/producao/historico delegar para historico_producao_api (machine_routes) para calcular produzido via producao_evento, mantendo a mesma URL usada pelo historico.html.
# INFO: lines_total=1456 lines_changed=~30
from flask import Blueprint, render_template, redirect, request, jsonify
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import sqlite3
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
    Mantem tudo simples e compatível com SQLite.
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

            -- Baselines capturados ao iniciar OP (delta = atual - baseline)
            -- Nesta etapa fica preparado; no proximo passo o front envia valores reais.
            baseline_pcs INTEGER NOT NULL DEFAULT 0,
            baseline_u1 REAL NOT NULL DEFAULT 0,
            baseline_u2 REAL NOT NULL DEFAULT 0,

            -- Totais da OP (calculados no encerramento)
            op_metros INTEGER NOT NULL DEFAULT 0,
            op_pcs INTEGER NOT NULL DEFAULT 0,
            op_conv_m_por_pcs REAL NOT NULL DEFAULT 0,

            unidade_1 TEXT,
            unidade_2 TEXT
        )
        """
    )



    # Migracao simples: adicionar colunas novas se a tabela ja existia.
    # SQLite nao tem "ADD COLUMN IF NOT EXISTS", entao usamos try/except.
    try:
        cur.execute("ALTER TABLE ordens_producao ADD COLUMN op_metros INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE ordens_producao ADD COLUMN op_pcs INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE ordens_producao ADD COLUMN op_conv_m_por_pcs REAL NOT NULL DEFAULT 0")
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
    return datetime.now().isoformat(timespec="seconds")


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


def _fetch_ops_for_range(machine_id: str | None, day_min: str, day_max: str):
    """
    Busca OPs que cruzam o intervalo [day_min, day_max].
    start_day <= day_max AND (end_day >= day_min OR end_day IS NULL).
    """
    conn = _get_conn()
    cur = conn.cursor()

    if machine_id:
        cur.execute(
            """
            SELECT id, machine_id, os, lote, operador, bobina, gr_fio, observacoes, started_at, ended_at, status, op_metros, op_pcs, op_conv_m_por_pcs
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
            SELECT id, machine_id, os, lote, operador, bobina, gr_fio, observacoes, started_at, ended_at, status, op_metros, op_pcs, op_conv_m_por_pcs
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
        ops.append(
            {
                "op_id": int(r[0]),
                "machine_id": r[1] or "",
                "os": r[2] or "",
                "lote": r[3] or "",
                "operador": r[4] or "",
                "bobina": r[5] or "",
                "gr_fio": r[6] or "",
                "observacoes": r[7] or "",
                "started_at": r[8] or "",
                "ended_at": r[9] or "",
                "status": r[10] or "",
                "op_metros": int(r[11] or 0),
                "op_pcs": int(r[12] or 0),
                "op_conv_m_por_pcs": float(r[13] or 0),
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
    # Delegar para o historico do machine_routes (usa producao_evento e fallback por OPs).
    # Mantemos a mesma URL /producao/api/producao/historico para o historico.html.
    try:
        from modules.machine_routes import historico_producao_api as _historico_producao_api
        return _historico_producao_api()
    except Exception:
        # Fallback: usa logica legada deste modulo (producao_diaria/producao_horaria).
        pass

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

    return jsonify(
        {
            "active": True,
            "op_id": op.get("op_id"),
            "machine_id": machine_id,
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

    baseline_in = data.get("baseline") if isinstance(data.get("baseline"), dict) else {}
    try:
        baseline_pcs = int(baseline_in.get("pcs") or 0)
    except Exception:
        baseline_pcs = 0
    try:
        baseline_u1 = float(baseline_in.get("u1") or 0)
    except Exception:
        baseline_u1 = 0.0
    try:
        baseline_u2 = float(baseline_in.get("u2") or 0)
    except Exception:
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

    # Calculo da OP por regra: metros = soma das bobinas informadas (metros).
    # pcs = floor(metros / conv_m_por_pcs da maquina).
    bobinas = op.get("bobinas")
    if not isinstance(bobinas, list) or bobinas is None:
        bobinas = _parse_bobinas_from_str(op.get("bobina") or "") or []

    try:
        op_metros = int(sum(int(x or 0) for x in bobinas))
    except Exception:
        op_metros = 0

    conv = 0.0
    try:
        conv = float(op.get("op_conv_m_por_pcs") or 0)
    except Exception:
        conv = 0.0
    if conv <= 0:
        conv = _get_conv_m_por_pcs(machine_id)

    op_pcs = _calc_pcs_from_metros(op_metros, conv)

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

    return jsonify({"status": "ok", "active": False, "machine_id": machine_id, "ended_at": ended_at, "op_metros": op_metros, "op_pcs": op_pcs, "op_conv_m_por_pcs": conv})
