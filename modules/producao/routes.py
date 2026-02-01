# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\routes.py
# LAST_RECODE: 2026-02-01 17:20 America/Bahia
# MOTIVO: Criar backend de Ordem de Producao (OP) com endpoints status/iniciar/encerrar e persistencia em SQLite (tabela ordens_producao), mantendo compatibilidade com historico existente.

from flask import Blueprint, render_template, redirect, request, jsonify
from datetime import datetime, timedelta
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
DB_PATH = Path("indflow.db")
_op_lock = Lock()

# Uma OP ativa por maquina (em memoria):
# op_active[machine_id] = { ... }
op_active = {}


def _get_conn():
    return sqlite3.connect(DB_PATH)


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

            unidade_1 TEXT,
            unidade_2 TEXT
        )
        """
    )

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


def _insert_op_row(payload: dict) -> int:
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO ordens_producao (
            machine_id, os, lote, operador, bobina, gr_fio, observacoes,
            started_at, ended_at, status,
            baseline_pcs, baseline_u1, baseline_u2,
            unidade_1, unidade_2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            payload.get("unidade_1"),
            payload.get("unidade_2"),
        ),
    )

    conn.commit()
    op_id = int(cur.lastrowid)
    conn.close()
    return op_id


def _close_op_row(op_id: int, ended_at: str):
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE ordens_producao
        SET ended_at = ?, status = ?
        WHERE id = ?
        """,
        (ended_at, "ENCERRADA", int(op_id)),
    )

    conn.commit()
    conn.close()


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

    try:
        rows = listar_historico(machine_id=machine_id, limit=limit)
    except Exception:
        rows = []

    out = []
    for r in rows:
        produzido = int(r.get("produzido", 0) or 0)
        out.append(
            {
                "machine_id": r.get("machine_id", ""),
                "data": r.get("data", ""),
                "produzido": produzido,
                "pecas_boas": produzido,
                "refugo_total": 0,
                "meta": int(r.get("meta", 0) or 0),
                "percentual": int(r.get("percentual", 0) or 0),
            }
        )

    return jsonify(out)


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
            "gr_fio": op.get("gr_fio") or "",
            "observacoes": op.get("observacoes") or "",
            "started_at": op.get("started_at"),
            "baseline": op.get("baseline") or {},
            "unidade_1": op.get("unidade_1") or "",
            "unidade_2": op.get("unidade_2") or "",
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

    bobina = _as_str(data.get("bobina"))
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
        "gr_fio": gr_fio,
        "observacoes": observacoes,
        "started_at": started_at,
        "ended_at": None,
        "status": "ATIVA",
        "baseline_pcs": baseline_pcs,
        "baseline_u1": baseline_u1,
        "baseline_u2": baseline_u2,
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
        "gr_fio": gr_fio,
        "observacoes": observacoes,
        "started_at": started_at,
        "baseline": {"pcs": baseline_pcs, "u1": baseline_u1, "u2": baseline_u2},
        "unidade_1": unidade_1,
        "unidade_2": unidade_2,
    }

    with _op_lock:
        op_active[machine_id] = op_mem

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

    try:
        if op_id > 0:
            _close_op_row(op_id, ended_at)
    except Exception:
        return jsonify({"error": "Falha ao encerrar OP no banco"}), 500

    with _op_lock:
        op_active.pop(machine_id, None)

    return jsonify({"status": "ok", "active": False, "machine_id": machine_id, "ended_at": ended_at})
