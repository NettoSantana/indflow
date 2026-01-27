# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\routes.py
# LAST_RECODE: 2026-01-27 19:30 America/Bahia
# MOTIVO: Conectar a tela de Historico a uma API real (/api/producao/historico) e habilitar persistencia basica em SQLite (init_db + salvar/listar)

from flask import Blueprint, render_template, redirect, request, jsonify
from datetime import datetime, timedelta

# =====================================================
# AUTH
# =====================================================
from modules.admin.routes import login_required

# =====================================================
# DATA (SQLite)
# =====================================================
# Observacao: este modulo existe em modules/producao/data.py
# e contem init_db, salvar_producao_diaria e listar_historico.
try:
    from modules.producao.data import init_db, salvar_producao_diaria, listar_historico
except Exception:
    # fallback caso o Python esteja resolvendo pacotes de forma diferente
    from .data import init_db, salvar_producao_diaria, listar_historico

# Inicializa o banco ao carregar o modulo
try:
    init_db()
except Exception:
    # Se falhar, a API ainda sobe; mas o historico nao vai persistir.
    # Nao levantamos excecao aqui para nao derrubar o app por conta de DB.
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

    # Adapter de campos para o front
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

    Observacao: se voce ja tem outro ponto do sistema que calcula produzido/meta,
    pode chamar esse endpoint quando quiser "fechar o dia" ou em qualquer update.
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

    # -------------------------------------------------
    # CALCULO DE HORAS DO TURNO
    # -------------------------------------------------
    fmt = "%H:%M"
    inicio = datetime.strptime(hora_inicio, fmt)
    fim = datetime.strptime(hora_fim, fmt)

    if fim <= inicio:
        return jsonify({"error": "Hora fim deve ser maior que inicio"}), 400

    horas_totais = int((fim - inicio).total_seconds() / 3600)

    if horas_totais <= 0:
        return jsonify({"error": "Turno invalido"}), 400

    # -------------------------------------------------
    # CALCULO DE METAS
    # -------------------------------------------------
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

    # -------------------------------------------------
    # SALVA NO CONTEXTO
    # -------------------------------------------------
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
