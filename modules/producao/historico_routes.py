# PATH: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\producao\routes.py
# LAST_RECODE: 2026-02-04 12:00 America/Bahia
# MOTIVO: Registrar historico_bp (modules/producao/historico_routes.py) no producao_bp e corrigir imports ausentes (os).

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

try:
    from modules.db_indflow import init_db
    from modules.machine_state import get_machine
    from modules.machine_calc import calcular_totais
except Exception:
    from .data import init_db, get_machine, calcular_totais  # fallback local

# =====================================================
# HISTORICO (mirror) - blueprint separado
# =====================================================
try:
    from modules.producao.historico_routes import historico_bp
except Exception:
    from .historico_routes import historico_bp

# Inicializa o banco
init_db()

TZ_BAHIA = ZoneInfo("America/Bahia")

producao_bp = Blueprint("producao_bp", __name__, template_folder="templates")

# REGISTRA O ENDPOINT ESPELHO:
# /producao/api/producao/historico  -> historico_routes.py
producao_bp.register_blueprint(historico_bp)


# =====================================================
# PAGINA PRINCIPAL DE PRODUCAO
# =====================================================
@producao_bp.route("/", methods=["GET"])
def producao_home():
    # Exibe um resumo simples de maquinas
    maquinas = []
    for mid in ["maquina01", "maquina02", "maquina03", "maquina04"]:
        try:
            m = get_machine(mid)
            tot = calcular_totais(m)
            maquinas.append(
                {
                    "machine_id": mid,
                    "nome": m.get("nome", mid).upper(),
                    "status_ui": tot.get("status_ui", "—"),
                    "producao_turno": tot.get("producao_turno", 0),
                    "percentual_turno": tot.get("percentual_turno", 0),
                }
            )
        except Exception:
            maquinas.append(
                {
                    "machine_id": mid,
                    "nome": mid.upper(),
                    "status_ui": "—",
                    "producao_turno": 0,
                    "percentual_turno": 0,
                }
            )

    return render_template("producao_home.html", maquinas=maquinas)


# =====================================================
# TELA HISTORICO (HTML)
# =====================================================
@producao_bp.route("/historico", methods=["GET"])
def producao_historico_page():
    # Apenas renderiza a tela; o JS consome:
    # /producao/api/producao/historico
    return render_template("historico.html")


# =====================================================
# EXEMPLO: ENDPOINTS AUXILIARES (mantidos do teu arquivo)
# =====================================================

@producao_bp.route("/api/ping", methods=["GET"])
def api_ping():
    return jsonify({"ok": True, "ts": datetime.now(TZ_BAHIA).isoformat()})


# =====================================================
# UTILITARIOS (mantidos)
# =====================================================

def now_bahia() -> datetime:
    return datetime.now(TZ_BAHIA)


# =====================================================
# FIM
# =====================================================