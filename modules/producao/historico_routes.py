# PATH: modules/producao/historico_routes.py
# LAST_RECODE: 2026-02-04 13:05 America/Bahia
# MOTIVO: Corrigir cabeçalho (PATH) e alinhar histórico ao mesmo critério do dashboard (producao_evento).

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

from flask import Blueprint, jsonify, render_template, request

try:
    from modules.db_indflow import init_db
    from modules.machine_state import get_machine
    from modules.machine_calc import calcular_totais
except Exception:
    # fallback local (ambiente alternativo)
    from .data import init_db, get_machine, calcular_totais


# =====================================================
# CONFIG
# =====================================================
TZ_BAHIA = ZoneInfo("America/Bahia")

# Blueprint exclusivo do histórico
historico_bp = Blueprint(
    "historico_bp",
    __name__,
    template_folder="templates"
)

# Inicializa DB (idempotente)
init_db()


# =====================================================
# API – HISTÓRICO (JSON)
# Usa o MESMO critério do dashboard
# =====================================================
@historico_bp.route("/api/producao/historico", methods=["GET"])
def api_producao_historico():
    machine_id = request.args.get("machine_id")
    days = int(request.args.get("days", 10))

    if not machine_id:
        return jsonify({"ok": False, "error": "machine_id obrigatório"}), 400

    hoje = datetime.now(TZ_BAHIA).date()
    inicio = hoje - timedelta(days=days - 1)

    dados = []

    for i in range(days):
        dia = inicio + timedelta(days=i)

        try:
            machine = get_machine(machine_id)
            totais = calcular_totais(machine, data_ref=dia)

            dados.append({
                "data": dia.isoformat(),
                "produzido": totais.get("producao_dia", 0),
                "pecas_boas": totais.get("pecas_boas", totais.get("producao_dia", 0)),
                "refugo": totais.get("refugo_dia", 0),
                "meta": totais.get("meta_dia"),
                "percentual": totais.get("percentual_dia"),
            })

        except Exception as e:
            dados.append({
                "data": dia.isoformat(),
                "produzido": 0,
                "pecas_boas": 0,
                "refugo": 0,
                "meta": None,
                "percentual": None,
                "erro": str(e),
            })

    return jsonify({
        "ok": True,
        "machine_id": machine_id,
        "dados": dados
    })


# =====================================================
# TELA HTML – HISTÓRICO
# =====================================================
@historico_bp.route("/historico", methods=["GET"])
def historico_page():
    # A página consome a API acima via JS
    return render_template("historico.html")
