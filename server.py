from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import sqlite3

# ============================================================
# BLUEPRINTS
# ============================================================
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp
from modules.utilidades.routes import utilidades_bp

app = Flask(__name__)

# ============================================================
# BANCO SQLITE
# ============================================================
DB_PATH = "indflow.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_diaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT,
            data TEXT,
            produzido INTEGER,
            meta INTEGER,
            percentual INTEGER
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ============================================================
# ====================== PRODUÇÃO =============================
# ============================================================

machine_data = {}

def get_machine(machine_id: str):
    if machine_id not in machine_data:
        machine_data[machine_id] = {
            "nome": machine_id.upper(),
            "status": "DESCONHECIDO",

            "meta_turno": 0,
            "turno_inicio": None,
            "turno_fim": None,
            "rampa_percentual": 0,

            "esp_absoluto": 0,
            "baseline_diario": 0,

            "producao_turno": 0,
            "producao_turno_anterior": 0,

            "horas_turno": [],
            "meta_por_hora": [],
            "producao_hora": 0,
            "percentual_hora": 0,
            "ultima_hora": None,

            "percentual_turno": 0,
            "ultimo_dia": datetime.now().date()
        }
    return machine_data[machine_id]

# ============================================================
# RESET + GRAVA HISTÓRICO
# ============================================================
def reset_contexto(m, machine_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual)
        VALUES (?, ?, ?, ?, ?)
    """, (
        machine_id,
        m["ultimo_dia"].isoformat(),
        m["producao_turno"],
        m["meta_turno"],
        m["percentual_turno"]
    ))

    conn.commit()
    conn.close()

    m["baseline_diario"] = m["esp_absoluto"]
    m["producao_turno"] = 0
    m["producao_turno_anterior"] = 0
    m["producao_hora"] = 0
    m["percentual_hora"] = 0
    m["percentual_turno"] = 0
    m["ultima_hora"] = None
    m["ultimo_dia"] = datetime.now().date()

# ============================================================
# UPDATE ESP
# ============================================================
@app.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json()
    machine_id = data.get("machine_id", "maquina01")
    m = get_machine(machine_id)

    if m["ultimo_dia"] != datetime.now().date():
        reset_contexto(m, machine_id)

    m["status"] = data.get("status", "DESCONHECIDO")
    m["esp_absoluto"] = int(data["producao_turno"])

    producao_atual = m["esp_absoluto"] - m["baseline_diario"]
    producao_atual = max(producao_atual, 0)

    m["producao_turno"] = producao_atual

    if m["meta_turno"] > 0:
        m["percentual_turno"] = round(
            (producao_atual / m["meta_turno"]) * 100
        )

    return jsonify({"message": "OK"})

# ============================================================
# STATUS
# ============================================================
@app.route("/machine/status", methods=["GET"])
def machine_status():
    return jsonify(get_machine(request.args.get("machine_id", "maquina01")))

# ============================================================
# HISTÓRICO (COM FILTROS)
# ============================================================
@app.route("/producao/historico", methods=["GET"])
def historico_producao():
    machine_id = request.args.get("machine_id")
    inicio = request.args.get("inicio")
    fim = request.args.get("fim")

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

    return jsonify([
        {
            "machine_id": r["machine_id"],
            "data": r["data"],
            "produzido": r["produzido"],
            "meta": r["meta"],
            "percentual": r["percentual"]
        } for r in rows
    ])

# ============================================================
# BLUEPRINTS
# ============================================================
app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")
app.register_blueprint(utilidades_bp, url_prefix="/utilidades")

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
