# PATH: indflow/server.py
# LAST_RECODE: 2026-02-19 20:20 America/Bahia
# MOTIVO: Corrigir NameError ProxyFix no Railway (import ausente) mantendo ajuste de cookies/HTTPS.

import os
import logging
import sqlite3
from flask import Flask, render_template, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

# ============================================================
# BLUEPRINTS (já existentes)
# ============================================================
from modules.producao.routes import producao_bp
from modules.manutencao.routes import manutencao_bp
from modules.ativos.routes import ativos_bp
from modules.admin.routes import admin_bp
from modules.api.routes import api_bp
from modules.devices.routes import devices_bp
from modules.utilidades.routes import utilidades_bp

# ============================================================
# NOVO: CLIENTES (TENANT)
# ============================================================
from modules.clientes.routes import clientes_bp

# ============================================================
# NOVOS MÓDULOS (extraídos do server)
# ============================================================
from modules.db_indflow import init_db
from modules.machine_routes import machine_bp

# ============================================================
# LOGGING (mínimo, para Railway)
# ============================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("indflow")

app = Flask(__name__)

# ============================================================
# SESSÃO / LOGIN (base)
# ============================================================
# Necessário para session/cookies (login web).
# Configure a env var FLASK_SECRET_KEY (string longa e secreta).
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-inseguro-trocar")

# ============================================================
# RAILWAY / HTTPS (PROXY) + COOKIES
# ============================================================
# No Railway o app fica atrás de proxy HTTPS. Sem ProxyFix, Flask pode
# acreditar que o esquema é http e gerar redirects incorretos (perdendo
# cookie de sessão), fazendo o usuário voltar para /admin/login.
#
# ProxyFix lê X-Forwarded-* e ajusta request.scheme/host/remote_addr.
# Ajustamos também configs de cookie e scheme preferencial.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

_is_railway = bool(
    os.getenv("RAILWAY_ENVIRONMENT")
    or os.getenv("RAILWAY_PROJECT_ID")
    or os.getenv("RAILWAY_SERVICE_ID")
    or os.getenv("RAILWAY_PUBLIC_DOMAIN")
)

_preferred_scheme = os.getenv("PREFERRED_URL_SCHEME")
if not _preferred_scheme:
    _preferred_scheme = "https" if _is_railway else "http"

# Cookies: em produção HTTPS, cookie seguro evita bloqueios modernos.
# SameSite=Lax é bom para navegação normal e redirects internos.
_cookie_samesite = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
_cookie_secure_env = os.getenv("SESSION_COOKIE_SECURE")
if _cookie_secure_env is None:
    _cookie_secure = (_preferred_scheme == "https")
else:
    _cookie_secure = _cookie_secure_env.strip().lower() in ("1", "true", "yes", "on")

app.config.update(
    PREFERRED_URL_SCHEME=_preferred_scheme,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=_cookie_samesite,
    SESSION_COOKIE_SECURE=_cookie_secure,
)

# ============================================================
# BANCO SQLITE
# ============================================================
def _log_db_context():
    db_env = os.getenv("INDFLOW_DB_PATH")
    cwd = os.getcwd()
    log.info("startup: CWD=%s", cwd)
    log.info("startup: INDFLOW_DB_PATH=%s", db_env)
    log.info("startup: PREFERRED_URL_SCHEME=%s", app.config.get("PREFERRED_URL_SCHEME"))
    log.info("startup: SESSION_COOKIE_SECURE=%s", app.config.get("SESSION_COOKIE_SECURE"))
    log.info("startup: SESSION_COOKIE_SAMESITE=%s", app.config.get("SESSION_COOKIE_SAMESITE"))

_log_db_context()

try:
    log.info("startup: init_db() begin")
    init_db()
    log.info("startup: init_db() ok")
except Exception:
    log.exception("startup: init_db() failed")
    raise

# ============================================================
# LOG REQUESTS (mínimo)
# ============================================================
@app.after_request
def _log_request(response):
    try:
        xfwd = request.headers.get("X-Forwarded-For", "")
        addr = xfwd.split(",")[0].strip() if xfwd else (request.remote_addr or "")
        log.info("http: %s %s %s ip=%s", request.method, request.path, response.status_code, addr)
    except Exception:
        # Nunca quebrar request por causa de log.
        pass
    return response

# ============================================================
# ADMIN: DB CHECK (TEMPORÁRIO)
# ============================================================
def _admin_token_ok() -> bool:
    # Aceita token via query (?token=) ou header X-Admin-Token.
    token_in = (request.args.get("token") or "").strip()
    if not token_in:
        token_in = (request.headers.get("X-Admin-Token") or "").strip()

    # Tokens aceitos (Railway Variables):
    # - INDFLOW_ADMIN_TOKEN (preferencial)
    # - ADMIN_RESET_TOKEN (fallback, já existe no projeto)
    expected = (os.getenv("INDFLOW_ADMIN_TOKEN") or "").strip()
    if not expected:
        expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()

    if not expected:
        # Sem token configurado: não expor endpoint.
        return False

    return token_in == expected


def get_db_path() -> str:
    # Banco usado pelo app no Railway (volume /data).
    return os.getenv("INDFLOW_DB_PATH", "/data/indflow.db")

def _check_admin_auth():
    """
    Valida token administrativo para endpoints sensíveis.
    Retorna None quando autorizado; caso contrário, retorna (json, status).
    """
    if _admin_token_ok():
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401


def _pragma_table_info(cur, table: str):
    try:
        return cur.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return []


def _columns_from_pragma(pragma_rows):
    # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in (pragma_rows or []) if len(r) >= 2]


def _pick_first(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None


@app.get("/admin/db-check")
def admin_db_check():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    db_path = os.getenv("INDFLOW_DB_PATH", "/data/indflow.db")
    machine_id = (request.args.get("machine_id") or "maquina02").strip()
    days_limit = int((request.args.get("days") or "10").strip() or "10")
    if days_limit < 1:
        days_limit = 1
    if days_limit > 31:
        days_limit = 31

    out = {
        "ok": True,
        "db_path": db_path,
        "machine_id": machine_id,
        "tables": [],
        "counts": {},
        "samples": {},
        "compare": [],
        "errors": [],
    }

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        tables = [r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()]
        out["tables"] = tables

        def safe_count(tbl: str):
            try:
                n = cur.execute(f"SELECT COUNT(1) FROM {tbl}").fetchone()[0]
                out["counts"][tbl] = int(n)
            except Exception as e:
                out["counts"][tbl] = None
                out["errors"].append(f"count {tbl}: {e}")

        for t in [
            "producao_diaria",
            "producao_horaria",
            "producao_evento",
            "machine_config",
            "devices",
            "usuarios",
            "clientes",
        ]:
            if t in tables:
                safe_count(t)

        # Schema (para debugar nome de colunas no Railway)
        if "producao_evento" in tables:
            try:
                pe_info = _pragma_table_info(cur, "producao_evento")
                out["samples"]["producao_evento_schema"] = pe_info
            except Exception as e:
                out["errors"].append(f"schema producao_evento: {e}")

        # Samples maquina (últimos N)
        if "producao_horaria" in tables:
            try:
                rows = cur.execute(
                    "SELECT data_ref, machine_id, hora_idx, produzido, meta, updated_at "
                    "FROM producao_horaria "
                    "WHERE machine_id=? "
                    "ORDER BY data_ref DESC, hora_idx DESC "
                    "LIMIT 20",
                    (machine_id,),
                ).fetchall()
                out["samples"][f"producao_horaria_{machine_id}"] = rows
            except Exception as e:
                out["errors"].append(f"sample producao_horaria: {e}")

        if "producao_diaria" in tables:
            try:
                rows = cur.execute(
                    "SELECT data, machine_id, produzido, meta, percentual "
                    "FROM producao_diaria "
                    "WHERE machine_id=? "
                    "ORDER BY data DESC "
                    "LIMIT 20",
                    (machine_id,),
                ).fetchall()
                out["samples"][f"producao_diaria_{machine_id}"] = rows
            except Exception as e:
                out["errors"].append(f"sample producao_diaria: {e}")

        # ============================================================
        # COMPARATIVO POR DIA: sum(producao_horaria) x producao_diaria
        # + contagem de producao_evento (se existir) no mesmo dia
        # ============================================================
        days = []
        if "producao_diaria" in tables:
            try:
                days = [r[0] for r in cur.execute(
                    "SELECT DISTINCT data FROM producao_diaria "
                    "WHERE machine_id=? "
                    "ORDER BY data DESC "
                    "LIMIT ?",
                    (machine_id, days_limit),
                ).fetchall()]
            except Exception as e:
                out["errors"].append(f"days from producao_diaria: {e}")
                days = []

        if not days and "producao_horaria" in tables:
            try:
                days = [r[0] for r in cur.execute(
                    "SELECT DISTINCT data_ref FROM producao_horaria "
                    "WHERE machine_id=? "
                    "ORDER BY data_ref DESC "
                    "LIMIT ?",
                    (machine_id, days_limit),
                ).fetchall()]
            except Exception as e:
                out["errors"].append(f"days from producao_horaria: {e}")
                days = []

        # Se ainda não tiver dias, usa hoje/ontem (Bahia) como fallback.
        if not days:
            try:
                today = cur.execute("SELECT date('now','-3 hours')").fetchone()[0]
                yesterday = cur.execute("SELECT date('now','-3 hours','-1 day')").fetchone()[0]
                days = [today, yesterday]
            except Exception:
                days = []

        # Prepara forma de agrupar producao_evento por dia (se existir)
        evento_day_expr = None
        evento_ts_col = None
        if "producao_evento" in tables:
            pe_cols = _columns_from_pragma(_pragma_table_info(cur, "producao_evento"))
            # Escolha de coluna de data/tempo
            evento_ts_col = _pick_first(pe_cols, ["ts_ms", "ts", "timestamp", "created_at", "updated_at", "data_ref", "data"])
            if evento_ts_col == "ts_ms":
                # Ajuste Bahia: subtrai 3h de UTC (assumindo ts_ms em UTC)
                evento_day_expr = "date(datetime(ts_ms/1000,'unixepoch','-3 hours'))"
            elif evento_ts_col in ("ts", "timestamp"):
                # Tenta tratar como ISO: date(ts)
                evento_day_expr = f"date({evento_ts_col})"
            elif evento_ts_col in ("created_at", "updated_at"):
                evento_day_expr = f"date({evento_ts_col})"
            elif evento_ts_col in ("data_ref", "data"):
                evento_day_expr = f"{evento_ts_col}"
            else:
                evento_day_expr = None

        for d in days:
            item = {"day": d}

            # Diário (se existir)
            if "producao_diaria" in tables:
                try:
                    row = cur.execute(
                        "SELECT produzido, meta, percentual "
                        "FROM producao_diaria "
                        "WHERE machine_id=? AND data=? "
                        "LIMIT 1",
                        (machine_id, d),
                    ).fetchone()
                    if row:
                        item["diaria_produzido"] = row[0]
                        item["diaria_meta"] = row[1]
                        item["diaria_percentual"] = row[2]
                    else:
                        item["diaria_produzido"] = None
                        item["diaria_meta"] = None
                        item["diaria_percentual"] = None
                except Exception as e:
                    out["errors"].append(f"diaria day={d}: {e}")

            # Horária soma/contagem
            if "producao_horaria" in tables:
                try:
                    row = cur.execute(
                        "SELECT COALESCE(SUM(produzido),0), COUNT(1), COALESCE(MAX(meta),0) "
                        "FROM producao_horaria "
                        "WHERE machine_id=? AND data_ref=?",
                        (machine_id, d),
                    ).fetchone()
                    if row:
                        item["horaria_soma"] = row[0]
                        item["horaria_linhas"] = row[1]
                        item["horaria_meta_max"] = row[2]
                except Exception as e:
                    out["errors"].append(f"horaria day={d}: {e}")

            # Diferença direta (se ambos existirem)
            if item.get("diaria_produzido") is not None and item.get("horaria_soma") is not None:
                try:
                    item["diff_diaria_menos_horaria"] = int(item["diaria_produzido"]) - int(item["horaria_soma"])
                except Exception:
                    item["diff_diaria_menos_horaria"] = None

            # Evento contagem + amostra curta
            if "producao_evento" in tables and evento_day_expr and evento_ts_col:
                try:
                    # Contagem
                    q = (
                        f"SELECT COUNT(1) FROM producao_evento "
                        f"WHERE machine_id=? AND {evento_day_expr}=?"
                    )
                    item["evento_count"] = int(cur.execute(q, (machine_id, d)).fetchone()[0])

                    # Amostra (máx 5) para ver se está caindo no dia certo
                    # Seleciona até 6 colunas úteis
                    pe_cols = _columns_from_pragma(_pragma_table_info(cur, "producao_evento"))
                    keep = []
                    for c in ["machine_id", evento_ts_col, "ts_ms", "data_ref", "data", "produzido", "delta", "count", "valor", "op_id", "bp_id"]:
                        if c in pe_cols and c not in keep:
                            keep.append(c)
                        if len(keep) >= 6:
                            break
                    if not keep:
                        keep = pe_cols[:6]

                    col_sql = ", ".join(keep)
                    q2 = (
                        f"SELECT {col_sql} FROM producao_evento "
                        f"WHERE machine_id=? AND {evento_day_expr}=? "
                        f"ORDER BY rowid DESC LIMIT 5"
                    )
                    item["evento_sample_cols"] = keep
                    item["evento_sample"] = cur.execute(q2, (machine_id, d)).fetchall()
                except Exception as e:
                    out["errors"].append(f"evento day={d}: {e}")

            out["compare"].append(item)

        conn.close()

    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)

    return jsonify(out)

# ============================================================
# ROTAS PRINCIPAIS
# ============================================================

@app.post("/admin/purge-production")
def admin_purge_production():
    """
    Apaga definitivamente dados operacionais das tabelas de producao/estado, preservando cadastros.
    Tabelas alvo (conforme autorizacao do usuario):
      - producao_evento, producao_horaria, producao_diaria
      - machine_count_state, machine_stop
      - nao_programado_diario, nao_programado_horaria
      - refugo_horaria

    Body (JSON) opcional:
      { "machine_id": "maquina02" }  # se informado, tenta apagar apenas dessa maquina quando a tabela tiver a coluna machine_id
    """
    auth = _check_admin_auth()
    if auth is not None:
        return auth

    payload = request.get_json(silent=True) or {}
    machine_id = (payload.get("machine_id") or "").strip() or None

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    tables = [
        "producao_evento",
        "producao_horaria",
        "producao_diaria",
        "machine_count_state",
        "machine_stop",
        "nao_programado_diario",
        "nao_programado_horaria",
        "refugo_horaria",
    ]

    deleted = {}
    errors = []

    def _table_has_column(table_name: str, col: str) -> bool:
        try:
            cols = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
            return any(r[1] == col for r in cols)
        except Exception:
            return False

    for t in tables:
        try:
            if machine_id and _table_has_column(t, "machine_id"):
                cur.execute(f"DELETE FROM {t} WHERE machine_id = ?", (machine_id,))
            else:
                cur.execute(f"DELETE FROM {t}")
            deleted[t] = cur.rowcount
        except Exception as e:
            errors.append({"table": t, "error": str(e)})

    # Zera AUTOINCREMENT (SQLite) para as tabelas que usam sqlite_sequence
    try:
        cur.execute(
            "DELETE FROM sqlite_sequence WHERE name IN ({})".format(",".join("?" for _ in tables)),
            tuple(tables),
        )
    except Exception:
        pass

    conn.commit()
    conn.close()

    note = "Purge executado. Dados operacionais apagados."
    if machine_id:
        note += f" machine_id={machine_id}"

    return jsonify({
        "ok": len(errors) == 0,
        "db_path": db_path,
        "machine_id": machine_id,
        "deleted": deleted,
        "errors": errors,
        "note": note,
    })

@app.route("/")
def index():
    return render_template("index.html")

# ============================================================
# BLUEPRINTS
# ============================================================
app.register_blueprint(machine_bp)  # mantém /machine/*, /admin/reset-manual, /producao/historico

app.register_blueprint(producao_bp, url_prefix="/producao")
app.register_blueprint(manutencao_bp, url_prefix="/manutencao")
app.register_blueprint(ativos_bp, url_prefix="/ativos")
app.register_blueprint(admin_bp, url_prefix="/admin")
app.register_blueprint(clientes_bp, url_prefix="/clientes")
app.register_blueprint(api_bp, url_prefix="/api")
app.register_blueprint(devices_bp, url_prefix="/devices")
app.register_blueprint(utilidades_bp, url_prefix="/utilidades")
###