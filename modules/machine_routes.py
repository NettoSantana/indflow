# modules/machine_routes.py
import os
import hashlib
from flask import Blueprint, request, jsonify, render_template
from datetime import datetime, timedelta

from modules.db_indflow import get_db
from modules.machine_state import get_machine
from modules.machine_calc import (
    aplicar_unidades,
    salvar_conversao,
    atualizar_producao_hora,
    verificar_reset_diario,
    reset_contexto,
    calcular_ultima_hora_idx,
    calcular_tempo_medio,
    aplicar_derivados_ml,
    carregar_baseline_diario,
    now_bahia,
    dia_operacional_ref_str,
)

from modules.repos.machine_config_repo import upsert_machine_config
from modules.repos.refugo_repo import load_refugo_24, upsert_refugo

# ✅ helpers de device (já criado por você)
from modules.machine.device_helpers import (
    norm_device_id,
    touch_device_seen,
    get_machine_from_device,
)

machine_bp = Blueprint("machine_bp", __name__)


def _norm_machine_id(v):
    v = (v or "").strip().lower()
    return v or "maquina01"


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _sum_refugo_24(machine_id: str, dia_ref: str) -> int:
    try:
        arr = load_refugo_24(_norm_machine_id(machine_id), (dia_ref or "").strip())
        if not isinstance(arr, list):
            return 0
        return sum(_safe_int(x, 0) for x in arr)
    except Exception:
        return 0


def _admin_token_ok() -> bool:
    """
    Proteção simples:
      - Configure no Railway/ENV: INDFLOW_ADMIN_TOKEN=<seu_token>
      - Envie no header: X-Admin-Token: <seu_token>
    """
    expected = (os.getenv("INDFLOW_ADMIN_TOKEN") or "").strip()
    if not expected:
        return False
    received = (request.headers.get("X-Admin-Token") or "").strip()
    return received == expected


def _get_cliente_from_api_key() -> dict | None:
    """
    AUTH do ESP por header X-API-Key:
      - calcula SHA256(api_key)
      - compara com clientes.api_key_hash
      - exige status 'active'
    Retorna dict {id, nome, status} ou None.
    """
    api_key = (request.headers.get("X-API-Key") or "").strip()
    if not api_key:
        return None

    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, nome, status FROM clientes WHERE api_key_hash = ?",
            (api_key_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None
        if (row["status"] or "").strip().lower() != "active":
            return None
        return {"id": row["id"], "nome": row["nome"], "status": row["status"]}
    finally:
        conn.close()


# ============================================================
# MULTI-TENANT (DEVICES) — helpers locais
# ============================================================
def _ensure_devices_table_min(conn):
    """
    Segurança: garante tabela devices e colunas cliente_id/created_at.
    (No ideal, isso fica em init_db; aqui é só para não quebrar ambientes antigos.)
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            machine_id TEXT,
            alias TEXT,
            last_seen TEXT
        )
    """)

    # tenta adicionar colunas (não quebra se já existirem)
    try:
        conn.execute("ALTER TABLE devices ADD COLUMN cliente_id TEXT")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE devices ADD COLUMN created_at TEXT")
    except Exception:
        pass

    # índices (não quebra se já existir)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_devices_cliente_id ON devices(cliente_id)")
    except Exception:
        pass

    conn.commit()


def _device_owner_cliente_id(conn, device_id: str) -> str | None:
    try:
        cur = conn.execute("SELECT cliente_id FROM devices WHERE device_id = ? LIMIT 1", (device_id,))
        row = cur.fetchone()
        if not row:
            return None
        return (row["cliente_id"] if isinstance(row, sqlite3.Row) else row[0])  # type: ignore
    except Exception:
        # fallback seguro
        try:
            cur = conn.execute("SELECT cliente_id FROM devices WHERE device_id = ? LIMIT 1", (device_id,))
            row = cur.fetchone()
            if not row:
                return None
            try:
                return row["cliente_id"]
            except Exception:
                return row[0]
        except Exception:
            return None


def _upsert_device_for_cliente(device_id: str, cliente_id: str, now_str: str) -> bool:
    """
    Garante que o device (MAC) fique amarrado ao cliente.
    Regra:
      - se device já pertence a outro cliente -> bloqueia (False)
      - se não existe -> cria com cliente_id + created_at + last_seen
      - se existe sem cliente_id -> seta cliente_id
      - sempre atualiza last_seen
    """
    conn = get_db()
    try:
        _ensure_devices_table_min(conn)

        cur = conn.execute("SELECT device_id, cliente_id FROM devices WHERE device_id = ? LIMIT 1", (device_id,))
        row = cur.fetchone()

        if row is None:
            conn.execute("""
                INSERT INTO devices (device_id, cliente_id, machine_id, alias, created_at, last_seen)
                VALUES (?, ?, NULL, NULL, ?, ?)
            """, (device_id, cliente_id, now_str, now_str))
            conn.commit()
            return True

        try:
            owner = row["cliente_id"]
        except Exception:
            owner = row[1] if len(row) > 1 else None

        # se já tem dono e é diferente => bloqueia
        if owner and owner != cliente_id:
            return False

        # atualiza last_seen e seta cliente_id se estiver vazio
        conn.execute("""
            UPDATE devices
               SET last_seen = ?,
                   cliente_id = COALESCE(cliente_id, ?)
             WHERE device_id = ?
        """, (now_str, cliente_id, device_id))
        conn.commit()
        return True

    finally:
        conn.close()


def _get_linked_machine_for_cliente(device_id: str, cliente_id: str) -> str | None:
    """
    Resolve machine_id vinculado ao device, mas garantindo o tenant.
    """
    conn = get_db()
    try:
        _ensure_devices_table_min(conn)
        cur = conn.execute("""
            SELECT machine_id
              FROM devices
             WHERE device_id = ?
               AND cliente_id = ?
             LIMIT 1
        """, (device_id, cliente_id))
        row = cur.fetchone()
        if not row:
            return None
        try:
            return (row["machine_id"] or None)
        except Exception:
            return (row[0] or None)
    finally:
        conn.close()


# ============================================================
# BASELINE (agora com suporte a cliente_id quando existir)
# ============================================================
def _ensure_baseline_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS baseline_diario (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            dia_ref TEXT NOT NULL,
            baseline_esp INTEGER NOT NULL,
            esp_last INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # tenta adicionar cliente_id (migração leve)
    try:
        conn.execute("ALTER TABLE baseline_diario ADD COLUMN cliente_id TEXT")
    except Exception:
        pass

    # índices (mantém o antigo e tenta criar o novo por cliente)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario
        ON baseline_diario(machine_id, dia_ref)
    """)
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_cliente
            ON baseline_diario(cliente_id, machine_id, dia_ref)
        """)
    except Exception:
        pass

    conn.commit()


def _has_baseline_for_day(conn, machine_id: str, dia_ref: str, cliente_id: str | None) -> bool:
    try:
        # se tiver cliente_id, filtra
        if cliente_id:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id=? LIMIT 1",
                (machine_id, dia_ref, cliente_id),
            )
        else:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? LIMIT 1",
                (machine_id, dia_ref),
            )
        return cur.fetchone() is not None
    except Exception:
        return False


def _insert_baseline_for_day(conn, machine_id: str, dia_ref: str, esp_abs: int, updated_at: str, cliente_id: str | None):
    """
    Ancora baseline inicial para máquina nova / primeiro vínculo:
    baseline_esp = esp_abs atual
    esp_last     = esp_abs atual
    """
    if cliente_id:
        conn.execute("""
            INSERT OR IGNORE INTO baseline_diario (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (cliente_id, machine_id, dia_ref, int(esp_abs), int(esp_abs), updated_at))
    else:
        conn.execute("""
            INSERT OR IGNORE INTO baseline_diario (machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (machine_id, dia_ref, int(esp_abs), int(esp_abs), updated_at))
    conn.commit()


# ============================================================
# ADMIN - HARD RESET (LIMPA BANCO)
# ============================================================
@machine_bp.route("/admin/hard-reset", methods=["POST"])
def admin_hard_reset():
    """
    HARD RESET: apaga TODOS os dados do SQLite (tabelas principais).
    Não depende de shell do Railway.
    """
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    conn = get_db()
    cur = conn.cursor()

    tables = [
        "producao_diaria",
        "producao_horaria",
        "baseline_diario",
        "refugo_horaria",
        "machine_config",
        # OBS: não apagamos "devices" aqui por padrão (cadastro do hardware)
    ]

    deleted = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            before = cur.fetchone()[0]
        except Exception:
            before = None

        try:
            cur.execute(f"DELETE FROM {t}")
            deleted[t] = before
        except Exception:
            deleted[t] = "skipped"

    try:
        cur.execute("DELETE FROM sqlite_sequence")
    except Exception:
        pass

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "deleted_tables": deleted,
        "note": "Banco limpo. Recomece a contagem a partir do próximo envio do ESP."
    })


# ============================================================
# CONFIGURAÇÃO DA MÁQUINA
# ============================================================
@machine_bp.route("/machine/config", methods=["POST"])
def configurar_maquina():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    meta_turno = int(data["meta_turno"])
    rampa = int(data["rampa"])

    m["meta_turno"] = meta_turno
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = rampa

    aplicar_unidades(m, data.get("unidade_1"), data.get("unidade_2"))
    salvar_conversao(m, data)

    inicio = datetime.strptime(m["turno_inicio"], "%H:%M")
    fim = datetime.strptime(m["turno_fim"], "%H:%M")

    if fim <= inicio:
        fim += timedelta(days=1)

    horas = []
    atual = inicio
    while atual < fim:
        proxima = atual + timedelta(hours=1)
        horas.append(f"{atual.strftime('%H:%M')} - {proxima.strftime('%H:%M')}")
        atual = proxima

    m["horas_turno"] = horas

    qtd_horas = len(horas)
    metas = []
    if qtd_horas > 0:
        meta_base = meta_turno / qtd_horas

        meta_primeira = round(meta_base * (rampa / 100))
        restante = meta_turno - meta_primeira
        horas_restantes = qtd_horas - 1

        metas = [meta_primeira]

        if horas_restantes > 0:
            meta_restante_base = restante // horas_restantes
            sobra = restante % horas_restantes

            for i in range(horas_restantes):
                valor = meta_restante_base + (1 if i < sobra else 0)
                metas.append(valor)

    m["meta_por_hora"] = metas

    m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
    m["ultima_hora"] = calcular_ultima_hora_idx(m)
    m["producao_hora"] = 0
    m["percentual_hora"] = 0

    try:
        upsert_machine_config(machine_id, m)
    except Exception:
        pass

    return jsonify({
        "status": "configurado",
        "machine_id": machine_id,
        "meta_por_hora": m["meta_por_hora"],
        "unidade_1": m.get("unidade_1"),
        "unidade_2": m.get("unidade_2"),
        "conv_m_por_pcs": m.get("conv_m_por_pcs")
    })


# ============================================================
# UPDATE ESP  ✅ AQUI ESTÁ A REGRA DO "ZERAR AO VINCULAR"
# ============================================================
@machine_bp.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json() or {}

    # ✅ AUTH (ESP): exige X-API-Key e resolve o cliente
    cliente = _get_cliente_from_api_key()
    if not cliente:
        return jsonify({"error": "unauthorized"}), 401

    cliente_id = cliente["id"]

    # 1) MAC do ESP (CPF)
    device_id = norm_device_id(data.get("mac") or data.get("device_id") or "")
    if device_id:
        # atualiza last_seen (compat) — e amarra ao cliente (multi-tenant)
        agora = now_bahia()
        now_str = agora.strftime("%Y-%m-%d %H:%M:%S")

        ok_owner = _upsert_device_for_cliente(device_id=device_id, cliente_id=cliente_id, now_str=now_str)
        if not ok_owner:
            return jsonify({"error": "device pertence a outro cliente"}), 403

        # mantém o helper legado (não quebra)
        try:
            touch_device_seen(device_id)
        except Exception:
            pass

    # 2) Resolve machine_id com filtro por cliente_id (evita “vazamento”)
    linked_machine = _get_linked_machine_for_cliente(device_id, cliente_id) if device_id else None
    if linked_machine:
        machine_id = _norm_machine_id(linked_machine)
    else:
        machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    m = get_machine(machine_id)

    verificar_reset_diario(m, machine_id)

    m["status"] = data.get("status", "DESCONHECIDO")
    m["esp_absoluto"] = int(data.get("producao_turno", 0) or 0)

    # ✅ FIX: persistir RUN (1/0) vindo do ESP no estado da máquina
    m["run"] = _safe_int(data.get("run", 0), 0)

    # ========================================================
    # ✅ REGRA: PRIMEIRO UPDATE REAL (MÁQUINA NOVA SEM BASELINE)
    # ========================================================
    baseline_initialized = False
    try:
        agora = now_bahia()
        dia_ref = dia_operacional_ref_str(agora)
        updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")

        conn = get_db()
        try:
            _ensure_baseline_table(conn)

            if not _has_baseline_for_day(conn, machine_id, dia_ref, cliente_id):
                _insert_baseline_for_day(conn, machine_id, dia_ref, int(m["esp_absoluto"]), updated_at, cliente_id)
                baseline_initialized = True
        finally:
            conn.close()

    except Exception:
        baseline_initialized = False

    carregar_baseline_diario(m, machine_id)

    producao_atual = max(int(m.get("esp_absoluto", 0) or 0) - int(m.get("baseline_diario", 0) or 0), 0)
    m["producao_turno"] = producao_atual

    if int(m.get("meta_turno", 0) or 0) > 0:
        m["percentual_turno"] = round((producao_atual / m["meta_turno"]) * 100)
    else:
        m["percentual_turno"] = 0

    atualizar_producao_hora(m)

    return jsonify({
        "message": "OK",
        "machine_id": machine_id,
        "cliente_id": cliente_id,
        "device_id": device_id or None,
        "linked_machine": linked_machine or None,
        "baseline_initialized": bool(baseline_initialized),
    })


# ============================================================
# ADMIN - RESET SOMENTE DA HORA (REANCORA BASELINE_HORA)
# ============================================================
@machine_bp.route("/admin/reset-hour", methods=["POST"])
def admin_reset_hour():
    if not _admin_token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    try:
        carregar_baseline_diario(m, machine_id)
    except Exception:
        pass

    try:
        prod_turno = int(m.get("producao_turno", 0) or 0)
    except Exception:
        prod_turno = 0

    if prod_turno <= 0:
        try:
            esp_abs = int(m.get("esp_absoluto", 0) or 0)
        except Exception:
            esp_abs = 0
        try:
            base_d = int(m.get("baseline_diario", 0) or 0)
        except Exception:
            base_d = 0
        prod_turno = max(0, esp_abs - base_d)

    idx = calcular_ultima_hora_idx(m)
    m["ultima_hora"] = idx
    m["baseline_hora"] = int(prod_turno)
    m["producao_hora"] = 0
    m["percentual_hora"] = 0

    try:
        if isinstance(idx, int) and "producao_por_hora" in m and isinstance(m.get("producao_por_hora"), list):
            if 0 <= idx < len(m["producao_por_hora"]):
                m["producao_por_hora"][idx] = 0
    except Exception:
        pass

    m["_ph_loaded"] = False

    return jsonify({
        "ok": True,
        "machine_id": machine_id,
        "hora_idx": idx,
        "baseline_hora": int(m.get("baseline_hora", 0) or 0),
        "note": "Hora resetada. Produção da hora volta a contar a partir de agora.",
    })


# ============================================================
# RESET MANUAL
# ============================================================
@machine_bp.route("/admin/reset-manual", methods=["POST"])
def reset_manual():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))
    m = get_machine(machine_id)
    reset_contexto(m, machine_id)
    return jsonify({"status": "resetado", "machine_id": machine_id})


# ============================================================
# REFUGO: SALVAR (PERSISTENTE)
# ============================================================
@machine_bp.route("/machine/refugo", methods=["POST"])
def salvar_refugo():
    data = request.get_json() or {}
    machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    agora = now_bahia()
    dia_atual = dia_operacional_ref_str(agora)

    dia_ref = (data.get("dia_ref") or "").strip() or dia_atual
    hora_dia = _safe_int(data.get("hora_dia"), -1)
    refugo = _safe_int(data.get("refugo"), 0)

    if hora_dia < 0 or hora_dia > 23:
        return jsonify({"ok": False, "error": "hora_dia inválida (0..23)"}), 400

    if refugo < 0:
        refugo = 0

    if dia_ref > dia_atual:
        return jsonify({"ok": False, "error": "dia_ref futuro não permitido"}), 400

    if dia_ref == dia_atual:
        hora_atual = int(agora.hour)
        if hora_dia >= hora_atual:
            return jsonify({"ok": False, "error": "Só é permitido lançar refugo em horas passadas"}), 400

    ok = upsert_refugo(
        machine_id=machine_id,
        dia_ref=dia_ref,
        hora_dia=hora_dia,
        refugo=refugo,
        updated_at_iso=agora.isoformat(),
    )

    if not ok:
        return jsonify({"ok": False, "error": "Falha ao salvar no banco"}), 500

    return jsonify({
        "ok": True,
        "machine_id": machine_id,
        "dia_ref": dia_ref,
        "hora_dia": hora_dia,
        "refugo": refugo
    })


# ============================================================
# STATUS
# ============================================================
@machine_bp.route("/machine/status", methods=["GET"])
def machine_status():
    machine_id = _norm_machine_id(request.args.get("machine_id", "maquina01"))
    m = get_machine(machine_id)

    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)
    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

    dia_ref = dia_operacional_ref_str(now_bahia())
    m["refugo_por_hora"] = load_refugo_24(machine_id, dia_ref)

    if "run" not in m:
        m["run"] = 0

    try:
        np_prod = int(m.get("np_producao", 0) or 0)
    except Exception:
        np_prod = 0

    if m.get("ultima_hora") is None and np_prod > 0:
        m["producao_hora"] = np_prod
        m["percentual_hora"] = 0
        m["fora_turno"] = True
        m["producao_hora_liquida"] = np_prod
        return jsonify(m)

    try:
        hora_atual = int(now_bahia().hour)
    except Exception:
        hora_atual = None

    try:
        ph = int(m.get("producao_hora", 0) or 0)
    except Exception:
        ph = 0

    if isinstance(hora_atual, int) and 0 <= hora_atual < 24:
        m["producao_hora_liquida"] = max(0, ph - int(m["refugo_por_hora"][hora_atual] or 0))
    else:
        m["producao_hora_liquida"] = ph

    return jsonify(m)


# ============================================================
# HISTÓRICO - TELA (HTML)
# ============================================================
@machine_bp.route("/producao/historico", methods=["GET"])
def historico_page():
    return render_template("historico.html")


# ============================================================
# HISTÓRICO - API (JSON)
# ============================================================
@machine_bp.route("/api/producao/historico", methods=["GET"])
def historico_producao_api():
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
        params.append(_norm_machine_id(machine_id))

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

    out = []
    for r in rows:
        d = dict(r)

        mid = d.get("machine_id") or "maquina01"
        dia_ref = d.get("data") or ""

        refugo_total = _sum_refugo_24(mid, dia_ref)

        produzido = _safe_int(d.get("produzido"), 0)
        pecas_boas = max(0, produzido - refugo_total)

        d["refugo_total"] = refugo_total
        d["pecas_boas"] = pecas_boas

        out.append(d)

    return jsonify(out)
