# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_routes.py
# Último recode: 2026-01-28 12:41 (America/Bahia)
# Motivo: Backfill retroativo de cliente_id em producao_diaria (historico legado) + fechamento diario continua via /machine/status sem remover funcionalidades.

# modules/machine_routes.py
import os
import hashlib
from flask import Blueprint, request, jsonify, render_template, session
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
    TZ_BAHIA,
)

from modules.machine_service import processar_nao_programado
from modules.repos.nao_programado_horaria_repo import load_np_por_hora_24

from modules.repos.machine_config_repo import upsert_machine_config
from modules.repos.refugo_repo import load_refugo_24, upsert_refugo

from modules.admin.routes import login_required

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


def _unscope_machine_id(v: str) -> str:
    """
    Compat: se vier "cliente_id::maquina01", devolve "maquina01".
    """
    s = (v or "").strip().lower()
    if "::" in s:
        return (s.split("::", 1)[1] or "").strip() or "maquina01"
    return s or "maquina01"


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _parse_hhmm(hhmm: str) -> tuple[int, int] | None:
    try:
        h_str, m_str = (hhmm or '').strip().split(':', 1)
        h = int(h_str); m = int(m_str)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return (h, m)
    except Exception:
        pass
    return None


def _calc_minutos_parados_somente_turno(start_ms: int, end_ms: int, turno_inicio: str | None, turno_fim: str | None) -> int:
    ini = _parse_hhmm(turno_inicio or '')
    fim = _parse_hhmm(turno_fim or '')
    if ini is None or fim is None or end_ms <= start_ms:
        return 0
    a0 = datetime.fromtimestamp(int(start_ms) / 1000, TZ_BAHIA)
    a1 = datetime.fromtimestamp(int(end_ms) / 1000, TZ_BAHIA)
    d = a0.date() - timedelta(days=1)
    d_end = a1.date() + timedelta(days=1)
    total = 0
    while d <= d_end:
        s = datetime(d.year, d.month, d.day, ini[0], ini[1], 0, tzinfo=TZ_BAHIA)
        e = datetime(d.year, d.month, d.day, fim[0], fim[1], 0, tzinfo=TZ_BAHIA)
        if e <= s:
            e = e + timedelta(days=1)
        x0 = a0 if a0 > s else s
        x1 = a1 if a1 < e else e
        if x1 > x0:
            total += int((x1 - x0).total_seconds())
        d = d + timedelta(days=1)
    return int(total // 60)


def _sum_refugo_24(machine_id: str, dia_ref: str) -> int:
    """
    Refugo ainda está por machine_id (legado). Para não misturar,
    usamos sempre o machine_id "limpo" (sem cliente_id::).
    """
    try:
        mid = _unscope_machine_id(machine_id)
        arr = load_refugo_24(_norm_machine_id(mid), (dia_ref or "").strip())
        if not isinstance(arr, list):
            return 0
        return sum(_safe_int(x, 0) for x in arr)
    except Exception:
        return 0


# ============================================================
# NÃO PROGRAMADO (HORA EXTRA)
#   - Persistência/decisão: modules.machine_service.processar_nao_programado()
#   - Leitura 24h: repos.nao_programado_horaria_repo.load_np_por_hora_24()
#   - IMPORTANTE: routes não deve ter SQL nem lógica de delta do NP
# ============================================================

def _looks_like_uuid(v: str) -> bool:
    """
    Validacao simples para evitar usar session['cliente_id'] errado (ex: id de usuario).
    Aceita UUID no formato 8-4-4-4-12 (36 chars, 4 hifens).
    """
    s = (v or "").strip()
    if len(s) != 36:
        return False
    if s.count("-") != 4:
        return False
    parts = s.split("-")
    if len(parts) != 5:
        return False
    sizes = [8, 4, 4, 4, 12]
    for i, p in enumerate(parts):
        if len(p) != sizes[i]:
            return False
        for ch in p:
            if ch not in "0123456789abcdefABCDEF":
                return False
    return True


def _resolve_cliente_id_for_status(m: dict) -> str | None:
    """
    Resolve tenant para leitura do NP no /machine/status.
    Ordem (OPCAO 1):
      1) X-API-Key (se existir)
      2) m['cliente_id'] (gravado no update do ESP)
      3) session['cliente_id'] (web) somente se parecer UUID valido
    """
    try:
        c = _get_cliente_from_api_key()
        if c and c.get("id"):
            return str(c["id"])
    except Exception:
        pass

    try:
        cid_m = (m.get("cliente_id") or "").strip()
        if cid_m:
            return cid_m
    except Exception:
        pass

    try:
        cid_sess = (session.get("cliente_id") or "").strip()
        if cid_sess and _looks_like_uuid(cid_sess):
            return cid_sess
    except Exception:
        pass

    return None


def _machine_id_scoped(cliente_id: str | None, machine_id: str) -> str:
    if cliente_id:
        return f"{cliente_id}::{machine_id}"
    return machine_id


def _load_np_por_hora_24_scoped(machine_id: str, dia_ref: str, cliente_id: str | None) -> list:
    """Carrega NP por hora (24) do banco para a máquina (scoped)."""
    try:
        mid = _machine_id_scoped(cliente_id, machine_id)
        conn = get_db()
        try:
            return load_np_por_hora_24(conn, mid, (dia_ref or "").strip())
        finally:
            conn.close()
    except Exception:
        return [0] * 24


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


def _get_cliente_id_for_request() -> str | None:
    """
    Resolve tenant do request:
      1) se tiver X-API-Key válida -> cliente_id
      2) senão, se tiver sessão web -> session['cliente_id']
      3) senão -> None
    """
    c = _get_cliente_from_api_key()
    if c:
        return c["id"]
    cid = (session.get("cliente_id") or "").strip()
    return cid or None



def _backfill_producao_diaria_cliente_id_all(machine_id: str, cliente_id: str) -> None:
    '''
    Backfill retroativo:
    - Preenche cliente_id em TODOS os registros de producao_diaria dessa maquina
      onde cliente_id esteja NULL ou vazio.
    - Faz match tanto no machine_id raw quanto no scoped (cliente_id::machine_id).

    Objetivo:
    - A API /api/producao/historico filtra por cliente_id. Registros antigos sem cliente_id ficam invisiveis.
    '''
    cid = (cliente_id or "").strip()
    if not cid:
        return

    raw_mid = _norm_machine_id(machine_id)
    scoped_mid = f"{cid}::{raw_mid}"

    conn = get_db()
    try:
        conn.execute(
            '''
            UPDATE producao_diaria
               SET cliente_id = ?
             WHERE (cliente_id IS NULL OR TRIM(cliente_id) = '')
               AND (machine_id = ? OR machine_id = ?)
            ''',
            (cid, raw_mid, scoped_mid),
        )
        conn.commit()
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

    try:
        conn.execute("ALTER TABLE devices ADD COLUMN cliente_id TEXT")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE devices ADD COLUMN created_at TEXT")
    except Exception:
        pass

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_devices_cliente_id ON devices(cliente_id)")
    except Exception:
        pass

    conn.commit()


def _upsert_device_for_cliente(device_id: str, cliente_id: str, now_str: str, allow_takeover: bool = False) -> bool:
    """
    Garante que o device (MAC) fique amarrado ao cliente.
    Regra:
      - se device já pertence a outro cliente -> bloqueia (False)
      - se não existe -> cria com cliente_id + created_at + last_seen
      - se existe sem cliente_id -> seta cliente_id
      - sempre atualiza last_seen

    Observação (DEV):
      - se allow_takeover=True, permite reassociar device preso a outro cliente.
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

        if owner and owner != cliente_id:
            # Se o device já estiver amarrado a outro cliente, por padrão bloqueia (403).
            # No DEV, você pode permitir "takeover" controlado via ENV:
            #   INDFLOW_ALLOW_DEVICE_TAKEOVER=1
            if allow_takeover:
                conn.execute("""
                    UPDATE devices
                       SET cliente_id = ?,
                           last_seen = ?
                     WHERE device_id = ?
                """, (cliente_id, now_str, device_id))
                conn.commit()
                return True
            return False

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
# PARADA OFICIAL (BACKEND) — grava stopped_since_ms por máquina
# ============================================================
def _ensure_machine_stop_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS machine_stop (
            machine_id TEXT PRIMARY KEY,
            stopped_since_ms INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_machine_stop_updated_at ON machine_stop(updated_at)")
    except Exception:
        pass
    conn.commit()


def _get_stopped_since_ms(machine_id: str) -> int | None:
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        cur = conn.execute(
            "SELECT stopped_since_ms FROM machine_stop WHERE machine_id = ? LIMIT 1",
            (machine_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            v = row["stopped_since_ms"]
        except Exception:
            v = row[0]
        try:
            iv = int(v)
            return iv if iv > 0 else None
        except Exception:
            return None
    finally:
        conn.close()


def _set_stopped_since_ms(machine_id: str, stopped_since_ms: int, updated_at: str):
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        conn.execute("""
            INSERT INTO machine_stop (machine_id, stopped_since_ms, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(machine_id) DO UPDATE SET
                stopped_since_ms=excluded.stopped_since_ms,
                updated_at=excluded.updated_at
        """, (machine_id, int(stopped_since_ms), updated_at))
        conn.commit()
    finally:
        conn.close()


def _clear_stopped_since(machine_id: str, updated_at: str):
    conn = get_db()
    try:
        _ensure_machine_stop_table(conn)
        conn.execute("DELETE FROM machine_stop WHERE machine_id = ?", (machine_id,))
        conn.commit()
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

    # ⚠️ MUITO IMPORTANTE:
    # NÃO criar mais o UNIQUE antigo (machine_id, dia_ref) porque pode haver duplicados
    # e isso derruba o deploy/serviço.
    try:
        conn.execute("DROP INDEX IF EXISTS ux_baseline_diario")
    except Exception:
        pass

    # Índice multi-tenant (principal)
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_cliente
            ON baseline_diario(cliente_id, machine_id, dia_ref)
        """)
    except Exception:
        pass

    # Índice legado parcial (somente registros antigos com cliente_id NULL)
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_diario_legacy
            ON baseline_diario(machine_id, dia_ref)
            WHERE cliente_id IS NULL
        """)
    except Exception:
        pass

    conn.commit()


def _has_baseline_for_day(conn, machine_id: str, dia_ref: str, cliente_id: str | None) -> bool:
    try:
        if cliente_id:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id=? LIMIT 1",
                (machine_id, dia_ref, cliente_id),
            )
        else:
            cur = conn.execute(
                "SELECT 1 FROM baseline_diario WHERE machine_id=? AND dia_ref=? AND cliente_id IS NULL LIMIT 1",
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
            INSERT OR IGNORE INTO baseline_diario (cliente_id, machine_id, dia_ref, baseline_esp, esp_last, updated_at)
            VALUES (NULL, ?, ?, ?, ?, ?)
        """, (machine_id, dia_ref, int(esp_abs), int(esp_abs), updated_at))
    conn.commit()


# ============================================================
# ADMIN - HARD RESET (LIMPA BANCO)
# ============================================================
@machine_bp.route("/admin/hard-reset", methods=["POST"])
def admin_hard_reset():
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
        "machine_stop",
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

    # guarda tenant no estado para leitura do NP no /machine/status (web)
    # ✅ RECODE: antes era cliente_id (variável inexistente). Agora resolvemos de forma segura.
    cliente_id = None
    try:
        cliente_id = _get_cliente_id_for_request()
    except Exception:
        cliente_id = None
    m["cliente_id"] = cliente_id

    meta_turno = int(data["meta_turno"])
    rampa = int(data["rampa"])

    m["meta_turno"] = meta_turno
    m["turno_inicio"] = data["inicio"]
    m["turno_fim"] = data["fim"]
    m["rampa_percentual"] = rampa

    # no_count_stop_sec: alerta de parada por inatividade (segundos sem producao)
    try:
        ncss = int(data.get("no_count_stop_sec", 0) or 0)
        if ncss >= 5:
            m["no_count_stop_sec"] = ncss
    except Exception:
        pass


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
# UPDATE ESP
# ============================================================
@machine_bp.route("/machine/update", methods=["POST"])
def update_machine():
    data = request.get_json() or {}

    cliente = _get_cliente_from_api_key()
    if not cliente:
        return jsonify({"error": "unauthorized"}), 401

    cliente_id = cliente["id"]
    allow_takeover = False

    device_id = norm_device_id(data.get("mac") or data.get("device_id") or "")
    if device_id:
        agora = now_bahia()
        now_str = agora.strftime("%Y-%m-%d %H:%M:%S")

        allow_takeover = (os.getenv("INDFLOW_ALLOW_DEVICE_TAKEOVER") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

        ok_owner = _upsert_device_for_cliente(
            device_id=device_id,
            cliente_id=cliente_id,
            now_str=now_str,
            allow_takeover=allow_takeover,
        )
        if not ok_owner:
            return jsonify({"error": "device pertence a outro cliente", "hint": "se for DEV, você pode liberar takeover setando INDFLOW_ALLOW_DEVICE_TAKEOVER=1"}), 403

        try:
            touch_device_seen(device_id)
        except Exception:
            pass

    linked_machine = _get_linked_machine_for_cliente(device_id, cliente_id) if device_id else None
    if linked_machine:
        machine_id = _norm_machine_id(linked_machine)
    else:
        machine_id = _norm_machine_id(data.get("machine_id", "maquina01"))

    m = get_machine(machine_id)

    # ✅ RECODE: garantir que o status consiga ler NP scoped (cliente_id::machine_id)
    m["cliente_id"] = cliente_id

    verificar_reset_diario(m, machine_id)

    # --- captura status anterior antes de sobrescrever
    prev_status = (m.get("status") or "").strip().upper()

    # status novo vindo do ESP
    new_status = (data.get("status", "DESCONHECIDO") or "DESCONHECIDO").strip().upper()

    m["status"] = new_status
    m["esp_absoluto"] = int(data.get("producao_turno", 0) or 0)

    # inatividade: marca o ultimo momento em que houve aumento de contagem
    # (permite considerar PARADA mesmo se o ESP continuar enviando status AUTO)
    try:
        agora_lc = now_bahia()
        now_ms_lc = int(agora_lc.timestamp() * 1000)
        esp_now = int(m.get("esp_absoluto", 0) or 0)
        esp_prev = m.get("_last_esp_abs_seen")
        if esp_prev is None:
            esp_prev = esp_now
        esp_prev = int(esp_prev)
        if esp_now != esp_prev:
            m["_last_count_ts_ms"] = now_ms_lc
        elif m.get("_last_count_ts_ms") is None:
            m["_last_count_ts_ms"] = now_ms_lc
        m["_last_esp_abs_seen"] = esp_now
    except Exception:
        pass

    m["run"] = _safe_int(data.get("run", 0), 0)

    # --- parada oficial (backend)
    try:
        agora = now_bahia()
        updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")
        now_ms = int(agora.timestamp() * 1000)

        if new_status == "AUTO":
            # voltou a produzir -> limpa parada
            _clear_stopped_since(machine_id, updated_at)
            m["stopped_since_ms"] = None
        else:
            # entrou/parado -> seta apenas se ainda não existir
            existing = _get_stopped_since_ms(machine_id)
            if existing is None:
                _set_stopped_since_ms(machine_id, now_ms, updated_at)
                m["stopped_since_ms"] = now_ms
            else:
                m["stopped_since_ms"] = existing
    except Exception:
        pass

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

    # ✅ RECODE: persistir NP hora a hora (sem travar o update)
    try:
        agora_np = now_bahia()
        # assinatura pode variar entre versões, por isso usamos kwargs e protegemos com try/except
        processar_nao_programado(
            m=m,
            machine_id=machine_id,
            cliente_id=cliente_id,
            esp_absoluto=int(m.get("esp_absoluto", 0) or 0),
            agora=agora_np,
        )
    except Exception:
        try:
            # fallback posicional (caso seu processar_nao_programado seja antigo)
            processar_nao_programado(m, machine_id, cliente_id)
        except Exception:
            pass

    return jsonify({
        "message": "OK",
        "machine_id": machine_id,
        "cliente_id": cliente_id,
        "device_id": device_id or None,
        "linked_machine": linked_machine or None,
        "baseline_initialized": bool(baseline_initialized),
        "allow_takeover": bool(allow_takeover),
    })


# ============================================================
# ADMIN - RESET SOMENTE DA HORA
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


    # =====================================================
    # FECHAMENTO DIARIO (OPCAO A) TAMBEM NO /machine/status
    # - Se o ESP nao enviar /machine/update na virada (23:59),
    #   o reset nao roda e o historico diario fica vazio.
    # - Garantimos que o registro diario fique com cliente_id,
    #   pois a API de historico filtra por cliente_id.
    # =====================================================
    cid_req = None
    try:
        cid_req = _get_cliente_id_for_request()
    except Exception:
        cid_req = None

    if cid_req:
        m["cliente_id"] = cid_req

        # Backfill retroativo (uma vez por processo): corrige historico legado sem cliente_id
        if not m.get("_pd_backfill_done"):
            try:
                _backfill_producao_diaria_cliente_id_all(machine_id, cid_req)
                m["_pd_backfill_done"] = True
            except Exception:
                pass

    dia_ref_before = str(m.get("ultimo_dia") or "").strip()
    try:
        verificar_reset_diario(m, machine_id)
    except Exception:
        pass
    dia_ref_after = str(m.get("ultimo_dia") or "").strip()

    # Se houve virada e reset, o reset_contexto gravou producao_diaria para dia_ref_before.
    # Em algumas versoes, esse insert nao popula a coluna cliente_id. Garantimos aqui.
    if cid_req and dia_ref_before and dia_ref_after and dia_ref_before != dia_ref_after:
        try:
            raw_mid = _norm_machine_id(machine_id)
            scoped_mid = f"{cid_req}::{raw_mid}"
            conn = get_db()
            try:
                conn.execute(
                    "UPDATE producao_diaria SET cliente_id=? "
                    "WHERE (cliente_id IS NULL OR cliente_id='') "
                    "AND data=? AND (machine_id=? OR machine_id=?)",
                    (cid_req, dia_ref_before, raw_mid, scoped_mid),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    carregar_baseline_diario(m, machine_id)

    atualizar_producao_hora(m)
    calcular_tempo_medio(m)
    aplicar_derivados_ml(m)

    dia_ref = dia_operacional_ref_str(now_bahia())
    m["refugo_por_hora"] = load_refugo_24(machine_id, dia_ref)

    # =====================================================
    # HORA EXTRA (NÃO PROGRAMADO): lista 24h persistida (DB)
    # =====================================================
    try:
        cid = _resolve_cliente_id_for_status(m)
        m["np_por_hora_24"] = _load_np_por_hora_24_scoped(machine_id, dia_ref, cid)
    except Exception:
        m["np_por_hora_24"] = [0] * 24


    # =====================================================
    # OPÇÃO 1 (BACKEND): produção exibível 24h (turno + NP)
    # - Dentro do turno: usa producao_por_hora (index do turno)
    # - Fora do turno: usa np_por_hora_24 (hora do dia)
    # Resultado em m["producao_exibicao_24"] (lista 24)
    # =====================================================
    try:
        exib = [0] * 24

        horas_turno = m.get("horas_turno") or []
        prod_turno = m.get("producao_por_hora") or []

        # mapeia cada slot do turno para hora inicial (ex: "12:00 - 13:00" -> 12)
        if isinstance(horas_turno, list) and isinstance(prod_turno, list):
            for i, faixa in enumerate(horas_turno):
                if i >= len(prod_turno):
                    break
                try:
                    h_ini = int(str(faixa).split("-", 1)[0].strip().split(":", 1)[0])
                except Exception:
                    continue
                if 0 <= h_ini <= 23:
                    v = prod_turno[i]
                    if v is None:
                        continue
                    exib[h_ini] = _safe_int(v, 0)

        # fora do turno: sobrescreve com NP quando houver valor
        np24 = m.get("np_por_hora_24") or [0] * 24
        if isinstance(np24, list) and len(np24) == 24:
            for h in range(24):
                if exib[h] == 0 and _safe_int(np24[h], 0) > 0:
                    exib[h] = _safe_int(np24[h], 0)

        m["producao_exibicao_24"] = exib
    except Exception:
        m["producao_exibicao_24"] = [0] * 24

    if "run" not in m:
        m["run"] = 0

    # --- parada oficial (backend): devolve parado_min e status_ui
    try:
        status = (m.get("status") or "").strip().upper()
        agora = now_bahia()
        now_ms = int(agora.timestamp() * 1000)

        # regra: se status do ESP for AUTO, mas ficar sem aumento de contagem por no_count_stop_sec, considerar PARADA
        thr = 0
        try:
            thr = int(m.get("no_count_stop_sec", 0) or 0)
        except Exception:
            thr = 0
        try:
            agora_i = now_bahia()
            now_ms_i = int(agora_i.timestamp() * 1000)
            last_ts = m.get("_last_count_ts_ms")
            if last_ts is None:
                last_ts = now_ms_i
                m["_last_count_ts_ms"] = last_ts
            last_ts = int(last_ts)
            sem_contar = (now_ms_i - last_ts) // 1000
        except Exception:
            sem_contar = 0
        
        if status == "AUTO" and thr >= 5 and sem_contar >= thr:
            m["status_ui"] = "PARADA"
            ss = _get_stopped_since_ms(machine_id)
            if ss is None:
                try:
                    updated_at = now_bahia().strftime("%Y-%m-%d %H:%M:%S")
                    _set_stopped_since_ms(machine_id, int(m.get("_last_count_ts_ms", now_ms)), updated_at)
                except Exception:
                    pass
                ss = _get_stopped_since_ms(machine_id)
            turno_inicio = (m.get("turno_inicio") or "").strip()
            turno_fim = (m.get("turno_fim") or "").strip()
            if turno_inicio and turno_fim:
                m["parado_min"] = _calc_minutos_parados_somente_turno(int(ss or now_ms), now_ms, turno_inicio, turno_fim)
            else:
                m["parado_min"] = None
        elif status == "AUTO":
            m["status_ui"] = "PRODUZINDO"
            m["parado_min"] = None
        else:
            m["status_ui"] = "PARADA"
            ss = _get_stopped_since_ms(machine_id)
            if ss is None:
                # fallback: se não existir (ex: primeiro status), ancora agora
                updated_at = agora.strftime("%Y-%m-%d %H:%M:%S")
                _set_stopped_since_ms(machine_id, now_ms, updated_at)
                ss = now_ms
            turno_inicio = (m.get("turno_inicio") or "").strip()
            turno_fim = (m.get("turno_fim") or "").strip()
            if turno_inicio and turno_fim:
                m["parado_min"] = _calc_minutos_parados_somente_turno(int(ss), now_ms, turno_inicio, turno_fim)
            else:
                diff_ms = max(0, now_ms - int(ss))
                m["parado_min"] = int(diff_ms // 60000)
    except Exception:
        m["status_ui"] = "PRODUZINDO" if (m.get("status") == "AUTO") else "PARADA"
        m["parado_min"] = None

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
@login_required
def historico_page():
    return render_template("historico.html")


# ============================================================
# HISTÓRICO - API (JSON)  ✅ AGORA É MULTI-TENANT
# ============================================================
@machine_bp.route("/api/producao/historico", methods=["GET"])
def historico_producao_api():
    cliente_id = _get_cliente_id_for_request()
    if not cliente_id:
        return jsonify({"error": "unauthorized"}), 401

    machine_id = request.args.get("machine_id")
    inicio = request.args.get("inicio")
    fim = request.args.get("fim")

    query = """
        SELECT machine_id, data, produzido, meta, percentual
        FROM producao_diaria
        WHERE cliente_id = ?
    """
    params = [cliente_id]

    if machine_id:
        raw_mid = _norm_machine_id(machine_id)
        scoped_mid = f"{cliente_id}::{raw_mid}"
        query += " AND (machine_id = ? OR machine_id = ?)"
        params.extend([raw_mid, scoped_mid])

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

        # para refugo, sempre usa máquina "limpa"
        refugo_total = _sum_refugo_24(mid, dia_ref)

        produzido = _safe_int(d.get("produzido"), 0)
        pecas_boas = max(0, produzido - refugo_total)

        d["machine_id"] = _unscope_machine_id(mid)
        d["refugo_total"] = refugo_total
        d["pecas_boas"] = pecas_boas

        out.append(d)

    return jsonify(out)
