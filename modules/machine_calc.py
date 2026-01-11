# modules/machine_calc.py
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from modules.db_indflow import get_db

UNIDADES_VALIDAS = {"pcs", "m", "m2"}

# ============================================================
# FUSO HORÁRIO OFICIAL DO SISTEMA
# ============================================================
TZ_BAHIA = ZoneInfo("America/Bahia")


def now_bahia():
    return datetime.now(TZ_BAHIA)


def norm_u(v):
    if v is None:
        return None
    v = str(v).strip().lower()
    if v in ("", "none"):
        return None
    return v if v in UNIDADES_VALIDAS else None


def aplicar_unidades(m, u1, u2):
    u1 = norm_u(u1)
    u2 = norm_u(u2)
    if u1 and u2 and u1 == u2:
        u2 = None
    m["unidade_1"] = u1
    m["unidade_2"] = u2


def salvar_conversao(m, data):
    try:
        if "conv_m_por_pcs" in data and data.get("conv_m_por_pcs") not in (None, "", "none"):
            conv = float(data.get("conv_m_por_pcs"))
            if conv > 0:
                m["conv_m_por_pcs"] = conv
    except Exception:
        pass


def get_turno_inicio_dt(m, agora):
    inicio_str = m.get("turno_inicio")
    if not inicio_str:
        return None

    inicio_dt = datetime.strptime(inicio_str, "%H:%M")

    # garante data + fuso da Bahia
    inicio_dt = inicio_dt.replace(
        year=agora.year,
        month=agora.month,
        day=agora.day,
        tzinfo=TZ_BAHIA
    )

    if agora < inicio_dt:
        inicio_dt -= timedelta(days=1)

    return inicio_dt


def _turno_data_ref(m, agora):
    """
    Data de referência do turno (a data do início do turno).
    Isso evita bagunça quando o turno cruza meia-noite.
    """
    inicio_dt = get_turno_inicio_dt(m, agora)
    if inicio_dt:
        return inicio_dt.date().isoformat()
    return agora.date().isoformat()


def calcular_ultima_hora_idx(m):
    horas = m.get("horas_turno") or []
    if not horas:
        return None

    agora = now_bahia()
    inicio_dt = get_turno_inicio_dt(m, agora)
    if not inicio_dt:
        return None

    diff_h = int((agora - inicio_dt).total_seconds() // 3600)
    if diff_h < 0:
        diff_h = 0
    if diff_h >= len(horas):
        diff_h = len(horas) - 1

    return diff_h


# ============================================================
# BASELINE DO TURNO (PERSISTIDO NO SQLITE)  ✅
# ============================================================

def _ensure_baseline_turno(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS baseline_turno (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,           -- data do início do turno (YYYY-MM-DD)
            baseline_diario INTEGER NOT NULL, -- esp_absoluto no início do turno
            esp_last INTEGER NOT NULL,        -- último esp_absoluto visto
            updated_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_baseline_turno
        ON baseline_turno(machine_id, data_ref)
    """)
    conn.commit()


def carregar_baseline_turno(m, machine_id):
    """
    Garante m["baseline_diario"] persistido no SQLite por:
      - machine_id
      - data_ref (data do início do turno)

    Regra:
      - se não existir, cria baseline = esp_absoluto atual
      - se esp_absoluto voltar (reset do ESP), reancora baseline no novo valor
    """
    try:
        machine_id = (machine_id or "").strip().lower()
        if not machine_id:
            return
    except Exception:
        return

    agora = now_bahia()
    data_ref = _turno_data_ref(m, agora)

    try:
        esp_abs = int(m.get("esp_absoluto", 0) or 0)
    except Exception:
        esp_abs = 0

    # micro-cache pra reduzir I/O
    if m.get("_bt_data_ref") == data_ref and m.get("_bt_esp_last") == esp_abs and isinstance(m.get("baseline_diario"), int):
        return

    try:
        conn = get_db()
        _ensure_baseline_turno(conn)
        cur = conn.cursor()

        cur.execute("""
            SELECT baseline_diario
            FROM baseline_turno
            WHERE machine_id=? AND data_ref=?
            LIMIT 1
        """, (machine_id, data_ref))
        row = cur.fetchone()

        if row and row[0] is not None:
            try:
                baseline = int(row[0])
            except Exception:
                baseline = esp_abs
        else:
            baseline = esp_abs

        # se o contador resetou, reancora
        if esp_abs < baseline:
            baseline = esp_abs

        cur.execute("""
            INSERT INTO baseline_turno (machine_id, data_ref, baseline_diario, esp_last, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(machine_id, data_ref)
            DO UPDATE SET
                baseline_diario=excluded.baseline_diario,
                esp_last=excluded.esp_last,
                updated_at=excluded.updated_at
        """, (machine_id, data_ref, int(baseline), int(esp_abs), now_bahia().isoformat()))

        conn.commit()
        conn.close()

        m["baseline_diario"] = int(baseline)
        m["_bt_data_ref"] = data_ref
        m["_bt_esp_last"] = esp_abs
    except Exception:
        # fallback seguro (não derruba o app)
        if "baseline_diario" not in m or m.get("baseline_diario") is None:
            m["baseline_diario"] = esp_abs


# ============================================================
# PERSISTÊNCIA POR HORA (SQLITE)
# ============================================================

def _get_machine_id_from_m(m):
    """
    Não mexe em outros arquivos: tenta derivar machine_id do m.
    Hoje m["nome"] é tipo "MAQUINA01" -> vira "maquina01".
    """
    nome = (m.get("nome") or "").strip()
    if not nome:
        return None
    return nome.lower()


def _ensure_producao_horaria(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producao_horaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT NOT NULL,
            data_ref TEXT NOT NULL,         -- data do início do turno (YYYY-MM-DD)
            hora_idx INTEGER NOT NULL,      -- índice da hora dentro do turno (0..n-1)
            baseline_esp INTEGER NOT NULL,  -- esp_absoluto no início da hora
            esp_last INTEGER NOT NULL,      -- último esp_absoluto visto
            produzido INTEGER NOT NULL,
            meta INTEGER NOT NULL,
            percentual INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_producao_horaria
        ON producao_horaria(machine_id, data_ref, hora_idx)
    """)
    conn.commit()


def _upsert_hora(conn, machine_id, data_ref, hora_idx, baseline_esp, esp_last, produzido, meta, percentual):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO producao_horaria
        (machine_id, data_ref, hora_idx, baseline_esp, esp_last, produzido, meta, percentual, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(machine_id, data_ref, hora_idx)
        DO UPDATE SET
            baseline_esp=excluded.baseline_esp,
            esp_last=excluded.esp_last,
            produzido=excluded.produzido,
            meta=excluded.meta,
            percentual=excluded.percentual,
            updated_at=excluded.updated_at
    """, (
        machine_id,
        data_ref,
        int(hora_idx),
        int(baseline_esp),
        int(esp_last),
        int(produzido),
        int(meta),
        int(percentual),
        now_bahia().isoformat()
    ))
    conn.commit()


def _get_baseline_for_hora(conn, machine_id, data_ref, hora_idx):
    cur = conn.cursor()
    cur.execute("""
        SELECT baseline_esp
        FROM producao_horaria
        WHERE machine_id=? AND data_ref=? AND hora_idx=?
        LIMIT 1
    """, (machine_id, data_ref, int(hora_idx)))
    row = cur.fetchone()
    if row and row[0] is not None:
        try:
            return int(row[0])
        except Exception:
            return None
    return None


def _load_producao_por_hora(conn, machine_id, data_ref, n_horas):
    """
    Retorna lista tamanho n_horas com valores (int) ou None.
    """
    out = [None] * int(n_horas or 0)
    cur = conn.cursor()
    cur.execute("""
        SELECT hora_idx, produzido
        FROM producao_horaria
        WHERE machine_id=? AND data_ref=?
    """, (machine_id, data_ref))
    rows = cur.fetchall() or []
    for r in rows:
        try:
            idx = int(r[0])
            val = int(r[1])
            if 0 <= idx < len(out):
                out[idx] = val
        except Exception:
            continue
    return out


def _meta_by_idx(m, idx):
    meta_h = 0
    try:
        meta_h = (m.get("meta_por_hora") or [])[idx]
    except Exception:
        meta_h = 0
    try:
        meta_h = int(meta_h or 0)
    except Exception:
        meta_h = 0
    return meta_h


def _percentual(prod, meta):
    if meta and meta > 0:
        try:
            return int(round((prod / meta) * 100))
        except Exception:
            return 0
    return 0


def atualizar_producao_hora(m):
    idx = calcular_ultima_hora_idx(m)

    if idx is None:
        m["ultima_hora"] = None
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    machine_id = _get_machine_id_from_m(m)
    agora = now_bahia()
    data_ref = _turno_data_ref(m, agora)

    horas = m.get("horas_turno") or []
    horas_len = len(horas)

    if m.get("_ph_data_ref") != data_ref or m.get("_ph_len") != horas_len:
        m["_ph_loaded"] = False
        m["_ph_data_ref"] = data_ref
        m["_ph_len"] = horas_len

    if "producao_por_hora" not in m or not isinstance(m.get("producao_por_hora"), list) or len(m.get("producao_por_hora")) != horas_len:
        m["producao_por_hora"] = [None] * horas_len
        m["_ph_loaded"] = False

    if machine_id and not m.get("_ph_loaded"):
        try:
            conn = get_db()
            _ensure_producao_horaria(conn)
            m["producao_por_hora"] = _load_producao_por_hora(conn, machine_id, data_ref, horas_len)
            conn.close()
            m["_ph_loaded"] = True
        except Exception:
            m["_ph_loaded"] = False

    esp_abs = int(m.get("esp_absoluto", 0) or 0)
    prev_idx = m.get("ultima_hora")

    if prev_idx is None or prev_idx != idx:
        if isinstance(prev_idx, int) and prev_idx >= 0:
            base_prev = int(m.get("baseline_hora", esp_abs) or esp_abs)
            prod_prev = esp_abs - base_prev
            if prod_prev < 0:
                prod_prev = 0
            prod_prev = int(prod_prev)

            meta_prev = _meta_by_idx(m, prev_idx)
            pct_prev = _percentual(prod_prev, meta_prev)

            try:
                if 0 <= prev_idx < len(m["producao_por_hora"]):
                    m["producao_por_hora"][prev_idx] = prod_prev
            except Exception:
                pass

            if machine_id:
                try:
                    conn = get_db()
                    _ensure_producao_horaria(conn)
                    _upsert_hora(
                        conn,
                        machine_id=machine_id,
                        data_ref=data_ref,
                        hora_idx=prev_idx,
                        baseline_esp=base_prev,
                        esp_last=esp_abs,
                        produzido=prod_prev,
                        meta=meta_prev,
                        percentual=pct_prev
                    )
                    conn.close()
                except Exception:
                    pass

        m["ultima_hora"] = idx

        baseline = None
        if machine_id:
            try:
                conn = get_db()
                _ensure_producao_horaria(conn)
                baseline = _get_baseline_for_hora(conn, machine_id, data_ref, idx)
                conn.close()
            except Exception:
                baseline = None

        if baseline is None:
            baseline = esp_abs

        m["baseline_hora"] = int(baseline)
        m["producao_hora"] = 0
        m["percentual_hora"] = 0

        if machine_id:
            try:
                meta_now = _meta_by_idx(m, idx)
                conn = get_db()
                _ensure_producao_horaria(conn)
                _upsert_hora(
                    conn,
                    machine_id=machine_id,
                    data_ref=data_ref,
                    hora_idx=idx,
                    baseline_esp=int(baseline),
                    esp_last=esp_abs,
                    produzido=0,
                    meta=meta_now,
                    percentual=0
                )
                conn.close()
            except Exception:
                pass

        return

    base_h = int(m.get("baseline_hora", esp_abs) or esp_abs)
    prod_h = esp_abs - base_h
    if prod_h < 0:
        prod_h = 0
    m["producao_hora"] = int(prod_h)

    meta_h = _meta_by_idx(m, idx)
    m["percentual_hora"] = _percentual(m["producao_hora"], meta_h)

    try:
        if 0 <= idx < len(m["producao_por_hora"]):
            m["producao_por_hora"][idx] = int(m["producao_hora"])
    except Exception:
        pass

    if machine_id:
        try:
            conn = get_db()
            _ensure_producao_horaria(conn)
            _upsert_hora(
                conn,
                machine_id=machine_id,
                data_ref=data_ref,
                hora_idx=idx,
                baseline_esp=base_h,
                esp_last=esp_abs,
                produzido=int(m["producao_hora"]),
                meta=meta_h,
                percentual=int(m["percentual_hora"])
            )
            conn.close()
        except Exception:
            pass


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
    m["tempo_medio_min_por_peca"] = None
    m["ultima_hora"] = None
    m["baseline_hora"] = m["esp_absoluto"]

    m["_ph_loaded"] = False
    m["_bt_data_ref"] = None
    m["_bt_esp_last"] = None

    m["ultimo_dia"] = now_bahia().date()
    m["reset_executado_hoje"] = True


def verificar_reset_diario(m, machine_id):
    agora = now_bahia()
    horario_reset = time(23, 59)

    if agora.time() >= horario_reset and not m["reset_executado_hoje"]:
        reset_contexto(m, machine_id)

    if agora.date() != m["ultimo_dia"]:
        m["reset_executado_hoje"] = False


def calcular_tempo_medio(m):
    try:
        produzido = int(m.get("producao_turno", 0) or 0)
        inicio_str = m.get("turno_inicio")

        if produzido > 0 and inicio_str:
            agora = now_bahia()
            inicio_dt = datetime.strptime(inicio_str, "%H:%M")

            inicio_dt = inicio_dt.replace(
                year=agora.year,
                month=agora.month,
                day=agora.day,
                tzinfo=TZ_BAHIA
            )

            if agora < inicio_dt:
                inicio_dt -= timedelta(days=1)

            minutos = (agora - inicio_dt).total_seconds() / 60
            minutos = max(minutos, 1)
            m["tempo_medio_min_por_peca"] = round(minutos / produzido, 2)
        else:
            m["tempo_medio_min_por_peca"] = None
    except Exception:
        m["tempo_medio_min_por_peca"] = None


def aplicar_derivados_ml(m):
    try:
        conv = float(m.get("conv_m_por_pcs", 1.0) or 1.0)
        if conv <= 0:
            conv = 1.0
    except Exception:
        conv = 1.0

    m["conv_m_por_pcs"] = conv

    m["meta_turno_ml"] = round((m.get("meta_turno", 0) or 0) * conv, 2)
    m["producao_turno_ml"] = round((m.get("producao_turno", 0) or 0) * conv, 2)

    meta_hora_pcs = 0
    try:
        idx = m.get("ultima_hora")
        if isinstance(idx, int) and idx >= 0:
            meta_hora_pcs = (m.get("meta_por_hora") or [])[idx]
    except Exception:
        meta_hora_pcs = 0

    m["meta_hora_pcs"] = int(meta_hora_pcs or 0)
    m["meta_hora_ml"] = round(m["meta_hora_pcs"] * conv, 2)
    m["producao_hora_ml"] = round((m.get("producao_hora", 0) or 0) * conv, 2)
