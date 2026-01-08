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


def atualizar_producao_hora(m):
    idx = calcular_ultima_hora_idx(m)

    if idx is None:
        m["ultima_hora"] = None
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    if m.get("ultima_hora") is None or m.get("ultima_hora") != idx:
        m["ultima_hora"] = idx
        m["baseline_hora"] = int(m.get("esp_absoluto", 0) or 0)
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    esp_abs = int(m.get("esp_absoluto", 0) or 0)
    base_h = int(m.get("baseline_hora", esp_abs) or esp_abs)

    prod_h = esp_abs - base_h
    if prod_h < 0:
        prod_h = 0
    m["producao_hora"] = int(prod_h)

    meta_h = 0
    try:
        meta_h = (m.get("meta_por_hora") or [])[idx]
    except Exception:
        meta_h = 0

    if meta_h and meta_h > 0:
        m["percentual_hora"] = round((m["producao_hora"] / meta_h) * 100)
    else:
        m["percentual_hora"] = 0


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
