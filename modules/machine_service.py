from datetime import datetime, time, timedelta
from modules.db_indflow import get_db

UNIDADES_VALIDAS = {"pcs", "m", "m2"}

def normalizar_unidade(v):
    if v is None:
        return None
    v = str(v).strip().lower()
    if v == "" or v == "none":
        return None
    return v if v in UNIDADES_VALIDAS else None

def aplicar_unidades(m, unidade_1, unidade_2):
    u1 = normalizar_unidade(unidade_1)
    u2 = normalizar_unidade(unidade_2)

    # não deixar duplicado
    if u1 and u2 and u1 == u2:
        u2 = None

    m["unidade_1"] = u1
    m["unidade_2"] = u2

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
    m["ultimo_dia"] = datetime.now().date()
    m["reset_executado_hoje"] = True

def verificar_reset_diario(m, machine_id):
    agora = datetime.now()
    horario_reset = time(23, 59)

    if agora.time() >= horario_reset and not m["reset_executado_hoje"]:
        reset_contexto(m, machine_id)

    if agora.date() != m["ultimo_dia"]:
        m["reset_executado_hoje"] = False

def calcular_horas_turno(inicio_str, fim_str):
    inicio = datetime.strptime(inicio_str, "%H:%M")
    fim = datetime.strptime(fim_str, "%H:%M")

    if fim <= inicio:
        fim += timedelta(days=1)

    horas = []
    atual = inicio
    while atual < fim:
        proxima = atual + timedelta(hours=1)
        horas.append(f"{atual.strftime('%H:%M')} - {proxima.strftime('%H:%M')}")
        atual = proxima

    return horas, inicio, fim

def calcular_metas_por_hora(meta_turno, horas, rampa_percentual):
    qtd_horas = len(horas)
    if qtd_horas <= 0:
        return []

    meta_base = meta_turno / qtd_horas
    meta_primeira = round(meta_base * (rampa_percentual / 100))
    restante = meta_turno - meta_primeira
    horas_restantes = qtd_horas - 1

    metas = [meta_primeira]

    if horas_restantes > 0:
        meta_restante_base = restante // horas_restantes
        sobra = restante % horas_restantes

        for i in range(horas_restantes):
            valor = meta_restante_base + (1 if i < sobra else 0)
            metas.append(valor)

    return metas

def calcular_tempo_medio_turno_min_por_peca(m):
    try:
        produzido = int(m.get("producao_turno", 0) or 0)
        inicio_str = m.get("turno_inicio")

        if produzido > 0 and inicio_str:
            agora = datetime.now()

            inicio_dt = datetime.strptime(inicio_str, "%H:%M")
            inicio_dt = inicio_dt.replace(year=agora.year, month=agora.month, day=agora.day)

            # turno atravessou meia-noite
            if agora < inicio_dt:
                inicio_dt -= timedelta(days=1)

            minutos = (agora - inicio_dt).total_seconds() / 60
            minutos = max(minutos, 1)

            return round(minutos / produzido, 2)

        return None
    except Exception:
        return None

def buscar_historico(machine_id=None, inicio=None, fim=None):
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

    return [dict(r) for r in rows]
