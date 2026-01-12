# modules/machine_calc.py
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

UNIDADES_VALIDAS = {"pcs", "m", "m2"}

# ============================================================
# FUSO HORÁRIO OFICIAL DO SISTEMA
# ============================================================
TZ_BAHIA = ZoneInfo("America/Bahia")

# Dia operacional vira às 23:59 (não à meia-noite)
DIA_OPERACIONAL_VIRA = time(23, 59)


def now_bahia():
    return datetime.now(TZ_BAHIA)


def _dia_operacional_ref(agora: datetime) -> str:
    """
    Dia operacional:
      - de 23:59 até 23:58 do dia seguinte.
    Logo:
      - antes de 23:59 => ainda é "dia operacional" de ontem
      - a partir de 23:59 => vira para o dia de hoje
    """
    if agora.time() >= DIA_OPERACIONAL_VIRA:
        return agora.date().isoformat()
    return (agora.date() - timedelta(days=1)).isoformat()


# ============================================================
# ✅ compatibilidade para imports (machine_state)
# ============================================================
def dia_operacional_ref_dt(agora: datetime):
    """
    Retorna a date do dia operacional (vira às 23:59).
    """
    if agora.time() >= DIA_OPERACIONAL_VIRA:
        return agora.date()
    return agora.date() - timedelta(days=1)


def dia_operacional_ref_str(agora: datetime) -> str:
    """
    Retorna YYYY-MM-DD do dia operacional (vira às 23:59).
    """
    return _dia_operacional_ref(agora)


def dia_operacional_atual():
    """Ajuda para outros módulos decidirem a virada às 23:59."""
    return _dia_operacional_ref(now_bahia())


# ============================================================
# UNIDADES
# ============================================================
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


# ============================================================
# TURNO / HORA
# ============================================================
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

    # turno atravessou meia-noite
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
    """
    ✅ FIX DO BUG:
    - Se agora estiver fora da janela do turno => None
    - Se dentro => 0..len(horas)-1
    """
    horas = m.get("horas_turno") or []
    if not horas:
        return None

    agora = now_bahia()
    inicio_dt = get_turno_inicio_dt(m, agora)
    if not inicio_dt:
        return None

    fim_dt = inicio_dt + timedelta(hours=len(horas))

    if agora < inicio_dt:
        return None

    if agora >= fim_dt:
        return None

    diff_h = int((agora - inicio_dt).total_seconds() // 3600)

    if diff_h < 0:
        return None
    if diff_h >= len(horas):
        return None

    return diff_h


# ============================================================
# BASELINE DIÁRIO (REPO) - mantém interface antiga
# ============================================================
def _persistir_baseline_diario(machine_id: str, esp_abs: int):
    # import local para evitar circular
    from modules.repos.baseline_repo import persistir_baseline_diario as _repo_persistir
    _repo_persistir(machine_id, esp_abs)


def carregar_baseline_diario(m, machine_id):
    # import local para evitar circular
    from modules.repos.baseline_repo import carregar_baseline_diario as _repo_carregar
    _repo_carregar(m, machine_id)


# ============================================================
# PRODUÇÃO POR HORA (REPO)
# ============================================================
def _get_machine_id_from_m(m):
    nome = (m.get("nome") or "").strip()
    if not nome:
        return None
    return nome.lower()


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
    # import local para evitar circular
    from modules.repos.producao_horaria_repo import (
        ensure_producao_horaria_table,
        load_producao_por_hora,
        get_baseline_for_hora,
        upsert_hora,
    )

    idx = calcular_ultima_hora_idx(m)

    if idx is None:
        m["ultima_hora"] = None
        m["producao_hora"] = 0
        m["percentual_hora"] = 0
        return

    machine_id = _get_machine_id_from_m(m)
    agora = now_bahia()

    # ✅ CHAVE CERTA: dia operacional (vira 23:59)
    data_ref = _dia_operacional_ref(agora)

    horas = m.get("horas_turno") or []
    horas_len = len(horas)

    if m.get("_ph_data_ref") != data_ref or m.get("_ph_len") != horas_len:
        m["_ph_loaded"] = False
        m["_ph_data_ref"] = data_ref
        m["_ph_len"] = horas_len

    if (
        "producao_por_hora" not in m
        or not isinstance(m.get("producao_por_hora"), list)
        or len(m.get("producao_por_hora")) != horas_len
    ):
        m["producao_por_hora"] = [None] * horas_len
        m["_ph_loaded"] = False

    if machine_id and not m.get("_ph_loaded"):
        try:
            ensure_producao_horaria_table()
            m["producao_por_hora"] = load_producao_por_hora(machine_id, data_ref, horas_len)
            m["_ph_loaded"] = True
        except Exception:
            m["_ph_loaded"] = False

    esp_abs = int(m.get("esp_absoluto", 0) or 0)
    prev_idx = m.get("ultima_hora")

    if prev_idx is None or prev_idx != idx:
        # fecha a hora anterior (se existia)
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
                    ensure_producao_horaria_table()
                    upsert_hora(
                        machine_id=machine_id,
                        data_ref=data_ref,
                        hora_idx=prev_idx,
                        baseline_esp=base_prev,
                        esp_last=esp_abs,
                        produzido=prod_prev,
                        meta=meta_prev,
                        percentual=pct_prev,
                    )
                except Exception:
                    pass

        # abre a nova hora
        m["ultima_hora"] = idx

        baseline = None
        if machine_id:
            try:
                ensure_producao_horaria_table()
                baseline = get_baseline_for_hora(machine_id, data_ref, idx)
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
                ensure_producao_horaria_table()
                upsert_hora(
                    machine_id=machine_id,
                    data_ref=data_ref,
                    hora_idx=idx,
                    baseline_esp=int(baseline),
                    esp_last=esp_abs,
                    produzido=0,
                    meta=meta_now,
                    percentual=0,
                )
            except Exception:
                pass

        return

    # mesma hora: atualiza parcial
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
            ensure_producao_horaria_table()
            upsert_hora(
                machine_id=machine_id,
                data_ref=data_ref,
                hora_idx=idx,
                baseline_esp=base_h,
                esp_last=esp_abs,
                produzido=int(m["producao_hora"]),
                meta=meta_h,
                percentual=int(m["percentual_hora"]),
            )
        except Exception:
            pass


# ============================================================
# RESET / TEMPO MÉDIO / DERIVADOS
# ============================================================
def reset_contexto(m, machine_id):
    # (mantém como estava: escrita em producao_diaria continua onde já existe no projeto)
    # Essa função está aqui só porque o projeto já chamava ela por import.
    machine_id = (machine_id or "").strip().lower() or "maquina01"

    from modules.db_indflow import get_db  # import local (só aqui)

    # ✅ FIX: FECHAMENTO DIÁRIO IDEMPOTENTE
    # Garante 1 linha por máquina por dia mesmo com:
    # - deploy/restart
    # - múltiplos workers
    # - chamadas repetidas do reset
    dia_ref = str(m.get("ultimo_dia") or "").strip()

    conn = get_db()
    cur = conn.cursor()

    # remove qualquer registro anterior desse dia (se existir)
    try:
        cur.execute("""
            DELETE FROM producao_diaria
            WHERE machine_id = ? AND data = ?
        """, (machine_id, dia_ref))
    except Exception:
        pass

    cur.execute("""
        INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual)
        VALUES (?, ?, ?, ?, ?)
    """, (
        machine_id,
        dia_ref,
        int(m.get("producao_turno", 0) or 0),
        int(m.get("meta_turno", 0) or 0),
        int(m.get("percentual_turno", 0) or 0)
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
    m["_bd_dia_ref"] = None
    m["_bd_esp_last"] = None

    # ✅ persistir baseline do dia operacional no reset manual
    _persistir_baseline_diario(machine_id, int(m.get("esp_absoluto", 0) or 0))

    try:
        agora = now_bahia()
        m["_bd_dia_ref"] = _dia_operacional_ref(agora)
        m["_bd_esp_last"] = int(m.get("esp_absoluto", 0) or 0)
    except Exception:
        pass


def verificar_reset_diario(m, machine_id):
    agora = now_bahia()
    dia_ref = _dia_operacional_ref(agora)

    if m.get("ultimo_dia") != dia_ref:
        reset_contexto(m, machine_id)
        m["ultimo_dia"] = dia_ref


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
