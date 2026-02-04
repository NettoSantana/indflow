# Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\modules\machine_calc.py
# Último recode: 2026-02-04 10:07 (America/Bahia)
# Motivo: Ajustar dia operacional para virar às 05:00 e garantir reset de produção diária sem saldo acumulado.

# modules/machine_calc.py
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

UNIDADES_VALIDAS = {"pcs", "m", "m2"}

# ============================================================
# FUSO HORÁRIO OFICIAL DO SISTEMA
# ============================================================
TZ_BAHIA = ZoneInfo("America/Bahia")

# Dia operacional vira às 05:00 (inicio do turno)
DIA_OPERACIONAL_VIRA = time(23,59)


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
# MULTI-TENANT: machine_id interno com namespace
# ============================================================
def _scoped_machine_id(m, machine_id: str) -> str:
    """
    Isola dados por cliente sem mudar o contrato externo.
    Internamente usamos: <cliente_id>::<machine_id>

    FIX: normaliza machine_id (strip/lower) para não gerar chaves diferentes
    por variação de caixa/espaco, e evita None quebrando.
    """
    mid = (machine_id or "").strip().lower()
    if not mid:
        return ""

    cid = (m.get("cliente_id") or "").strip()
    if cid:
        return f"{cid}::{mid}"
    return mid


# ============================================================
# compatibilidade para imports (machine_state)
# ============================================================
def dia_operacional_ref_dt(agora: datetime):
    """
    Retorna a date do dia operacional (vira às 05:00).
    """
    if agora.time() >= DIA_OPERACIONAL_VIRA:
        return agora.date()
    return agora.date() - timedelta(days=1)


def dia_operacional_ref_str(agora: datetime) -> str:
    """
    Retorna YYYY-MM-DD do dia operacional (vira às 05:00).
    """
    return _dia_operacional_ref(agora)


def dia_operacional_atual():
    """Ajuda para outros módulos decidirem a virada às 05:00."""
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
    FIX DO BUG:
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

    # multi-tenant: isola baseline por cliente
    scoped = _scoped_machine_id(m, (machine_id or "").strip().lower() or "maquina01")
    _repo_carregar(m, scoped)


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

    # ============================================================
    # NOVO: acumular "não programado" (fora do turno)
    # ============================================================
    agora = now_bahia()

    # ============================================================
    # FIX (OPÇÃO 1): garantir reset diário sempre que houver cálculo
    # Isso evita o bug 'o dia não zerou' quando a máquina fica parada
    # ou quando não há produção após a virada do dia operacional.
    # ============================================================
    try:
        raw_mid = _get_machine_id_from_m(m) or "maquina01"
        verificar_reset_diario(m, raw_mid)
    except Exception:
        # não quebra o cálculo se alguma máquina estiver mal configurada
        pass

    idx = calcular_ultima_hora_idx(m)
    dentro_turno = idx is not None

    try:
        from modules.machine_calc_nao_programado import update_nao_programado
        update_nao_programado(m, dentro_turno=dentro_turno, agora=agora)
    except Exception:
        # não quebra o fluxo principal se o módulo novo não estiver disponível por algum motivo
        pass

    # IDs e data_ref (usado tanto dentro quanto na saída do turno)
    raw_machine_id = _get_machine_id_from_m(m)
    machine_id = _scoped_machine_id(m, raw_machine_id) if raw_machine_id else None

    # CHAVE CERTA: dia operacional (vira 23:59)
    data_ref = _dia_operacional_ref(agora)

    esp_abs = int(m.get("esp_absoluto", 0) or 0)
    prev_idx = m.get("ultima_hora")

    # ============================================================
    # MODO ROBUSTO (Opção C)
    # - Mantém um acumulador dedicado por hora baseado em delta de esp_absoluto
    # - Valida contra o método de baseline (esp_abs - baseline_hora)
    # - Se detectar "explosão" (baseline incoerente), confia no acumulador e realinha baseline_hora
    # ============================================================
    if "_ph_acc_idx" not in m:
        m["_ph_acc_idx"] = None
    if "_ph_acc" not in m:
        m["_ph_acc"] = 0
    if "_ph_esp_last_seen" not in m:
        m["_ph_esp_last_seen"] = None

    # ============================================================
    # FIX: AO SAIR DO TURNO (idx=None), FECHAR E PERSISTIR A ÚLTIMA HORA
    # Isso evita "sumir" da tabela/DB quando a hora vira e o sistema retorna cedo.
    # ============================================================
    if idx is None:
        # fecha a última hora programada (se existia) e persiste, inclusive se for 0
        if machine_id and isinstance(prev_idx, int) and prev_idx >= 0:
            try:
                ensure_producao_horaria_table()
                # prioridade: acumulador dedicado (se estiver alinhado com a última hora)
                acc_idx = m.get("_ph_acc_idx")
                acc_val = int(m.get("_ph_acc", 0) or 0)
                if isinstance(acc_idx, int) and acc_idx == prev_idx and acc_val >= 0:
                    prod_prev = int(acc_val)
                    base_prev = int(esp_abs - prod_prev)
                else:
                    base_prev = int(m.get("baseline_hora", esp_abs) or esp_abs)
                    prod_prev = esp_abs - base_prev
                    if prod_prev < 0:
                        prod_prev = 0
                    prod_prev = int(prod_prev)

                meta_prev = _meta_by_idx(m, prev_idx)
                pct_prev = _percentual(prod_prev, meta_prev)

                # mantém array coerente (não deixa "-")
                try:
                    if isinstance(m.get("producao_por_hora"), list) and 0 <= prev_idx < len(m["producao_por_hora"]):
                        m["producao_por_hora"][prev_idx] = prod_prev
                except Exception:
                    pass

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

        # zera HORA programada em memória (fora do turno)
        m["ultima_hora"] = None
        m["producao_hora"] = 0
        m["percentual_hora"] = 0

        # reset de rastreadores do modo robusto
        m["_ph_acc_idx"] = None
        m["_ph_acc"] = 0
        m["_ph_esp_last_seen"] = None
        return

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

    # ============================================================
    # FIX OFFLINE/WIFI: se a hora "pulou" (ficou sem update),
    # não perder produção. Joga o delta acumulado na hora atual.
    # ============================================================
    if isinstance(prev_idx, int) and prev_idx >= 0 and prev_idx < idx:
        jump = idx - prev_idx
        if jump >= 2:
            # baseline do início da hora anterior (guardado em memória)
            base_prev = int(m.get("baseline_hora", esp_abs) or esp_abs)
            delta_total = esp_abs - base_prev
            if delta_total < 0:
                delta_total = 0
            delta_total = int(delta_total)

            # horas intermediárias: marca 0 no array e no banco (para não ficar "-")
            for h in range(prev_idx, idx):
                try:
                    if 0 <= h < len(m["producao_por_hora"]):
                        m["producao_por_hora"][h] = 0
                except Exception:
                    pass

                if machine_id:
                    try:
                        ensure_producao_horaria_table()
                        meta_h = _meta_by_idx(m, h)
                        upsert_hora(
                            machine_id=machine_id,
                            data_ref=data_ref,
                            hora_idx=h,
                            baseline_esp=base_prev,
                            esp_last=base_prev,
                            produzido=0,
                            meta=meta_h,
                            percentual=_percentual(0, meta_h),
                        )
                    except Exception:
                        pass

            # hora atual recebe todo o delta
            m["ultima_hora"] = idx
            # realinha baseline para garantir coerência (baseline = esp_abs - produzido)
            m["baseline_hora"] = int(esp_abs - int(delta_total))
            m["producao_hora"] = int(delta_total)

            # rastreadores do modo robusto
            m["_ph_acc_idx"] = idx
            m["_ph_acc"] = int(delta_total)
            m["_ph_esp_last_seen"] = esp_abs

            meta_now = _meta_by_idx(m, idx)
            m["percentual_hora"] = _percentual(delta_total, meta_now)

            try:
                if 0 <= idx < len(m["producao_por_hora"]):
                    m["producao_por_hora"][idx] = int(delta_total)
            except Exception:
                pass

            if machine_id:
                try:
                    ensure_producao_horaria_table()
                    upsert_hora(
                        machine_id=machine_id,
                        data_ref=data_ref,
                        hora_idx=idx,
                        baseline_esp=int(m["baseline_hora"]),
                        esp_last=esp_abs,
                        produzido=int(delta_total),
                        meta=meta_now,
                        percentual=int(m["percentual_hora"]),
                    )
                except Exception:
                    pass

            return

    # ============================================================
    # Fluxo normal (sem pulo grande)
    # ============================================================
    if prev_idx is None or prev_idx != idx:
        # fecha a hora anterior (se existia)
        if isinstance(prev_idx, int) and prev_idx >= 0:
            # prioridade: acumulador dedicado (se estiver alinhado com a hora anterior)
            acc_idx = m.get("_ph_acc_idx")
            acc_val = int(m.get("_ph_acc", 0) or 0)
            if isinstance(acc_idx, int) and acc_idx == prev_idx and acc_val >= 0:
                prod_prev = int(acc_val)
                base_prev = int(esp_abs - prod_prev)
            else:
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

        # abre a nova hora (e PERSISTE zero imediatamente)
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

        # rastreadores do modo robusto
        m["_ph_acc_idx"] = idx
        m["_ph_acc"] = 0
        m["_ph_esp_last_seen"] = esp_abs

        if machine_id:
            try:
                meta_now = _meta_by_idx(m, idx)
                ensure_producao_horaria_table()
                # IMPORTANTÍSSIMO: gravar 0 ao abrir a hora (sem depender de produzir)
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

    # mesma hora: atualiza parcial (e persiste mesmo se for 0)
    # ============================================================
    # MODO ROBUSTO (Opção C)
    # 1) Atualiza acumulador por delta (esp_abs - esp_last_seen)
    # 2) Compara com cálculo por baseline
    # 3) Se detectar explosão, confia no acumulador e realinha baseline_hora
    # ============================================================
    last_seen = m.get("_ph_esp_last_seen")
    if last_seen is None:
        last_seen = esp_abs

    try:
        last_seen = int(last_seen or 0)
    except Exception:
        last_seen = esp_abs

    delta = esp_abs - last_seen
    if delta < 0:
        # contador voltou (reset no ESP / troca de device / overflow)
        delta = 0

    try:
        acc = int(m.get("_ph_acc", 0) or 0)
    except Exception:
        acc = 0

    acc = max(acc + int(delta), 0)

    m["_ph_acc_idx"] = idx
    m["_ph_acc"] = acc
    m["_ph_esp_last_seen"] = esp_abs

    # cálculo por baseline (método antigo)
    base_h = int(m.get("baseline_hora", esp_abs) or esp_abs)
    prod_base = esp_abs - base_h
    if prod_base < 0:
        prod_base = 0
    prod_base = int(prod_base)

    prod_acc = int(acc)

    meta_h = _meta_by_idx(m, idx)

    # anti-explosão:
    # - se baseline estiver muito maior que o acumulador e exceder um limite alto, é bug de baseline
    limiar_abs = 100000
    try:
        limiar_meta = int(meta_h or 0) * 10
    except Exception:
        limiar_meta = 0

    limiar_explosao = max(limiar_abs, limiar_meta)

    diff = prod_base - prod_acc
    limiar_diff = max(int(meta_h or 0) * 2, 5000)

    if diff > limiar_diff and prod_base > limiar_explosao:
        # baseline incoerente -> confia no acumulador e realinha baseline
        prod_final = prod_acc
        m["baseline_hora"] = int(esp_abs - prod_final)
    else:
        # baseline ok -> usa o maior valor (acumulador pode atrasar em casos de backfill/primeira leitura)
        prod_final = max(prod_base, prod_acc)

    m["producao_hora"] = int(prod_final)

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
                baseline_esp=int(m.get("baseline_hora", base_h) or base_h),
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
    raw_machine_id = (machine_id or "").strip().lower() or "maquina01"
    scoped_machine_id = _scoped_machine_id(m, raw_machine_id)

    from modules.db_indflow import get_db  # import local (só aqui)

    # FIX: FECHAMENTO DIÁRIO IDEMPOTENTE
    dia_ref = str(m.get("ultimo_dia") or "").strip()
    if not dia_ref:
        # primeira execução sem ultimo_dia definido: não fecha dia anterior no DB
        dia_ref = None


    if dia_ref is not None:
        conn = get_db()
        cur = conn.cursor()
    
        # remove qualquer registro anterior desse dia (se existir)
        try:
            cur.execute("""
                DELETE FROM producao_diaria
                WHERE machine_id = ? AND data = ?
            """, (scoped_machine_id, dia_ref))
        except Exception:
            pass
    
        cur.execute("""
            INSERT INTO producao_diaria (machine_id, data, produzido, meta, percentual)
            VALUES (?, ?, ?, ?, ?)
        """, (
            scoped_machine_id,
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

    # NOVO: reset também das métricas não programadas (mantém simples e previsível)
    m["np_producao"] = 0
    m["np_minutos"] = 0
    m["_np_active"] = False
    m["_np_last_ts"] = None
    m["_np_last_esp"] = int(m.get("esp_absoluto", 0) or 0)

    # persistir baseline do dia operacional no reset manual (isolado por cliente)
    _persistir_baseline_diario(scoped_machine_id, int(m.get("esp_absoluto", 0) or 0))

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
    """
    Ritmo médio (min/peça) do card.
    Agora considera:
      - produção programada (producao_turno)
      - + produção não programada (np_producao)
      - minutos desde início do turno (quando houver turno_inicio)
      - + minutos não programados (np_minutos)
    """
    try:
        prod_turno = int(m.get("producao_turno", 0) or 0)
        prod_np = int(m.get("np_producao", 0) or 0)
        produzido_total = prod_turno + prod_np

        inicio_str = m.get("turno_inicio")
        minutos_np = int(m.get("np_minutos", 0) or 0)

        if produzido_total <= 0:
            m["tempo_medio_min_por_peca"] = None
            return

        agora = now_bahia()

        # minutos programados: só se tiver turno_inicio configurado
        minutos_prog = 0
        if inicio_str:
            try:
                inicio_dt = datetime.strptime(inicio_str, "%H:%M")
                inicio_dt = inicio_dt.replace(
                    year=agora.year,
                    month=agora.month,
                    day=agora.day,
                    tzinfo=TZ_BAHIA
                )
                if agora < inicio_dt:
                    inicio_dt -= timedelta(days=1)

                minutos_prog = int(max((agora - inicio_dt).total_seconds() / 60, 0))
            except Exception:
                minutos_prog = 0

        minutos_total = max(minutos_prog + minutos_np, 1)
        m["tempo_medio_min_por_peca"] = round(minutos_total / produzido_total, 2)
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
