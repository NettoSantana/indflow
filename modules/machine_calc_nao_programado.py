# modules/machine_calc_nao_programado.py
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_BAHIA = ZoneInfo("America/Bahia")


def now_bahia() -> datetime:
    return datetime.now(TZ_BAHIA)


def _get_bool(v) -> bool:
    # aceita 1/0, "1"/"0", True/False, "true"/"false"
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "on")


def _safe_int(v, default=0) -> int:
    try:
        return int(v or 0)
    except Exception:
        return default


def _delta_non_negative(curr: int, prev: int) -> int:
    d = curr - prev
    if d < 0:
        return 0
    return int(d)


def update_nao_programado(m: dict, dentro_turno: bool, agora: datetime | None = None) -> None:
    """
    Acumula produção e tempo "não programados" (fora do turno).

    ✅ Ajuste importante:
      - Heartbeat a cada ~5s NÃO pode usar round(dt/60), senão nunca vira 1 minuto.
      - Agora acumulamos segundos em m["_np_secs"] e só convertemos quando bater >= 60s.

    Regras:
      - Dentro do turno: fecha janela não-programada e atualiza marcadores.
      - Fora do turno:
          * Produção: soma delta do esp_absoluto (se delta>0)
          * Tempo: soma enquanto houver atividade no intervalo:
              - run==1 (preferencial) OU
              - delta>0 (houve produção) OU
              - já estava ativo (was_active)

    Campos em m:
      - np_producao: int
      - np_minutos: int
      - _np_secs: int (acumulador de segundos fora do turno)
      - _np_last_ts: iso str
      - _np_last_esp: int
      - _np_active: bool
    """
    if agora is None:
        agora = now_bahia()

    # init acumuladores
    if "np_producao" not in m:
        m["np_producao"] = 0
    if "np_minutos" not in m:
        m["np_minutos"] = 0
    if "_np_secs" not in m:
        m["_np_secs"] = 0

    curr_esp = _safe_int(m.get("esp_absoluto", 0), 0)
    prev_esp = _safe_int(m.get("_np_last_esp", curr_esp), curr_esp)
    delta = _delta_non_negative(curr_esp, prev_esp)

    # sinal de RUN vindo do ESP (agora existe no backend como m["run"])
    run_flag = (
        _get_bool(m.get("run"))
        or _get_bool(m.get("rodando"))
        or _get_bool(m.get("sinal_run"))
    )

    # dentro do turno: fecha janela e atualiza baseline/ts
    if dentro_turno:
        m["_np_active"] = False
        m["_np_last_ts"] = agora.isoformat()
        m["_np_last_esp"] = curr_esp
        m["_np_secs"] = 0  # zera acumulador de segundos ao voltar pro turno
        return

    was_active = _get_bool(m.get("_np_active"))

    # atividade no intervalo: RUN ou produção ou já estava ativo
    activity_this_interval = bool(run_flag or (delta > 0) or was_active)

    # soma tempo (em segundos) desde último update
    last_ts_raw = m.get("_np_last_ts")
    if activity_this_interval and last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_ts_raw))
            dt_s = int((agora - last_ts).total_seconds())
            if dt_s > 0:
                # acumula segundos
                m["_np_secs"] = _safe_int(m.get("_np_secs", 0), 0) + dt_s

                # converte para minutos quando completar 60s
                if m["_np_secs"] >= 60:
                    add_min = m["_np_secs"] // 60
                    m["_np_secs"] = m["_np_secs"] % 60
                    m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0) + int(add_min)
        except Exception:
            pass

    # soma produção fora do turno
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # janela ativa fora do turno (preferencialmente por RUN)
    m["_np_active"] = bool(run_flag or (delta > 0))

    # atualiza marcadores
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp
