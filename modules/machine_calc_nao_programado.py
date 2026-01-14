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

    REGRA (segura):
      - Fora do turno:
          * Produção: soma delta do esp_absoluto (se delta>0)
          * Tempo: soma minutos do intervalo SEMPRE que houve atividade no intervalo:
              - delta>0 (houve produção) OU
              - janela estava ativa (was_active=True) OU
              - run_flag=True (se existir sinal real de RUN)
      - Dentro do turno:
          * fecha a janela e atualiza marcadores (sem zerar acumulados)

    Campos em m:
      - np_producao: int
      - np_minutos: int
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

    curr_esp = _safe_int(m.get("esp_absoluto", 0), 0)
    prev_esp = _safe_int(m.get("_np_last_esp", curr_esp), curr_esp)

    delta = _delta_non_negative(curr_esp, prev_esp)

    # tenta pegar sinal de run/parada SE existir (não depende de status Auto/Manual)
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
        return

    was_active = _get_bool(m.get("_np_active"))

    # atividade no intervalo (segura): delta>0 OU já estava ativo OU run_flag
    activity_this_interval = bool((delta > 0) or was_active or run_flag)

    # soma tempo desde último update se houve atividade no intervalo
    last_ts_raw = m.get("_np_last_ts")
    if activity_this_interval and last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_ts_raw))
            dt_s = (agora - last_ts).total_seconds()
            if dt_s > 0:
                add_min = int(round(dt_s / 60))
                if add_min > 0:
                    m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0) + add_min
        except Exception:
            pass

    # soma produção fora do turno
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # mantém a janela ativa se houve atividade agora (delta>0 ou run_flag)
    active_now = helps_active = bool((delta > 0) or run_flag)
    m["_np_active"] = bool(active_now)

    # atualiza marcadores
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp
