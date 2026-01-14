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
    Regras:
      - Fora do turno: se tiver RUN=1 OU delta de esp_absoluto > 0 => conta como atividade.
      - Tempo: soma minutos entre updates enquanto atividade estiver ativa fora do turno.
      - Produção: soma delta do esp_absoluto enquanto fora do turno.
      - Dentro do turno: não acumula, e "fecha" a atividade fora do turno (sem zerar acumulados).
    Campos salvos em m:
      - np_producao: int (peças/pulsos fora do turno)
      - np_minutos: int (minutos ativos fora do turno)
      - _np_last_ts: iso str
      - _np_last_esp: int
      - _np_active: bool
    """
    if agora is None:
        agora = now_bahia()

    # inicializa acumuladores
    if "np_producao" not in m:
        m["np_producao"] = 0
    if "np_minutos" not in m:
        m["np_minutos"] = 0

    curr_esp = _safe_int(m.get("esp_absoluto", 0), 0)
    prev_esp = _safe_int(m.get("_np_last_esp", curr_esp), curr_esp)

    # tenta pegar sinal de run/parada da máquina (você disse que existe 1=rodando 0=parada)
    run_flag = _get_bool(m.get("run")) or _get_bool(m.get("rodando")) or _get_bool(m.get("sinal_run"))

    delta = _delta_non_negative(curr_esp, prev_esp)

    # dentro do turno: fecha janela não-programada e atualiza baseline/ts
    if dentro_turno:
        m["_np_active"] = False
        m["_np_last_ts"] = agora.isoformat()
        m["_np_last_esp"] = curr_esp
        return

    # fora do turno: atividade se run=1 ou houve produção (delta>0)
    active_now = bool(run_flag or (delta > 0))
    was_active = _get_bool(m.get("_np_active"))

    # se estava ativo, soma o tempo desde o último update
    last_ts_raw = m.get("_np_last_ts")
    if was_active and last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(str(last_ts_raw))
            dt_s = (agora - last_ts).total_seconds()
            if dt_s > 0:
                add_min = int(round(dt_s / 60))
                if add_min > 0:
                    m["np_minutos"] = _safe_int(m.get("np_minutos", 0), 0) + add_min
        except Exception:
            pass

    # produção fora do turno: soma delta sempre que fora do turno (se delta>0)
    if delta > 0:
        m["np_producao"] = _safe_int(m.get("np_producao", 0), 0) + int(delta)

    # atualiza estado
    m["_np_active"] = bool(active_now)
    m["_np_last_ts"] = agora.isoformat()
    m["_np_last_esp"] = curr_esp

