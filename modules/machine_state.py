from datetime import datetime

machine_data = {}

def get_machine(machine_id: str):
    if machine_id not in machine_data:
        machine_data[machine_id] = {
            "nome": machine_id.upper(),
            "status": "DESCONHECIDO",

            "meta_turno": 0,
            "turno_inicio": None,
            "turno_fim": None,
            "rampa_percentual": 0,

            # NOVO: unidade (até 2)
            "unidade_1": None,  # ex: "pcs" | "m" | "m2"
            "unidade_2": None,  # ex: "pcs" | "m" | "m2"

            # NOVO: conversão (base atual = pcs)
            # 1 pcs = X metros (m)
            "conv_m_por_pcs": 1.0,

            "esp_absoluto": 0,
            "baseline_diario": 0,

            "producao_turno": 0,
            "producao_turno_anterior": 0,

            "horas_turno": [],
            "meta_por_hora": [],
            "producao_hora": 0,
            "percentual_hora": 0,
            "ultima_hora": None,

            "percentual_turno": 0,
            "tempo_medio_min_por_peca": None,

            "ultimo_dia": datetime.now().date(),
            "reset_executado_hoje": False
        }
    return machine_data[machine_id]
