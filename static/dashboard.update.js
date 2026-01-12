// static/dashboard.update.js

/* ===========================
   UNIDADES / REGRAS
   =========================== */

function normUnidade(u){
  const v = (u || "").toString().trim().toLowerCase();
  return v ? v : null;
}

function labelUnidade(u){
  const v = normUnidade(u);
  if(!v) return "PCS";
  if(v === "pcs") return "PCS";
  if(v === "m") return "M";
  if(v === "m2") return "M²";
  return v.toUpperCase();
}

function pickValuesByUnit(u, data, scope){
  const unit = normUnidade(u) || "pcs";

  if(scope === "turno"){
    if(unit === "m"){
      return {
        meta: data.meta_turno_ml,
        prod: data.producao_turno_ml
      };
    }
    return {
      meta: data.meta_turno,
      prod: data.producao_turno
    };
  }

  if(unit === "m"){
    return {
      meta: data.meta_hora_ml,
      prod: data.producao_hora_ml
    };
  }

  return {
    meta: data.meta_hora_pcs,
    prod: data.producao_hora
  };
}

function formatTempoMedio(v){
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "—";
  const fixed = n >= 10 ? n.toFixed(1) : n.toFixed(2);
  return fixed.replace(".", ",");
}

/* ===========================
   INDICADOR VISUAL
   =========================== */

function calcularIndicador(percentual){
  const p = Number(percentual) || 0;

  if(p >= 102){
    return { icon: "▲", color: "#16a34a" }; // verde
  }

  if(p <= 98){
    return { icon: "▼", color: "#dc2626" }; // vermelho
  }

  return { icon: "—", color: "#2563eb" }; // azul
}

function renderPercentWithIndicator(el, percentual){
  if(!el) return;

  const p = Number(percentual) || 0;
  const ind = calcularIndicador(p);

  // ✅ símbolo ANTES do número
  // ✅ símbolo menor (não quebra layout)
  // ✅ cor no símbolo
  // ✅ número continua grande (herda o CSS do percent-value)
  el.innerHTML = `
    <span style="display:inline-flex; align-items:baseline; gap:10px; white-space:nowrap;">
      <span style="font-size:26px; font-weight:900; line-height:1; color:${ind.color};">${ind.icon}</span>
      <span>${p}%</span>
    </span>
  `;
}

/* ===========================
   UPDATE
   =========================== */

function updateMachine(machineId){
  const sid = safeSid(machineId);

  fetch(`/machine/status?machine_id=${machineId}`)
    .then(r => r.json())
    .then(data => {

      const statusBadge = document.getElementById(`status-badge-${sid}`);
      if(!statusBadge) return;

      statusBadge.textContent = data.status;
      statusBadge.className =
        "machine-status " + (data.status === "AUTO" ? "status-auto" : "status-manual");

      const u1 = normUnidade(data.unidade_1) || "pcs";
      const u2 = normUnidade(data.unidade_2);

      const u1Label = labelUnidade(u1);
      const u2Label = u2 ? labelUnidade(u2) : null;

      /* ===== PERCENTUAIS COM SINAL (ANTES DO NÚMERO) ===== */

      const pTurno = Number(data.percentual_turno ?? 0);
      const pHora  = Number(data.percentual_hora ?? 0);

      const elTurno = document.getElementById(`percent-turno-${sid}`);
      const elHora  = document.getElementById(`percent-hora-${sid}`);

      renderPercentWithIndicator(elTurno, pTurno);
      renderPercentWithIndicator(elHora, pHora);

      /* ===== TURNO ===== */

      const vTurnoU1 = pickValuesByUnit(u1, data, "turno");
      setText(`lbl-meta-turno-u1-${sid}`, `Meta (${u1Label})`);
      setText(`lbl-prod-turno-u1-${sid}`, `Produzido (${u1Label})`);
      setText(`meta-turno-u1-${sid}`, vTurnoU1.meta);
      setText(`prod-turno-u1-${sid}`, vTurnoU1.prod);

      const showU2 = !!u2Label;
      setVisible(`row-meta-turno-u2-${sid}`, showU2);
      setVisible(`row-prod-turno-u2-${sid}`, showU2);

      if(showU2){
        const vTurnoU2 = pickValuesByUnit(u2, data, "turno");
        setText(`lbl-meta-turno-u2-${sid}`, `Meta (${u2Label})`);
        setText(`lbl-prod-turno-u2-${sid}`, `Produzido (${u2Label})`);
        setText(`meta-turno-u2-${sid}`, vTurnoU2.meta);
        setText(`prod-turno-u2-${sid}`, vTurnoU2.prod);
      }

      /* ===== HORA ===== */

      const vHoraU1 = pickValuesByUnit(u1, data, "hora");
      setText(`lbl-meta-hora-u1-${sid}`, `Meta (${u1Label})`);
      setText(`lbl-prod-hora-u1-${sid}`, `Produzido (${u1Label})`);
      setText(`meta-hora-u1-${sid}`, vHoraU1.meta);
      setText(`prod-hora-u1-${sid}`, vHoraU1.prod);

      setVisible(`row-meta-hora-u2-${sid}`, showU2);
      setVisible(`row-prod-hora-u2-${sid}`, showU2);

      if(showU2){
        const vHoraU2 = pickValuesByUnit(u2, data, "hora");
        setText(`lbl-meta-hora-u2-${sid}`, `Meta (${u2Label})`);
        setText(`lbl-prod-hora-u2-${sid}`, `Produzido (${u2Label})`);
        setText(`meta-hora-u2-${sid}`, vHoraU2.meta);
        setText(`prod-hora-u2-${sid}`, vHoraU2.prod);
      }

      /* ===== RITMO ===== */

      const elRitmo = document.getElementById(`ritmo-medio-${sid}`);
      const tempoMedioTxt = formatTempoMedio(data.tempo_medio_min_por_peca);
      if(elRitmo){
        elRitmo.textContent =
          tempoMedioTxt !== "—"
            ? `Ritmo médio: ${tempoMedioTxt} min/peça`
            : "Ritmo médio: —";
      }
    })
    .catch(() => {});
}

function updateAll(){
  const pageItems = getMachinesPage();
  pageItems.forEach(updateMachine);
}

/* INIT */
updateAll();
setInterval(updateAll, 1000);
