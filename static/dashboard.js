// static/dashboard.js
const LS_KEY = "indflow_machines_v1";

/* PAGINAÇÃO (ADIÇÃO) */
const PAGE_SIZE = 6;
let currentPage = 0;

function fmt(n){
  const x = Number(n);
  if(!Number.isFinite(x)) return "0";
  return x.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
}

function getMachines(){
  try{
    const raw = localStorage.getItem(LS_KEY);
    if(raw){
      const arr = JSON.parse(raw);
      if(Array.isArray(arr) && arr.length) return arr;
    }
  }catch(e){}
  return ["maquina01"];
}

function setMachines(arr){
  localStorage.setItem(LS_KEY, JSON.stringify(arr));
}

function normalizeId(s){
  let v = (s || "").trim().toLowerCase();
  v = v.replace(/\s+/g, "_");
  v = v.replace(/[^a-z0-9_\-]/g, "");
  return v;
}

function nextMachineId(machines){
  let maxN = 1;
  machines.forEach(id => {
    const m = String(id).match(/^maquina(\d+)$/);
    if(m){
      const n = parseInt(m[1], 10);
      if(Number.isFinite(n) && n > maxN) maxN = n;
    }
  });
  const next = maxN + 1;
  return "maquina" + String(next).padStart(2, "0");
}

function safeSid(machineId){
  return String(machineId).replace(/[^a-z0-9_\-]/g, "");
}

/* ===========================
   UNIDADES (DINÂMICO)
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
  // scope: "turno" | "hora"
  const unit = normUnidade(u) || "pcs";

  // Observação importante:
  // - backend hoje calcula derivados "ml" (metros lineares) via conv_m_por_pcs
  // - então, quando unidade for "m", usamos os campos *_ml
  // - para "m2" não existe derivado pronto -> usa base (mesma dos pcs)
  if(scope === "turno"){
    if(unit === "m"){
      return {
        meta: data.meta_turno_ml,
        prod: data.producao_turno_ml
      };
    }
    // pcs ou m2
    return {
      meta: data.meta_turno,
      prod: data.producao_turno
    };
  }

  // scope === "hora"
  if(unit === "m"){
    return {
      meta: data.meta_hora_ml,
      prod: data.producao_hora_ml
    };
  }
  // pcs ou m2
  return {
    meta: data.meta_hora_pcs,
    prod: data.producao_hora
  };
}

function setText(id, txt){
  const el = document.getElementById(id);
  if(el) el.textContent = txt;
}

function setVisible(id, isVisible){
  const el = document.getElementById(id);
  if(el) el.style.display = isVisible ? "" : "none";
}

/* ===========================
   PAGINAÇÃO (ADIÇÕES)
   =========================== */

function totalPages(){
  const total = getMachines().length;
  return Math.max(1, Math.ceil(total / PAGE_SIZE));
}

function clampCurrentPage(){
  const tp = totalPages();
  if(currentPage < 0) currentPage = 0;
  if(currentPage > tp - 1) currentPage = tp - 1;
}

function getMachinesPage(){
  const machines = getMachines();
  const start = currentPage * PAGE_SIZE;
  return machines.slice(start, start + PAGE_SIZE);
}

function ensurePager(){
  if(document.getElementById("pager")) return;

  const wrapper = document.querySelector(".dashboard-wrapper") || document.body;
  const grid = document.getElementById("machineGrid");

  const pager = document.createElement("div");
  pager.className = "pager";
  pager.id = "pager";
  pager.style.display = "none";

  const btnPrev = document.createElement("button");
  btnPrev.id = "btnPrev";
  btnPrev.type = "button";
  btnPrev.textContent = "←";
  btnPrev.title = "Anterior";

  const btnNext = document.createElement("button");
  btnNext.id = "btnNext";
  btnNext.type = "button";
  btnNext.textContent = "→";
  btnNext.title = "Próxima";

  pager.appendChild(btnPrev);
  pager.appendChild(btnNext);

  if(grid && grid.parentNode){
    grid.parentNode.insertBefore(pager, grid.nextSibling);
  }else{
    wrapper.appendChild(pager);
  }

  btnPrev.addEventListener("click", () => {
    if(currentPage > 0){
      currentPage--;
      renderMachines();
      updateAll();
    }
  });

  btnNext.addEventListener("click", () => {
    const tp = totalPages();
    if(currentPage < tp - 1){
      currentPage++;
      renderMachines();
      updateAll();
    }
  });
}

function renderPager(){
  ensurePager();

  const pager = document.getElementById("pager");
  const btnPrev = document.getElementById("btnPrev");
  const btnNext = document.getElementById("btnNext");

  if(!pager || !btnPrev || !btnNext) return;

  const tp = totalPages();
  clampCurrentPage();

  if(tp <= 1){
    pager.style.display = "none";
    return;
  }

  pager.style.display = "flex";

  btnPrev.disabled = currentPage === 0;
  btnNext.disabled = currentPage >= tp - 1;
}

/* EXCLUIR */
function removeMachine(machineId){
  const id = String(machineId || "");
  const machines = getMachines();

  if(!machines.includes(id)) return;

  const isDefault = id === "maquina01";

  if(isDefault){
    const ok1 = window.confirm("ATENÇÃO: você está tentando excluir a MAQUINA01 (padrão).\n\nDeseja continuar?");
    if(!ok1) return;

    const ok2 = window.confirm("Confirma MESMO a exclusão da MAQUINA01?\n\nIsso pode quebrar seus testes.");
    if(!ok2) return;
  }else{
    const ok = window.confirm(`Excluir o equipamento "${id.toUpperCase()}"?\n\nEssa ação remove do dashboard (localStorage).`);
    if(!ok) return;
  }

  const next = machines.filter(x => x !== id);

  if(next.length === 0){
    next.push("maquina01");
  }

  setMachines(next);

  clampCurrentPage();

  renderMachines();
  updateAll();
}

function cardHTML(machineId){
  const sid = safeSid(machineId);
  const upper = String(machineId).toUpperCase();

  return `
    <div class="machine-card" onclick="window.location.href='/producao/config/${machineId}'">
      <div class="machine-header">
        <div class="machine-name">${upper}</div>

        <div style="display:flex; align-items:center; gap:10px;">
          <div id="status-badge-${sid}" class="machine-status status-auto">AUTO</div>

          <button
            type="button"
            class="btn-delete-machine"
            title="Excluir equipamento"
            aria-label="Excluir equipamento ${upper}"
            onclick="event.stopPropagation(); removeMachine('${machineId}')"
          >
            ✕
          </button>
        </div>
      </div>

      <div class="percent-container">

        <div class="percent-block">
          <div class="percent-value" id="percent-turno-${sid}">0%</div>
          <div class="percent-label">Meta do Dia</div>

          <div class="stats-sub">
            <span id="lbl-meta-turno-u1-${sid}">Meta (—)</span>
            <b id="meta-turno-u1-${sid}">0</b>
          </div>
          <div class="stats-sub">
            <span id="lbl-prod-turno-u1-${sid}">Produzido (—)</span>
            <b id="prod-turno-u1-${sid}">0</b>
          </div>

          <div class="stats-sub" id="row-meta-turno-u2-${sid}">
            <span id="lbl-meta-turno-u2-${sid}">Meta (—)</span>
            <b id="meta-turno-u2-${sid}">0</b>
          </div>
          <div class="stats-sub" id="row-prod-turno-u2-${sid}">
            <span id="lbl-prod-turno-u2-${sid}">Produzido (—)</span>
            <b id="prod-turno-u2-${sid}">0</b>
          </div>
        </div>

        <div class="divider"></div>

        <div class="percent-block">
          <div class="percent-value" id="percent-hora-${sid}">0%</div>
          <div class="percent-label">Meta da Hora</div>

          <div class="stats-sub">
            <span id="lbl-meta-hora-u1-${sid}">Meta (—)</span>
            <b id="meta-hora-u1-${sid}">0</b>
          </div>
          <div class="stats-sub">
            <span id="lbl-prod-hora-u1-${sid}">Produzido (—)</span>
            <b id="prod-hora-u1-${sid}">0</b>
          </div>

          <div class="stats-sub" id="row-meta-hora-u2-${sid}">
            <span id="lbl-meta-hora-u2-${sid}">Meta (—)</span>
            <b id="meta-hora-u2-${sid}">0</b>
          </div>
          <div class="stats-sub" id="row-prod-hora-u2-${sid}">
            <span id="lbl-prod-hora-u2-${sid}">Produzido (—)</span>
            <b id="prod-hora-u2-${sid}">0</b>
          </div>
        </div>

      </div>

      <div class="ritmo-medio" id="ritmo-medio-${sid}">
        Ritmo médio: —
      </div>
    </div>
  `;
}

/* ALTERADO: agora renderiza só a PÁGINA atual (máx 6) e desenha o pager */
function renderMachines(){
  const grid = document.getElementById("machineGrid");

  clampCurrentPage();

  const pageItems = getMachinesPage();
  grid.innerHTML = pageItems.map(cardHTML).join("");

  renderPager();
}

function formatTempoMedio(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "—";
  const fixed = n >= 10 ? n.toFixed(1) : n.toFixed(2);
  return fixed.replace(".", ",");
}

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

      // Unidades vindas da configuração
      const u1 = normUnidade(data.unidade_1) || "pcs";   // obrigatória (fallback pcs)
      const u2 = normUnidade(data.unidade_2);           // opcional

      const u1Label = labelUnidade(u1);
      const u2Label = u2 ? labelUnidade(u2) : null;

      // Percentuais
      setText(`percent-turno-${sid}`, (data.percentual_turno ?? 0) + "%");
      setText(`percent-hora-${sid}`,  (data.percentual_hora ?? 0) + "%");

      // TURNO: unidade 1 (em cima)
      const vTurnoU1 = pickValuesByUnit(u1, data, "turno");
      setText(`lbl-meta-turno-u1-${sid}`, `Meta (${u1Label})`);
      setText(`lbl-prod-turno-u1-${sid}`, `Produzido (${u1Label})`);
      setText(`meta-turno-u1-${sid}`, fmt(vTurnoU1.meta));
      setText(`prod-turno-u1-${sid}`, fmt(vTurnoU1.prod));

      // TURNO: unidade 2 (embaixo / opcional)
      const showU2 = !!u2Label;
      setVisible(`row-meta-turno-u2-${sid}`, showU2);
      setVisible(`row-prod-turno-u2-${sid}`, showU2);
      if(showU2){
        const vTurnoU2 = pickValuesByUnit(u2, data, "turno");
        setText(`lbl-meta-turno-u2-${sid}`, `Meta (${u2Label})`);
        setText(`lbl-prod-turno-u2-${sid}`, `Produzido (${u2Label})`);
        setText(`meta-turno-u2-${sid}`, fmt(vTurnoU2.meta));
        setText(`prod-turno-u2-${sid}`, fmt(vTurnoU2.prod));
      }

      // HORA: unidade 1 (em cima)
      const vHoraU1 = pickValuesByUnit(u1, data, "hora");
      setText(`lbl-meta-hora-u1-${sid}`, `Meta (${u1Label})`);
      setText(`lbl-prod-hora-u1-${sid}`, `Produzido (${u1Label})`);
      setText(`meta-hora-u1-${sid}`, fmt(vHoraU1.meta));
      setText(`prod-hora-u1-${sid}`, fmt(vHoraU1.prod));

      // HORA: unidade 2 (embaixo / opcional)
      setVisible(`row-meta-hora-u2-${sid}`, showU2);
      setVisible(`row-prod-hora-u2-${sid}`, showU2);
      if(showU2){
        const vHoraU2 = pickValuesByUnit(u2, data, "hora");
        setText(`lbl-meta-hora-u2-${sid}`, `Meta (${u2Label})`);
        setText(`lbl-prod-hora-u2-${sid}`, `Produzido (${u2Label})`);
        setText(`meta-hora-u2-${sid}`, fmt(vHoraU2.meta));
        setText(`prod-hora-u2-${sid}`, fmt(vHoraU2.prod));
      }

      // Ritmo médio
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

/* ALTERADO: agora atualiza só os 6 visíveis (página atual) */
function updateAll(){
  const pageItems = getMachinesPage();
  pageItems.forEach(updateMachine);
}

/* MODAL */
const modalBackdrop = document.getElementById("modalBackdrop");
const machineIdInput = document.getElementById("machineIdInput");

function openModal(){
  machineIdInput.value = "";
  modalBackdrop.style.display = "flex";
  setTimeout(() => machineIdInput.focus(), 50);
}
function closeModal(){
  modalBackdrop.style.display = "none";
}

document.getElementById("btnAddMachine").addEventListener("click", openModal);
document.getElementById("btnCloseModal").addEventListener("click", closeModal);
document.getElementById("btnCancel").addEventListener("click", closeModal);

modalBackdrop.addEventListener("click", (e) => {
  if(e.target === modalBackdrop) closeModal();
});

document.getElementById("btnSave").addEventListener("click", () => {
  const current = getMachines();
  let id = normalizeId(machineIdInput.value);

  if(!id){
    id = nextMachineId(current);
  }

  if(!current.includes(id)){
    current.push(id);
    setMachines(current);

    currentPage = Math.floor((current.length - 1) / PAGE_SIZE);
  }

  closeModal();
  window.location.href = `/producao/config/${id}`;
});

/* INIT */
renderMachines();
updateAll();
setInterval(updateAll, 1000);
