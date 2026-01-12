// static/dashboard.ui.js

function fmt(n){
  const x = Number(n);
  if(!Number.isFinite(x)) return "0";
  return x.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
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
   PAGER (UI)
   =========================== */

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

/* ===========================
   UI ACTIONS
   =========================== */

function removeMachine(machineId){
  const id = String(machineId || "");
  const machines = getMachines();

  if(!machines.includes(id)) return;

  const isDefault = id === "maquina01";

  if(isDefault){
    const ok1 = window.confirm(
      "ATENÇÃO: você está tentando excluir a MAQUINA01 (padrão).\n\nDeseja continuar?"
    );
    if(!ok1) return;

    const ok2 = window.confirm(
      "Confirma MESMO a exclusão da MAQUINA01?\n\nIsso pode quebrar seus testes."
    );
    if(!ok2) return;
  }else{
    const ok = window.confirm(
      `Excluir o equipamento "${id.toUpperCase()}"?\n\nEssa ação remove do dashboard (localStorage).`
    );
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

/* ===========================
   CARD / RENDER
   =========================== */

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

function renderMachines(){
  const grid = document.getElementById("machineGrid");

  clampCurrentPage();

  const pageItems = getMachinesPage();
  grid.innerHTML = pageItems.map(cardHTML).join("");

  renderPager();
}

/* ===========================
   MODAL
   =========================== */

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

/* INIT UI */
renderMachines();
