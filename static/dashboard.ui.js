/*
Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\static\dashboard.ui.js
Último recode: 2026-01-16 21:25 (America/Bahia)
Motivo: No card de Produção, padronizar status para PRODUZINDO/PARADA e exibir "XX min parados" usando status_ui/parado_min do backend, sem alterar percentuais.
*/

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
   STATUS UI (PRODUZINDO / PARADA)
   =========================== */

function resolveStatusUI(data){
  const ui = String((data && data.status_ui) || "").trim().toUpperCase();
  if(ui === "PRODUZINDO" || ui === "PARADA") return ui;

  // fallback: backend antigo
  const raw = String((data && data.status) || "").trim().toUpperCase();
  if(raw === "AUTO") return "PRODUZINDO";
  if(raw) return "PARADA";
  return "PARADA";
}

function resolveParadoMin(data){
  const v = Number(data && data.parado_min);
  if(Number.isFinite(v) && v >= 0) return Math.floor(v);
  return null;
}

function applyStatusToCard(machineId, data){
  const sid = safeSid(machineId);

  const badgeId = `status-badge-${sid}`;
  const stopId = `stopline-${sid}`;

  const statusUI = resolveStatusUI(data);
  const produzindo = (statusUI === "PRODUZINDO");
  const mins = resolveParadoMin(data);

  // Badge: PRODUZINDO (verde) / PARADA (vermelho)
  const badge = document.getElementById(badgeId);
  if(badge){
    badge.textContent = statusUI;

    badge.classList.remove("status-auto");
    badge.classList.remove("status-manual");

    if(produzindo){
      badge.classList.add("status-auto");
    }else{
      badge.classList.add("status-manual");
    }
  }

  // Linha: "XX min parados" (só quando PARADA e mins disponível)
  const stopEl = document.getElementById(stopId);
  if(stopEl){
    if(!produzindo && mins !== null){
      stopEl.textContent = `${mins} min parados`;
      stopEl.style.display = "";
    }else{
      stopEl.textContent = "";
      stopEl.style.display = "none";
    }
  }
}

/* Poll simples só do status (não mexe em percentuais) */
function refreshStatuses(){
  try{
    const pageItems = getMachinesPage(); // só as máquinas visíveis (pager)
    pageItems.forEach((machineId) => {
      fetch(`/machine/status?machine_id=${machineId}`)
        .then(r => r.json())
        .then(data => applyStatusToCard(machineId, data))
        .catch(() => {
          // se falhar, não quebra a tela
        });
    });
  }catch(e){
    // silencioso
  }
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
          <!-- padrão inicial: será substituído por refreshStatuses() -->
          <div id="status-badge-${sid}" class="machine-status status-auto">PRODUZINDO</div>

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

      <!-- NOVO: linha abaixo do badge -->
      <div id="stopline-${sid}" class="machine-stopline" style="display:none"></div>

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

  // ✅ atualiza o status assim que renderiza
  refreshStatuses();
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

// ✅ Poll leve só do status (não mexe nos números do card)
setInterval(refreshStatuses, 2000);
