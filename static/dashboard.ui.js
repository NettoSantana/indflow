/*
Caminho: C:\Users\vlula\OneDrive\Área de Trabalho\Projetos Backup\indflow\static\dashboard.ui.js
Último recode: 2026-02-24 22:00 (America/Bahia)
Motivo: Adicionar indicador de internet (WiFi) no canto inferior esquerdo do card, baseado em last_seen (45s) com estados ONLINE/OFFLINE/SEM DADOS, sem alterar backend.
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

/* ===========================
   WIFI (ONLINE / OFFLINE / SEM DADOS) - FRONT
   Regra: ONLINE se last_seen <= 45s, OFFLINE se >45s, SEM DADOS se inexistente.
   =========================== */

const WIFI_OFFLINE_THRESHOLD_SEC = 45;

function resolveLastSeenMs(data){
  // Aceita variações de nome sem quebrar
  const candidates = [
    data && data.last_seen_ms,
    data && data.last_seen_ts,
    data && data.last_seen,
    data && data.device_last_seen,
    data && data.device_last_seen_iso
  ];

  for(const c of candidates){
    if(c === null || c === undefined) continue;

    // Número: assume ms epoch
    if(typeof c === "number" && Number.isFinite(c)){
      // se vier em segundos (10 dígitos), converte para ms
      if(c > 0 && c < 1e12) return Math.floor(c * 1000);
      return Math.floor(c);
    }

    // String ISO / data
    if(typeof c === "string"){
      const t = Date.parse(c);
      if(Number.isFinite(t)) return t;

      // Se vier só como número em string
      const n = Number(c);
      if(Number.isFinite(n) && n > 0){
        if(n < 1e12) return Math.floor(n * 1000);
        return Math.floor(n);
      }
    }
  }

  return null;
}

function resolveWifiState(data){
  const lastMs = resolveLastSeenMs(data);
  if(lastMs === null) return "SEM_DADOS";

  const nowMs = Date.now();
  const diffSec = (nowMs - lastMs) / 1000;

  if(!Number.isFinite(diffSec) || diffSec < 0) return "SEM_DADOS";
  return diffSec <= WIFI_OFFLINE_THRESHOLD_SEC ? "ONLINE" : "OFFLINE";
}

function applyWifiToCard(machineId, data){
  const sid = safeSid(machineId);

  const svg = document.getElementById(`wifi-svg-${sid}`);
  const xsvg = document.getElementById(`wifi-xsvg-${sid}`);
  if(!svg || !xsvg) return;

  const st = resolveWifiState(data);

  if(st === "ONLINE"){
    svg.style.color = "#2563eb";    // azul
    xsvg.style.display = "none";
    return;
  }

  if(st === "OFFLINE"){
    svg.style.color = "#64748b";    // cinza escuro
    xsvg.style.display = "";
    return;
  }

  // SEM_DADOS
  svg.style.color = "#9ca3af";      // cinza claro
  xsvg.style.display = "none";
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

  // WiFi (online/offline)
  applyWifiToCard(machineId, data);
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
    <div class="machine-card" style="position:relative;" onclick="window.location.href='/producao/config/${machineId}'">
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


      <!-- WIFI (canto inferior esquerdo): ONLINE (azul), OFFLINE (X vermelho), SEM DADOS (cinza) -->
      <div id="wifi-wrap-${sid}" style="position:absolute; left:18px; bottom:16px; width:28px; height:22px; pointer-events:none;">
        <svg id="wifi-svg-${sid}" viewBox="0 0 64 48" style="width:28px; height:22px; color:#9ca3af;">
          <path d="M8 16 C24 2, 40 2, 56 16" fill="none" stroke="currentColor" stroke-width="6" stroke-linecap="round"/>
          <path d="M16 24 C28 14, 36 14, 48 24" fill="none" stroke="currentColor" stroke-width="6" stroke-linecap="round"/>
          <path d="M24 32 C30 27, 34 27, 40 32" fill="none" stroke="currentColor" stroke-width="6" stroke-linecap="round"/>
          <circle cx="32" cy="40" r="4.5" fill="currentColor"/>
        </svg>

        <svg id="wifi-xsvg-${sid}" viewBox="0 0 20 20" style="position:absolute; right:-2px; top:-2px; width:14px; height:14px; display:none;">
          <circle cx="10" cy="10" r="9" fill="#dc2626"/>
          <path d="M6 6 L14 14" stroke="#ffffff" stroke-width="2.4" stroke-linecap="round"/>
          <path d="M14 6 L6 14" stroke="#ffffff" stroke-width="2.4" stroke-linecap="round"/>
        </svg>
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
