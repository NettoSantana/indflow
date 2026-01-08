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
  // cria o pager via JS se não existir no HTML (sem precisar mexer no template)
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

  const info = document.createElement("span");
  info.id = "pagerInfo";
  info.textContent = "";

  const btnNext = document.createElement("button");
  btnNext.id = "btnNext";
  btnNext.type = "button";
  btnNext.textContent = "→";

  pager.appendChild(btnPrev);
  pager.appendChild(info);
  pager.appendChild(btnNext);

  // insere logo após o grid quando possível
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
  const info = document.getElementById("pagerInfo");
  const btnPrev = document.getElementById("btnPrev");
  const btnNext = document.getElementById("btnNext");

  if(!pager || !info || !btnPrev || !btnNext) return;

  const tp = totalPages();
  clampCurrentPage();

  if(tp <= 1){
    pager.style.display = "none";
    return;
  }

  pager.style.display = "flex";
  info.textContent = `Página ${currentPage + 1} / ${tp}`;

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

  // Se removeu tudo, volta pro default
  if(next.length === 0){
    next.push("maquina01");
  }

  setMachines(next);

  // AJUSTE DE PÁGINA (ADIÇÃO)
  clampCurrentPage();

  renderMachines();   // re-render do grid
  updateAll();        // força atualizar já
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
          <div class="percent-label">Meta do Turno</div>

          <div class="stats-sub"><span>Meta (UNI)</span><b id="meta-turno-uni-${sid}">0</b></div>
          <div class="stats-sub"><span>Meta (ML)</span><b id="meta-turno-ml-${sid}">0</b></div>
          <div class="stats-sub"><span>Produzido (UNI)</span><b id="prod-turno-uni-${sid}">0</b></div>
          <div class="stats-sub"><span>Produzido (ML)</span><b id="prod-turno-ml-${sid}">0</b></div>
        </div>

        <div class="divider"></div>

        <div class="percent-block">
          <div class="percent-value" id="percent-hora-${sid}">0%</div>
          <div class="percent-label">Meta da Hora</div>

          <div class="stats-sub"><span>Meta (UNI)</span><b id="meta-hora-uni-${sid}">0</b></div>
          <div class="stats-sub"><span>Meta (ML)</span><b id="meta-hora-ml-${sid}">0</b></div>
          <div class="stats-sub"><span>Produzido (UNI)</span><b id="prod-hora-uni-${sid}">0</b></div>
          <div class="stats-sub"><span>Produzido (ML)</span><b id="prod-hora-ml-${sid}">0</b></div>
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
  const machines = getMachines();

  // garante página válida sempre que renderizar
  clampCurrentPage();

  // renderiza apenas a página atual
  const pageItems = getMachinesPage();
  grid.innerHTML = pageItems.map(cardHTML).join("");

  // pager (setinhas)
  renderPager();
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

      const elPercentTurno = document.getElementById(`percent-turno-${sid}`);
      const elMetaTurnoUni = document.getElementById(`meta-turno-uni-${sid}`);
      const elMetaTurnoMl  = document.getElementById(`meta-turno-ml-${sid}`);
      const elProdTurnoUni = document.getElementById(`prod-turno-uni-${sid}`);
      const elProdTurnoMl  = document.getElementById(`prod-turno-ml-${sid}`);

      const elPercentHora = document.getElementById(`percent-hora-${sid}`);
      const elMetaHoraUni = document.getElementById(`meta-hora-uni-${sid}`);
      const elMetaHoraMl  = document.getElementById(`meta-hora-ml-${sid}`);
      const elProdHoraUni = document.getElementById(`prod-hora-uni-${sid}`);
      const elProdHoraMl  = document.getElementById(`prod-hora-ml-${sid}`);

      const elRitmo = document.getElementById(`ritmo-medio-${sid}`);

      if(elPercentTurno) elPercentTurno.textContent = (data.percentual_turno ?? 0) + "%";
      if(elMetaTurnoUni) elMetaTurnoUni.textContent = fmt(data.meta_turno);
      if(elMetaTurnoMl)  elMetaTurnoMl.textContent  = fmt(data.meta_turno_ml);
      if(elProdTurnoUni) elProdTurnoUni.textContent = fmt(data.producao_turno);
      if(elProdTurnoMl)  elProdTurnoMl.textContent  = fmt(data.producao_turno_ml);

      if(elPercentHora) elPercentHora.textContent = (data.percentual_hora ?? 0) + "%";
      if(elMetaHoraUni) elMetaHoraUni.textContent = fmt(data.meta_hora_pcs);
      if(elMetaHoraMl)  elMetaHoraMl.textContent  = fmt(data.meta_hora_ml);
      if(elProdHoraUni) elProdHoraUni.textContent = fmt(data.producao_hora);
      if(elProdHoraMl)  elProdHoraMl.textContent  = fmt(data.producao_hora_ml);

      const tm = Number(data.tempo_medio_min_por_peca);
      if(elRitmo){
        elRitmo.textContent =
          Number.isFinite(tm) && tm > 0
            ? "Ritmo médio: " + tm.toFixed(2).replace(".", ",") + " min/peça"
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

    // ADIÇÃO: vai pra página onde o novo item cairia (última)
    currentPage = Math.floor((current.length - 1) / PAGE_SIZE);
  }

  closeModal();
  window.location.href = `/producao/config/${id}`;
});

/* INIT */
renderMachines();
updateAll();
setInterval(updateAll, 1000);
