// static/dashboard.state.js
// ===========================
// ESTADO + PAGINAÇÃO
// (CÓPIA DO dashboard.js — PASSO 1)
// ===========================

const LS_KEY = "indflow_machines_v1";

/* PAGINAÇÃO */
const PAGE_SIZE = 6;
let currentPage = 0;

/* ===========================
   ESTADO / MÁQUINAS
   =========================== */

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
   PAGINAÇÃO
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

/* ===========================
   EXCLUSÃO DE MÁQUINA
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
