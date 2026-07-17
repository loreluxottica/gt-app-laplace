// Ground Truth App — frontend (server-rendered page images + boundary annotation)
// Operator-blind: the model's prediction is never sent to this page. Pages are
// JPEGs rendered server-side by PyMuPDF and lazy-loaded as they scroll into view.

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  dayId: null,                 // batch being annotated (check/{day_id}/)
  filename: null,
  folderId: null,
  totalPages: 0,
  starts: new Set([1]),        // ground-truth boundaries (doc start pages)
  dirty: false,
};

let pageObserver = null;       // IntersectionObserver for lazy image loading

// ── DOM ────────────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const el = {
  pending: $("list-pending"),
  completed: $("list-completed"),
  wlPending: $("wl-pending"),
  wlCompleted: $("wl-completed"),
  progressFill: $("progress-fill"),
  currentFile: $("current-file"),
  pageCount: $("page-count"),
  docCount: $("doc-count"),
  isMultidoc: $("is-multidoc"),
  btnSave: $("btn-save"),
  btnReset: $("btn-reset"),
  pages: $("pages"),
  emptyState: $("empty-state"),
  evalPanel: $("eval-panel"),
  evalTitle: $("eval-title"),
  evalBody: $("eval-body"),
  evalClose: $("eval-close"),
  btnNext: $("btn-next"),
  toast: $("toast"),
  daySelect: $("day-select"),
};

// ── API ────────────────────────────────────────────────────────────────────
const dayQ = () => `day_id=${encodeURIComponent(state.dayId || "")}`;
const api = {
  days: () => fetch("/api/days").then((r) => r.json()),
  worklist: () => fetch(`/api/worklist?${dayQ()}`).then((r) => r.json()),
  file: (name) => fetch(`/api/file/${encodeURIComponent(name)}?${dayQ()}`).then((r) => r.json()),
  pageUrl: (name, n) => `/api/page/${encodeURIComponent(name)}/${n}?${dayQ()}`,
  save: (payload) =>
    fetch("/api/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).then((r) => r.json()),
};

// ── Batch (day_id) selector ─────────────────────────────────────────────────
async function loadDays() {
  const res = await api.days();
  if (res.error) return toast(`Batches error: ${res.error}`, true);
  const days = res.days || [];
  el.daySelect.innerHTML = "";
  if (!days.length) {
    el.daySelect.innerHTML = "<option value=''>No batches in check/</option>";
    return;
  }
  days.forEach((d) => {
    const opt = document.createElement("option");
    opt.value = d.day_id;
    opt.textContent = `${d.day_id}  (${d.n_completed}/${d.n_total} done)`;
    el.daySelect.appendChild(opt);
  });
  // Default: first batch that still has pending files, else the newest.
  const firstPending = days.find((d) => d.n_completed < d.n_total);
  state.dayId = (firstPending || days[0]).day_id;
  el.daySelect.value = state.dayId;
  loadWorklist();
}

el.daySelect && (el.daySelect.onchange = () => {
  if (state.dirty && !confirm("Unsaved changes. Switch batch anyway?")) {
    el.daySelect.value = state.dayId;
    return;
  }
  state.dayId = el.daySelect.value;
  state.filename = null;
  el.currentFile.textContent = "Select a file to begin";
  el.pages.innerHTML = "";
  el.emptyState.classList.remove("hidden");
  loadWorklist();
});

// ── Worklist ────────────────────────────────────────────────────────────────
async function loadWorklist(selectFirst = false) {
  if (!state.dayId) return;
  const wl = await api.worklist();
  if (wl.error) return toast(`Worklist error: ${wl.error}`, true);

  el.wlPending.textContent = wl.n_pending;
  el.wlCompleted.textContent = wl.n_completed;
  const pct = wl.n_total ? Math.round((wl.n_completed / wl.n_total) * 100) : 0;
  el.progressFill.style.width = `${pct}%`;

  renderList(el.pending, wl.pending, false);
  renderList(el.completed, wl.completed, true);

  if (selectFirst && wl.pending.length) loadFile(wl.pending[0]);
}

function renderList(ul, names, done) {
  ul.innerHTML = "";
  names.forEach((name) => {
    const li = document.createElement("li");
    li.textContent = name;
    li.className = "wl-item" + (done ? " done" : "");
    if (name === state.filename) li.classList.add("active");
    li.onclick = () => loadFile(name);
    ul.appendChild(li);
  });
}

// ── Load a file ─────────────────────────────────────────────────────────────
async function loadFile(name) {
  if (state.dirty && !confirm("Unsaved changes. Switch file anyway?")) return;

  state.filename = name;
  state.starts = new Set([1]);
  state.dirty = false;
  el.currentFile.textContent = name;
  el.emptyState.classList.add("hidden");
  el.pages.innerHTML = "<div class='loading'>Loading…</div>";

  const detail = await api.file(name);
  if (detail.error) {
    el.pages.innerHTML = `<div class='loading'>Error: ${detail.error}</div>`;
    return;
  }

  state.totalPages = detail.total_pages;
  state.folderId = detail.folder_id ?? null;

  // Pre-fill from existing GT if re-annotating; else just page 1.
  if (detail.ground_truth?.predicted_starts) {
    state.starts = new Set(detail.ground_truth.predicted_starts);
    el.isMultidoc.checked = !!detail.ground_truth.is_multidoc;
  } else {
    el.isMultidoc.checked = false;
  }

  el.pageCount.textContent = `${state.totalPages} pages`;
  renderPageShells();
  syncControls();
  markActiveInList();
}

function markActiveInList() {
  document.querySelectorAll(".wl-item").forEach((li) => {
    li.classList.toggle("active", li.textContent === state.filename);
  });
}

// ── Page shells + lazy image loading ────────────────────────────────────────
function renderPageShells() {
  if (pageObserver) pageObserver.disconnect();
  el.pages.innerHTML = "";

  for (let n = 1; n <= state.totalPages; n++) {
    const card = document.createElement("div");
    card.className = "page-card";
    card.dataset.page = n;

    const label = document.createElement("div");
    label.className = "page-label";
    label.textContent = `Page ${n}`;

    const badge = document.createElement("div");
    badge.className = "page-badge";

    const skeleton = document.createElement("div");
    skeleton.className = "page-skeleton";
    skeleton.textContent = `Page ${n}`;

    card.appendChild(label);
    card.appendChild(badge);
    card.appendChild(skeleton);
    card.onclick = () => toggleStart(n);
    el.pages.appendChild(card);
  }

  // Lazy-load images only as cards approach the viewport.
  pageObserver = new IntersectionObserver(
    (entries, obs) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        loadPageImage(entry.target);
        obs.unobserve(entry.target);
      });
    },
    { root: $("viewer"), rootMargin: "800px 0px" }
  );
  document.querySelectorAll(".page-card").forEach((c) => pageObserver.observe(c));

  paintBoundaries();
}

function loadPageImage(card) {
  const n = parseInt(card.dataset.page, 10);
  const img = new Image();
  img.className = "page-img";
  img.alt = `Page ${n}`;
  img.onload = () => {
    const sk = card.querySelector(".page-skeleton");
    if (sk) sk.replaceWith(img);
  };
  img.onerror = () => {
    const sk = card.querySelector(".page-skeleton");
    if (sk) sk.textContent = `Page ${n} — failed to load`;
  };
  img.src = api.pageUrl(state.filename, n);
}

function toggleStart(n) {
  if (n === 1) return; // page 1 is always a document start
  if (state.starts.has(n)) state.starts.delete(n);
  else state.starts.add(n);
  state.dirty = true;
  el.isMultidoc.checked = state.starts.size > 1;
  paintBoundaries();
  syncControls();
}

function paintBoundaries() {
  document.querySelectorAll(".page-card").forEach((card) => {
    const n = parseInt(card.dataset.page, 10);
    const isStart = state.starts.has(n);
    card.classList.toggle("is-start", isStart);
    const badge = card.querySelector(".page-badge");
    badge.textContent = isStart ? (n === 1 ? "▶ DOC START (1)" : "▶ NEW DOCUMENT") : "";
  });
}

function syncControls() {
  const docs = state.starts.size;
  el.docCount.textContent = `${docs} document${docs > 1 ? "s" : ""}`;
  el.btnSave.disabled = !state.filename;
  el.btnReset.disabled = !state.filename;
}

// ── Save ────────────────────────────────────────────────────────────────────
async function save() {
  if (!state.filename) return;
  el.btnSave.disabled = true;
  el.btnSave.textContent = "Saving…";

  const payload = {
    filename: state.filename,
    day_id: state.dayId,
    folder_id: state.folderId,
    total_pages: state.totalPages,
    predicted_starts: [...state.starts].sort((a, b) => a - b),
    is_multidoc: el.isMultidoc.checked,
  };

  const res = await api.save(payload);
  el.btnSave.textContent = "Save & Evaluate";
  el.btnSave.disabled = false;

  if (res.error) return toast(`Save failed: ${res.error}`, true);

  state.dirty = false;
  showEval(res.evaluation);
  loadWorklist();
}

// ── Evaluation panel (shown only AFTER the operator commits) ─────────────────
function showEval(ev) {
  el.evalPanel.classList.remove("hidden");
  if (!ev || ev.model_prediction_missing) {
    el.evalTitle.textContent = "Saved — no model prediction yet";
    el.evalBody.innerHTML =
      "<p class='muted'>Ground truth stored. This file has no model split result yet, so no comparison was computed.</p>";
    return;
  }
  el.evalTitle.textContent = ev.exact_match ? "✓ Exact match vs model" : "Mismatch vs model";
  el.evalBody.innerHTML = `
    <div class="metric-grid">
      ${metric("Exact match", ev.exact_match ? "yes" : "no", ev.exact_match)}
      ${metric("Multidoc correct", ev.multidoc_correct ? "yes" : "no", ev.multidoc_correct)}
      ${metric("Precision", ev.precision)}
      ${metric("Recall", ev.recall)}
      ${metric("F1", ev.f1)}
      ${metric("TP / FP / FN", `${ev.n_true_positive} / ${ev.n_false_positive} / ${ev.n_false_negative}`)}
      ${metric("F1 (±1 tol)", ev.f1_tol)}
      ${metric("Off-by-one", ev.n_offby1)}
    </div>`;
}

function metric(label, value, good) {
  const cls = good === undefined ? "" : good ? "ok" : "bad";
  return `<div class="metric ${cls}"><span class="m-val">${value}</span><span class="m-lbl">${label}</span></div>`;
}

function resetStarts() {
  state.starts = new Set([1]);
  el.isMultidoc.checked = false;
  state.dirty = true;
  paintBoundaries();
  syncControls();
}

function toast(msg, isError = false) {
  el.toast.textContent = msg;
  el.toast.className = "toast" + (isError ? " error" : "");
  setTimeout(() => el.toast.classList.add("hidden"), 4000);
}

// ── Wiring ──────────────────────────────────────────────────────────────────
el.btnSave.onclick = save;
el.btnReset.onclick = resetStarts;
el.evalClose.onclick = () => el.evalPanel.classList.add("hidden");
el.btnNext.onclick = () => {
  el.evalPanel.classList.add("hidden");
  loadWorklist(true);
};
el.isMultidoc.onchange = () => {
  state.dirty = true;
};

window.addEventListener("beforeunload", (e) => {
  if (state.dirty) {
    e.preventDefault();
    e.returnValue = "";
  }
});

loadDays();
