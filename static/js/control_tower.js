// Control Tower — vanilla JS, fetch + polling (same style as the GT app).
// The batch journey: uploaded → parsing → predicted → awaiting_annotation →
// annotated → delivering → delivered. Every error is an event and visible here.

// ── State ───────────────────────────────────────────────────────────────────
const state = {
  dayId: null,
  tab: "batches",
  pollTimer: null,
  lastFunnel: null,   // previous counts, to bump/animate stages that changed
  deferred: [],       // current deferred list for the redeliver dialog
};

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// ── API ─────────────────────────────────────────────────────────────────────
const jget = (url) => fetch(url).then((r) => r.json());
const jpost = (url, body) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((r) => r.json());
const dayQ = () => `day_id=${encodeURIComponent(state.dayId || "")}`;

// ── Toast ───────────────────────────────────────────────────────────────────
let toastTimer = null;
function toast(msg, isError = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast" + (isError ? " error" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.add("hidden"), 5000);
}

// ── Tabs ────────────────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach((b) => {
  b.onclick = () => {
    state.tab = b.dataset.tab;
    document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === b));
    document.querySelectorAll(".panel").forEach((p) =>
      p.classList.toggle("active", p.id === `panel-${state.tab}`));
    refreshTab();
    schedulePoll();
  };
});

function refreshTab() {
  const fn = {
    batches: loadBatches,
    flow: loadFlow,
    gate: loadGate,
    errors: loadErrors,
    sftp: loadSftp,
    review: loadReview,
    files: loadFiles,
  }[state.tab];
  if (fn) fn();
}

// ── Day selector ────────────────────────────────────────────────────────────
async function loadDays(pickFirst = false) {
  const res = await jget("/api/days");
  if (res.error) return toast(`Batches: ${res.error}`, true);
  const days = res.days || [];
  const sel = $("day-select");
  const prev = state.dayId;
  sel.innerHTML = "";
  days.forEach((d) => {
    const o = document.createElement("option");
    o.value = d.day_id;
    o.textContent = `${d.day_id} — ${d.lifecycle}`;
    sel.appendChild(o);
  });
  if (days.length) {
    state.dayId = prev && days.some((d) => d.day_id === prev) ? prev : days[0].day_id;
    sel.value = state.dayId;
  }
  renderBatches(days);
  return days;
}
$("day-select").onchange = () => {
  state.dayId = $("day-select").value;
  state.lastFunnel = null;
  refreshTab();
};

// ── Batches panel ───────────────────────────────────────────────────────────
async function loadBatches() {
  await loadDays();
}

function renderBatches(days) {
  const wrap = $("batch-list");
  if (!days.length) {
    wrap.innerHTML = "<div class='loading'>No batches. Create <span class='mono'>inbox/{day_id}/</span> and upload PDFs.</div>";
    return;
  }
  wrap.innerHTML = "";
  days.forEach((d) => {
    const c = d.counts || {};
    const card = document.createElement("div");
    card.className = "batch-card";

    const lifecycleBadge = `<span class="badge ${esc(d.lifecycle)}">${esc(d.lifecycle.replace("_", " "))}</span>`;
    const errBadge = d.has_errors ? `<span class="badge err">errors</span>` : "";
    const gateNote = d.gate
      ? `<span>gate <b>${d.gate.n_annotated}/${d.gate.n_sampled}</b></span>` : "";

    card.innerHTML = `
      <span class="day mono">${esc(d.day_id)}</span>
      ${lifecycleBadge} ${errBadge}
      <span class="counts">
        <span>inbox <b>${d.n_inbox}</b></span>
        <span>tracked <b>${d.n_files}</b></span>
        <span>predicted <b>${c.n_predicted ?? 0}</b></span>
        <span>delivered <b>${c.n_delivered ?? 0}</b></span>
        <span>errors <b>${c.n_error ?? 0}</b></span>
        ${gateNote}
      </span>
      <span class="spacer"></span>
      <span class="batch-actions"></span>`;

    const actions = card.querySelector(".batch-actions");
    const btn = (label, cls, fn) => {
      const b = document.createElement("button");
      b.className = `btn tiny ${cls}`;
      b.textContent = label;
      b.onclick = fn;
      actions.appendChild(b);
    };

    if (["uploaded", "parsing", "unknown"].includes(d.lifecycle) || d.n_inbox > d.n_files)
      btn("▶ Run ingest", "primary", () => openIngest(d.day_id));
    if (d.lifecycle === "awaiting_annotation")
      btn("✎ Annotate", "primary", () => window.open(window.GT_APP_URL, "_blank"));
    if (["annotated", "predicted"].includes(d.lifecycle))
      btn("⇪ Split & upload", "primary", () => openDeliver(d.day_id));
    if (d.lifecycle === "delivering")
      btn("⇪ Resume delivery", "", () => openDeliver(d.day_id));
    btn("Live flow", "", () => {
      state.dayId = d.day_id;
      $("day-select").value = d.day_id;
      document.querySelector('[data-tab="flow"]').click();
    });

    wrap.appendChild(card);
  });
}

// ── Live flow panel ─────────────────────────────────────────────────────────
const STAGES = [
  { key: "inbox",      lbl: "Inbox",      sub: "uploaded" },
  { key: "parsed",     lbl: "Parsed",     sub: "ai_parse_document" },
  { key: "predicted",  lbl: "Predicted",  sub: "LLM boundaries" },
  { key: "gate",       lbl: "Ground truth", sub: "annotation gate" },
  { key: "split",      lbl: "Split",      sub: "physical PDFs" },
  { key: "delivered",  lbl: "Delivered",  sub: "SFTP" },
];

async function loadFlow() {
  if (!state.dayId) return;
  $("flow-day").textContent = state.dayId;
  const res = await jget(`/api/progress?${dayQ()}`);
  if (res.error) return toast(`Progress: ${res.error}`, true);

  const f = res.funnel || {};
  const v = res.volumes || {};
  const g = res.gate || {};
  const n = (x) => parseInt(x || 0, 10);

  const counts = {
    inbox: v.inbox ?? 0,
    parsed: n(f.n_parsed) + n(f.n_predicted) + n(f.n_sftp_pending) + n(f.n_delivered) + n(f.n_sftp_failed) + n(f.n_deferred),
    predicted: n(f.n_predicted) + n(f.n_sftp_pending) + n(f.n_delivered) + n(f.n_sftp_failed) + n(f.n_deferred),
    gate: g.n_annotated ?? 0,
    split: n(f.n_sftp_pending) + n(f.n_delivered) + n(f.n_sftp_failed) + n(f.n_deferred),
    delivered: n(f.n_delivered),
  };
  const errs = {
    inbox: 0,
    parsed: n(f.n_error),
    predicted: n(f.n_needs_review),
    gate: (g.n_sampled ?? 0) - (g.n_annotated ?? 0),
    split: n(f.n_deferred),
    delivered: n(f.n_sftp_failed),
  };
  const errLbl = {
    parsed: "errors", predicted: "needs review", gate: "to annotate",
    split: "deferred", delivered: "failed",
  };

  const running = (res.active_runs || []).length > 0;
  $("flow-runstate").textContent = running
    ? `⏳ job running: ${res.active_runs.map((r) => `${r.job} (${r.state})`).join(", ")}`
    : "idle — no active job";
  $("poll-dot").classList.toggle("live", running);

  const funnel = $("funnel");
  funnel.classList.toggle("running", running);
  funnel.innerHTML = STAGES.map((s, i) => {
    const bumped = state.lastFunnel && counts[s.key] !== state.lastFunnel[s.key];
    const flowing = running && i > 0;
    return `
      <div class="stage ${bumped ? "bumped" : ""} ${flowing ? "flowing" : ""}">
        ${i > 0 ? '<span class="flow-token"></span>' : ""}
        <div class="bubble">${counts[s.key]}</div>
        <div class="lbl">${s.lbl}</div>
        <div class="sub">${s.sub}</div>
        ${errs[s.key] > 0 ? `<div class="errs">⚠ ${errs[s.key]} ${errLbl[s.key] || ""}</div>` : ""}
      </div>`;
  }).join("");
  state.lastFunnel = counts;

  const feed = $("flow-events");
  feed.innerHTML = (res.events || []).map((e) => `
    <div class="event-row ${e.event_type === "error" ? "err" : ""}">
      <span class="ts">${esc((e.event_ts || "").slice(0, 19))}</span>
      <span>${esc(e.stage)}</span>
      <span class="etype">${esc(e.event_type)}</span>
      <span class="fname">${esc(e.filename || e.detail || e.error_message || "")}</span>
    </div>`).join("") || "<div class='loading'>No events yet.</div>";
}

// ── Gate panel ──────────────────────────────────────────────────────────────
async function loadGate() {
  if (!state.dayId) return;
  $("gate-day").textContent = state.dayId;
  const g = await jget(`/api/gate?${dayQ()}`);
  if (g.error) return toast(`Gate: ${g.error}`, true);

  const pct = g.n_sampled ? Math.round((g.n_annotated / g.n_sampled) * 100) : 0;
  const m = g.metrics || {};
  const num = (x, d = 2) => (x == null ? "—" : Number(x).toFixed(d));
  const pctf = (x) => (x == null ? "—" : `${Math.round(Number(x) * 100)}%`);

  $("gate-body").innerHTML = `
    <div class="gate-card">
      <div><b>${g.n_annotated}</b> / <b>${g.n_sampled}</b> sampled files annotated
        ${g.complete ? '<span class="badge annotated">gate complete</span>'
                     : '<span class="badge awaiting_annotation">waiting</span>'}</div>
      <div class="gate-bar"><div class="fill" style="width:${pct}%"></div></div>
      ${g.n_sampled === 0 ? "<p class='muted'>No sample yet — run ingest first.</p>" : ""}
      ${g.missing?.length ? `
        <p class="muted">Still to annotate (${g.missing.length}):</p>
        <div class="missing-list">${g.missing.map(esc).join("<br>")}</div>
        <p><a class="btn" href="${esc(window.GT_APP_URL)}" target="_blank">✎ Open Ground Truth app</a></p>` : ""}
      ${g.metrics && m.n_evaluated > 0 ? `
        <h3>Model vs ground truth (sample of ${esc(m.n_evaluated)})</h3>
        <div class="metric-grid">
          <div class="metric"><span class="m-val">${pctf(m.exact_match_rate)}</span><span class="m-lbl">exact match</span></div>
          <div class="metric"><span class="m-val">${pctf(m.multidoc_rate)}</span><span class="m-lbl">multidoc correct</span></div>
          <div class="metric"><span class="m-val">${num(m.avg_precision)}</span><span class="m-lbl">avg precision</span></div>
          <div class="metric"><span class="m-val">${num(m.avg_recall)}</span><span class="m-lbl">avg recall</span></div>
          <div class="metric"><span class="m-val">${num(m.avg_f1)}</span><span class="m-lbl">avg F1</span></div>
          <div class="metric"><span class="m-val">${num(m.avg_f1_tol)}</span><span class="m-lbl">avg F1 (±1)</span></div>
        </div>` : ""}
      ${g.complete ? `
        <p style="margin-top:16px">
          <button class="btn primary" onclick="openDeliver(state.dayId)">⇪ Proceed to split &amp; upload</button>
        </p>` : ""}
    </div>`;
}

// ── Errors panel ────────────────────────────────────────────────────────────
async function loadErrors() {
  const res = await jget(`/api/errors?${dayQ()}`);
  if (res.error) return toast(`Errors: ${res.error}`, true);
  const rows = res.stuck || [];
  if (!rows.length) {
    $("errors-body").innerHTML = "<div class='loading'>✓ Nothing stuck. All documented errors resolved.</div>";
    return;
  }
  $("errors-body").innerHTML = `
    <div class="tbl-wrap"><table>
      <thead><tr><th>day</th><th>file</th><th>status</th><th>reason</th><th>actions</th></tr></thead>
      <tbody>${rows.map((r) => `
        <tr>
          <td class="mono">${esc(r.day_id)}</td>
          <td class="fname">${esc(r.filename)}</td>
          <td>${esc(r.status)}${r.sftp_delivery_status ? " / " + esc(r.sftp_delivery_status) : ""}</td>
          <td class="reason">${esc(r.stuck_reason || r.error_message || "")}</td>
          <td>
            ${r.status === "error" && r.error_stage === "parsing"
              ? actBtn("retry-parse", r, "↻ parse") : ""}
            ${r.status === "error" && r.error_stage === "pdf_split"
              ? actBtn("retry-split", r, "↻ split") : ""}
            ${["failed", "deferred"].includes(r.sftp_delivery_status)
              ? actBtn("retry-sftp", r, "↻ sftp") : ""}
            ${r.needs_review === "true" || r.needs_review === true
              ? actBtn("approve-review", r, "✓ approve") : ""}
            ${actBtn("mark-manual", r, "✋ manual")}
          </td>
        </tr>`).join("")}
      </tbody></table></div>`;
}

function actBtn(type, row, label) {
  return `<button class="btn tiny" onclick="doAction('${type}','${esc(row.day_id)}','${esc(row.filename)}')">${label}</button> `;
}

async function doAction(type, dayId, filename) {
  if (!confirm(`${type} → ${filename}?`)) return;
  const res = await jpost(`/api/action/${type}`, { day_id: dayId, filename });
  if (res.error) return toast(res.error, true);
  toast(res.message || "done");
  refreshTab();
}
window.doAction = doAction;

// ── SFTP panel ──────────────────────────────────────────────────────────────
async function loadSftp() {
  const res = await jget(`/api/sftp?${dayQ()}`);
  if (res.error) return toast(`SFTP: ${res.error}`, true);
  const folders = res.folders || [];
  const deferred = res.deferred || [];
  state.deferred = deferred;

  const folderTbl = folders.length ? `
    <div class="tbl-wrap"><table>
      <thead><tr><th>day</th><th>folder</th><th>files</th><th>delivered</th>
        <th>pending</th><th>failed</th><th>deferred</th><th>last delivery</th></tr></thead>
      <tbody>${folders.map((r) => `
        <tr>
          <td class="mono">${esc(r.day_id)}</td>
          <td class="mono">${esc(r.folder_id)}</td>
          <td>${esc(r.n_files)}</td>
          <td>${esc(r.n_delivered)}</td>
          <td>${esc(r.n_pending)}</td>
          <td>${r.n_failed > 0 ? `<b style="color:var(--critical)">⚠ ${esc(r.n_failed)}</b>` : "0"}</td>
          <td>${r.n_deferred > 0 ? `<b style="color:var(--serious)">◔ ${esc(r.n_deferred)}</b>` : "0"}</td>
          <td class="mono">${esc((r.last_delivered_at || "").slice(0, 19))}</td>
        </tr>`).join("")}
      </tbody></table></div>` : "<div class='loading'>No delivery activity yet.</div>";

  const deferredBlock = deferred.length ? `
    <div class="panel-sub">
      <h3>Not uploaded — remote folder missing (${deferred.length})</h3>
      <p class="muted">These files were skipped because their folder didn't exist on the SFTP.
        Re-deliver them later to a different remote path.</p>
      <div class="tbl-wrap"><table>
        <thead><tr><th>day</th><th>file</th><th>folder</th><th>why</th></tr></thead>
        <tbody>${deferred.map((r) => `
          <tr><td class="mono">${esc(r.day_id)}</td><td class="fname">${esc(r.filename)}</td>
              <td class="mono">${esc(r.folder_id)}</td><td class="reason">${esc(r.sftp_delivery_error || "")}</td></tr>`).join("")}
        </tbody></table></div>
      <p style="margin-top:10px">
        <button class="btn primary" onclick="openRedeliver()">⇪ Re-deliver all to a new folder…</button>
      </p>
    </div>` : "";

  $("sftp-body").innerHTML = folderTbl + deferredBlock;
}

// ── Review panel ────────────────────────────────────────────────────────────
async function loadReview() {
  const res = await jget(`/api/review?${dayQ()}`);
  if (res.error) return toast(`Review: ${res.error}`, true);
  const rows = res.needs_review || [];
  if (!rows.length) {
    $("review-body").innerHTML = "<div class='loading'>✓ Nothing needs review.</div>";
    return;
  }
  $("review-body").innerHTML = `
    <div class="tbl-wrap"><table>
      <thead><tr><th>day</th><th>file</th><th>pages</th><th>source</th><th>prediction</th><th>actions</th></tr></thead>
      <tbody>${rows.map((r) => `
        <tr>
          <td class="mono">${esc(r.day_id)}</td>
          <td class="fname">${esc(r.filename)}</td>
          <td>${esc(r.total_pages)}</td>
          <td>${esc(r.boundary_source)}</td>
          <td class="mono">${esc(r.predicted_starts)}</td>
          <td>
            ${actBtn("approve-review", r, "✓ deliver unsplit")}
            ${actBtn("retry-split", r, "↻ re-split")}
            ${actBtn("mark-manual", r, "✋ manual")}
          </td>
        </tr>`).join("")}
      </tbody></table></div>`;
}

// ── Files panel ─────────────────────────────────────────────────────────────
async function loadFiles() {
  const q = $("file-q").value.trim();
  const status = $("file-status").value;
  const params = new URLSearchParams();
  if (state.dayId) params.set("day_id", state.dayId);
  if (q) params.set("q", q);
  if (status) params.set("status", status);
  const res = await jget(`/api/files?${params}`);
  if (res.error) return toast(`Files: ${res.error}`, true);
  const rows = res.files || [];
  $("files-body").innerHTML = rows.length ? `
    <div class="tbl-wrap"><table>
      <thead><tr><th>day</th><th>file</th><th>status</th><th>sftp</th><th>pages</th>
        <th>docs</th><th>source</th></tr></thead>
      <tbody>${rows.map((r) => `
        <tr class="clickable" onclick="showFile('${esc(r.day_id)}','${esc(r.filename)}')">
          <td class="mono">${esc(r.day_id)}</td>
          <td class="fname">${esc(r.filename)}</td>
          <td>${esc(r.status)}</td>
          <td>${esc(r.sftp_delivery_status || "")}</td>
          <td>${esc(r.n_pages ?? "")}</td>
          <td>${esc(r.n_documents ?? "")}</td>
          <td>${esc(r.boundary_source ?? "")}</td>
        </tr>`).join("")}
      </tbody></table></div>` : "<div class='loading'>No files match.</div>";
  $("file-detail").classList.add("hidden");
}
$("file-search").onclick = loadFiles;
$("file-q").onkeydown = (e) => { if (e.key === "Enter") loadFiles(); };

async function showFile(dayId, filename) {
  const res = await jget(`/api/file/${encodeURIComponent(filename)}?day_id=${encodeURIComponent(dayId)}`);
  if (res.error) return toast(res.error, true);
  const s = res.status || {};
  const wrap = $("file-detail");
  wrap.classList.remove("hidden");
  wrap.innerHTML = `
    <h3>${esc(filename)} <span class="muted">(${esc(dayId)})</span></h3>
    <p>status <b>${esc(s.status)}</b> · sftp <b>${esc(s.sftp_delivery_status || "—")}</b>
       · pages <b>${esc(s.n_pages ?? "—")}</b> · docs <b>${esc(s.n_documents ?? "—")}</b>
       · source <b>${esc(s.boundary_source ?? "—")}</b>
       ${s.needs_review === "true" ? ' · <span class="badge err">needs review</span>' : ""}</p>
    <h4>Event timeline</h4>
    <div class="event-feed">${(res.events || []).map((e) => `
      <div class="event-row ${e.event_type === "error" ? "err" : ""}">
        <span class="ts">${esc((e.event_ts || "").slice(0, 19))}</span>
        <span>${esc(e.stage)}</span>
        <span class="etype">${esc(e.event_type)}</span>
        <span class="fname">${esc(e.detail || e.error_message || (e.old_status ? `${e.old_status} → ${e.new_status}` : ""))}</span>
      </div>`).join("") || "<span class='muted'>no events</span>"}</div>
    ${(res.llm_responses || []).length ? `
      <h4>LLM responses</h4>
      ${res.llm_responses.map((l) => `
        <p class="muted">${esc(l.stage)} · ${esc(l.model_used)} ${l.is_fallback === "true" ? "(fallback)" : ""}
           ${l.error_message ? ` · <b style="color:var(--critical)">${esc(l.error_message)}</b>` : ""}</p>
        ${l.raw_response ? `<pre>${esc(l.raw_response)}</pre>` : ""}`).join("")}` : ""}`;
  wrap.scrollIntoView({ behavior: "smooth" });
}
window.showFile = showFile;

// ── Dialogs: ingest / deliver / redeliver ───────────────────────────────────
function openIngest(dayId) {
  $("dlg-ingest-day").textContent = dayId;
  $("dlg-ingest").showModal();
  $("dlg-ingest-go").onclick = async () => {
    const pct = parseFloat($("dlg-sample-pct").value || "10");
    const res = await jpost("/api/run-ingest", { day_id: dayId, sample_pct: pct });
    if (res.error) return toast(res.error, true);
    toast(`job_ingest launched (run ${res.run_id})`);
    state.dayId = dayId;
    document.querySelector('[data-tab="flow"]').click();
  };
}
window.openIngest = openIngest;

function openDeliver(dayId) {
  $("dlg-deliver-day").textContent = dayId;
  $("dlg-deliver").showModal();
  $("dlg-deliver-go").onclick = async () => {
    const remote = $("dlg-sftp-path").value.trim();
    const res = await jpost("/api/run-deliver", { day_id: dayId, sftp_remote_base: remote });
    if (res.error) {
      toast(res.error, true);
      if (res.gate) { state.dayId = dayId; document.querySelector('[data-tab="gate"]').click(); }
      return;
    }
    toast(`job_deliver launched (run ${res.run_id})`);
    state.dayId = dayId;
    document.querySelector('[data-tab="flow"]').click();
  };
}
window.openDeliver = openDeliver;

function openRedeliver() {
  $("dlg-redeliver-day").textContent = state.dayId;
  $("dlg-redeliver-list").innerHTML =
    state.deferred.map((d) => esc(d.filename)).join("<br>") || "(none)";
  $("dlg-redeliver").showModal();
  $("dlg-redeliver-go").onclick = async () => {
    const remote = $("dlg-redeliver-path").value.trim();
    const res = await jpost("/api/redeliver", { day_id: state.dayId, sftp_remote_base: remote });
    if (res.error) return toast(res.error, true);
    toast(`re-delivery launched (run ${res.run_id})`);
    document.querySelector('[data-tab="flow"]').click();
  };
}
window.openRedeliver = openRedeliver;

// ── Polling (live flow refreshes while visible; faster when a job runs) ─────
function schedulePoll() {
  clearTimeout(state.pollTimer);
  const delay = state.tab === "flow" ? 4000 : 20000;
  state.pollTimer = setTimeout(async () => {
    try {
      if (state.tab === "flow") await loadFlow();
      else if (state.tab === "batches") await loadDays();
    } catch (e) { /* transient poll errors are silent */ }
    schedulePoll();
  }, delay);
}

// ── Boot ────────────────────────────────────────────────────────────────────
(async () => {
  await loadDays(true);
  schedulePoll();
})();
