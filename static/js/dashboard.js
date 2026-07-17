// Dashboard — fetch aggregate stats, render monochrome cards, export PNG/HTML.

const pct = (x) => `${Math.round((x || 0) * 100)}%`;
const num = (x) => (x == null ? "—" : x);

function statCard(val, label, sub, wide = false) {
  return `<div class="stat${wide ? " wide" : ""}">
    <span class="s-val">${val}</span>
    <span class="s-lbl">${label}</span>
    ${sub ? `<span class="s-sub">${sub}</span>` : ""}
  </div>`;
}

function bar(label, value) {
  return `<div class="stat wide">
    <span class="s-lbl">${label}</span>
    <span class="s-val">${pct(value)}</span>
    <div class="bar"><span style="width:${pct(value)}"></span></div>
  </div>`;
}

function renderGroup(title, s) {
  if (!s || s.n_annotated === 0) {
    return `<div class="dash-group">
      <div class="group-heading">${title}</div>
      <p class="muted">No PDFs in this view yet.</p>
    </div>`;
  }
  return `<div class="dash-group">
    <div class="group-heading">${title} <span class="group-count">${s.n_annotated} PDFs</span></div>

    <div class="stat-section-title">Coverage</div>
    <div class="stat-grid">
      ${statCard(s.n_annotated, "PDFs annotated", `${s.n_gt_multidoc} multi-doc · ${s.n_gt_single} single-doc`)}
      ${statCard(s.n_with_model, "Compared vs model", s.n_no_model ? `${s.n_no_model} without model yet` : "all have a model split")}
      ${statCard(s.n_exact, "Exact split matches", `of ${s.n_with_model} compared`)}
      ${statCard(s.n_multidoc_correct, "Multidoc classified OK", `of ${s.n_with_model} compared`)}
    </div>

    <div class="stat-section-title">Rates</div>
    <div class="stat-grid">
      ${bar("Exact match rate", s.exact_rate)}
      ${bar("Multidoc correctness rate", s.multidoc_rate)}
    </div>

    <div class="stat-section-title">Boundary metrics (avg over compared PDFs)</div>
    <div class="stat-grid">
      ${statCard(s.avg_precision, "Avg precision")}
      ${statCard(s.avg_recall, "Avg recall")}
      ${statCard(s.avg_f1, "Avg F1")}
      ${statCard(s.avg_f1_tol, "Avg F1 (±1)")}
      ${statCard(s.n_offby1_total, "Off-by-one boundaries", "total across all PDFs", true)}
    </div>
  </div>`;
}

async function load() {
  const res = await fetch("/api/annotate/stats");
  const data = await res.json();
  if (data.error) {
    document.getElementById("dash-content").innerHTML =
      `<p class="muted">Error: ${data.error}</p>`;
    return;
  }
  const all = data.all;
  const now = new Date().toLocaleString();
  document.getElementById("dash-generated").textContent = `Generated ${now}`;
  document.getElementById("dash-foot").textContent =
    `${all.n_annotated} PDFs · ${all.n_with_model} compared vs model`;

  document.getElementById("dash-content").innerHTML =
    renderGroup("Overall", all) +
    renderGroup("Multidocument", data.multidoc) +
    renderGroup("Non-multidocument", data.single);
}

// ── Export: PNG via html2canvas ──────────────────────────────────────────────
async function downloadPNG() {
  const node = document.getElementById("dash-capture");
  // Pin the capture to the theme actually on screen. html2canvas doesn't resolve
  // prefers-color-scheme, so a hardcoded colour renders light content on a dark
  // canvas (or the reverse) depending on the viewer's OS setting.
  const bg = getComputedStyle(document.documentElement)
    .getPropertyValue("--surface").trim() || "#fcfcfb";
  const canvas = await html2canvas(node, { backgroundColor: bg, scale: 2 });
  const a = document.createElement("a");
  a.href = canvas.toDataURL("image/png");
  a.download = `ground_truth_dashboard_${stamp()}.png`;
  a.click();
}

// ── Export: self-contained HTML (inlined CSS + current DOM) ──────────────────
async function downloadHTML() {
  const css = await fetch("/static/css/control_tower.css").then((r) => r.text());
  const capture = document.getElementById("dash-capture").outerHTML;
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Ground Truth Dashboard</title><style>${css}</style></head>
<body id="dash-body"><div class="dash-wrap">${capture}</div></body></html>`;
  const blob = new Blob([html], { type: "text/html" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `ground_truth_dashboard_${stamp()}.html`;
  a.click();
  URL.revokeObjectURL(a.href);
}

function stamp() {
  return new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
}

document.getElementById("btn-png").onclick = downloadPNG;
document.getElementById("btn-html").onclick = downloadHTML;
load();
