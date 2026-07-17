// Annotate tab — server-rendered page images + boundary annotation.
// Operator-blind: the model's prediction is never sent to this page. Pages are
// JPEGs rendered server-side by PyMuPDF and lazy-loaded as they scroll in.
//
// This file is an IIFE exporting one global on purpose. control_tower.js already
// declares `state`, `$`, `esc`, `toast`, `dayQ` at top level; a second top-level
// `const state` would be a SyntaxError that kills this whole file before a line
// of it runs — and the shell would keep working, so it would look "not wired up"
// rather than broken. Everything shared is reused from the shell, never redeclared.
//
// Loads AFTER control_tower.js.

const Annotate = (() => {
  // Batch identity lives in the shell (state.dayId) — never duplicated here.
  const ann = {
    filename: null,
    folderId: null,
    totalPages: 0,
    starts: new Set([1]),   // ground-truth boundaries (doc start pages)
    dirty: false,
    loadedDay: null,        // which batch the current worklist belongs to
  };

  let pageObserver = null;  // IntersectionObserver for lazy image loading

  // Every annotate URL goes through this. Building one by hand risks hitting the
  // pipeline's /api/file/<name>, which answers 200 with a different shape — the
  // viewer would render blank with no error anywhere.
  const A = (path) => `/api/annotate${path}`;

  // ── Worklist ──────────────────────────────────────────────────────────────
  async function loadWorklist(selectFirst = false) {
    const body = $("ann-body");
    if (!state.dayId) {
      body.classList.add("hidden");
      $("ann-nobatch").classList.remove("hidden");
      return;
    }
    $("ann-nobatch").classList.add("hidden");
    body.classList.remove("hidden");

    const wl = await jget(A(`/worklist?${dayQ()}`));
    if (wl.error) {
      toast(`Worklist: ${wl.error}`, true);
      return;
    }
    ann.loadedDay = state.dayId;

    $("ann-n-pending").textContent = wl.n_pending;
    $("ann-n-done").textContent = wl.n_completed;
    const pct = wl.n_total ? Math.round((wl.n_completed / wl.n_total) * 100) : 0;
    $("ann-progress-fill").style.width = `${pct}%`;

    renderList($("ann-pending"), wl.pending || [], false);
    renderList($("ann-completed"), wl.completed || [], true);

    if (!wl.n_total) {
      $("ann-empty").innerHTML =
        "<p>No annotation sample for this batch yet — run ingest first.</p>";
    }
    if (selectFirst && (wl.pending || []).length) loadFile(wl.pending[0]);
  }

  function renderList(ul, names, done) {
    ul.innerHTML = "";
    names.forEach((name) => {
      const li = document.createElement("li");
      li.textContent = name;            // textContent, so no escaping needed
      li.className = "wl-item" + (done ? " done" : "");
      if (name === ann.filename) li.classList.add("active");
      li.onclick = () => loadFile(name);
      ul.appendChild(li);
    });
  }

  function markActiveInList() {
    document.querySelectorAll("#panel-annotate .wl-item").forEach((li) => {
      li.classList.toggle("active", li.textContent === ann.filename);
    });
  }

  // ── Load a file ───────────────────────────────────────────────────────────
  async function loadFile(name) {
    if (ann.dirty && !confirm("Unsaved annotation. Discard your marks?")) return;

    ann.filename = name;
    ann.starts = new Set([1]);
    ann.dirty = false;
    $("ann-file").textContent = name;
    $("ann-empty").classList.add("hidden");
    $("ann-pages").innerHTML = "<div class='loading'>Loading…</div>";

    const detail = await jget(A(`/file/${encodeURIComponent(name)}?${dayQ()}`));
    if (detail.error) {
      $("ann-pages").innerHTML = `<div class='loading'>Error: ${esc(detail.error)}</div>`;
      return;
    }
    // Guard the shape, not just the error key: a response without total_pages
    // renders zero page shells silently instead of failing.
    if (!Number.isInteger(detail.total_pages) || detail.total_pages < 1) {
      $("ann-pages").innerHTML =
        "<div class='loading'>Error: unexpected response — no page count.</div>";
      return;
    }

    ann.totalPages = detail.total_pages;
    ann.folderId = detail.folder_id ?? null;

    // Pre-fill from existing GT if re-annotating; else just page 1.
    if (detail.ground_truth?.predicted_starts) {
      ann.starts = new Set(detail.ground_truth.predicted_starts);
      $("ann-multidoc").checked = !!detail.ground_truth.is_multidoc;
    } else {
      $("ann-multidoc").checked = false;
    }

    $("ann-pagecount").textContent = `${ann.totalPages} pages`;
    renderPageShells();
    syncControls();
    markActiveInList();
  }

  // ── Page shells + lazy image loading ──────────────────────────────────────
  function renderPageShells() {
    if (pageObserver) pageObserver.disconnect();
    const pages = $("ann-pages");
    pages.innerHTML = "";

    for (let n = 1; n <= ann.totalPages; n++) {
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
      pages.appendChild(card);
    }

    // root: null = the viewport. The shell scrolls the document (#main has no
    // overflow of its own), so anchoring to a non-scrolling element here would
    // make every card intersect at once and load the whole PDF up front.
    pageObserver = new IntersectionObserver(
      (entries, obs) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          loadPageImage(entry.target);
          obs.unobserve(entry.target);
        });
      },
      { root: null, rootMargin: "800px 0px" }
    );
    pages.querySelectorAll(".page-card").forEach((c) => pageObserver.observe(c));

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
    img.src = A(`/page/${encodeURIComponent(ann.filename)}/${n}?${dayQ()}`);
  }

  function toggleStart(n) {
    if (n === 1) return; // page 1 is always a document start
    if (ann.starts.has(n)) ann.starts.delete(n);
    else ann.starts.add(n);
    ann.dirty = true;
    $("ann-multidoc").checked = ann.starts.size > 1;
    paintBoundaries();
    syncControls();
  }

  function paintBoundaries() {
    document.querySelectorAll("#ann-pages .page-card").forEach((card) => {
      const n = parseInt(card.dataset.page, 10);
      const isStart = ann.starts.has(n);
      card.classList.toggle("is-start", isStart);
      // The label is the accessibility contract for the boundary: the coloured
      // border never carries this meaning on its own.
      const badge = card.querySelector(".page-badge");
      badge.textContent = isStart ? (n === 1 ? "▶ DOC START (1)" : "▶ NEW DOCUMENT") : "";
    });
  }

  function syncControls() {
    const docs = ann.starts.size;
    $("ann-doccount").textContent = `${docs} document${docs > 1 ? "s" : ""}`;
    $("ann-save").disabled = !ann.filename;
    $("ann-reset").disabled = !ann.filename;
  }

  // ── Save ──────────────────────────────────────────────────────────────────
  async function save() {
    if (!ann.filename) return;
    const btn = $("ann-save");
    btn.disabled = true;
    btn.textContent = "Saving…";

    const res = await jpost(A("/save"), {
      filename: ann.filename,
      day_id: state.dayId,
      folder_id: ann.folderId,
      total_pages: ann.totalPages,
      predicted_starts: [...ann.starts].sort((a, b) => a - b),
      is_multidoc: $("ann-multidoc").checked,
    });

    btn.textContent = "Save & Evaluate";
    btn.disabled = false;
    if (res.error) return toast(`Save failed: ${res.error}`, true);

    ann.dirty = false;
    showEval(res.evaluation);
    loadWorklist();
    // The gate reads the same ground_truth/ volume this just wrote to, so the
    // batch list's lifecycle and gate counter are now stale.
    loadDays();
  }

  // ── Evaluation dialog (shown only AFTER the operator commits) ──────────────
  function showEval(ev) {
    const title = $("dlg-eval-title");
    const body = $("dlg-eval-body");
    if (!ev || ev.model_prediction_missing) {
      title.textContent = "Saved — no model prediction yet";
      body.innerHTML =
        "<p class='muted'>Ground truth stored. This file has no model split result yet, so no comparison was computed.</p>";
    } else {
      title.textContent = ev.exact_match ? "✓ Exact match vs model" : "Mismatch vs model";
      body.innerHTML = `
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
    $("dlg-eval").showModal();
  }

  function metric(label, value, good) {
    // .ok/.bad add a ✓/✗ glyph in CSS — the state is never colour-only.
    const cls = good === undefined ? "" : good ? "ok" : "bad";
    return `<div class="metric ${cls}"><span class="m-val">${esc(value)}</span>` +
           `<span class="m-lbl">${esc(label)}</span></div>`;
  }

  function resetStarts() {
    ann.starts = new Set([1]);
    $("ann-multidoc").checked = false;
    ann.dirty = true;
    paintBoundaries();
    syncControls();
  }

  // ── Tab lifecycle ─────────────────────────────────────────────────────────
  function enter() {
    $("ann-day").textContent = state.dayId || "";
    if (state.dayId && state.dayId !== ann.loadedDay) onDayChange();
    loadWorklist();
    // leave() drops the rendered pages; rebuild them for the file still open.
    // The images come back from the browser cache (Cache-Control: max-age=3600).
    if (ann.filename && ann.totalPages) {
      renderPageShells();
      syncControls();
    }
  }

  function leave() {
    if (pageObserver) {
      pageObserver.disconnect();
      pageObserver = null;
    }
    // A few hundred decoded JPEGs is real memory; enter() re-renders on return.
    const pages = $("ann-pages");
    if (pages) pages.innerHTML = "";
  }

  function onDayChange() {
    ann.filename = null;
    ann.folderId = null;
    ann.totalPages = 0;
    ann.starts = new Set([1]);
    ann.dirty = false;
    ann.loadedDay = null;
    $("ann-file").textContent = "Select a file to begin";
    $("ann-pagecount").textContent = "";
    $("ann-pages").innerHTML = "";
    $("ann-empty").classList.remove("hidden");
    syncControls();
  }

  // ── Wiring ────────────────────────────────────────────────────────────────
  $("ann-save").onclick = save;
  $("ann-reset").onclick = resetStarts;
  $("ann-multidoc").onchange = () => { ann.dirty = true; };
  $("dlg-eval-next").onclick = () => {
    $("dlg-eval").close();
    loadWorklist(true);
  };

  window.addEventListener("beforeunload", (e) => {
    if (ann.dirty) {
      e.preventDefault();
      e.returnValue = "";
    }
  });

  return { enter, leave, onDayChange, isDirty: () => ann.dirty };
})();
window.Annotate = Annotate;
