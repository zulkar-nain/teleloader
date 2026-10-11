const $ = (id) => document.getElementById(id);

function fmtBytes(n) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}

function fmtTime(sec) {
  if (!isFinite(sec) || sec <= 0) return "--";
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = Math.floor(sec % 60);
  return h ? `${h}h ${m}m` : m ? `${m}m ${s}s` : `${s}s`;
}

// Smoothed speed from successive byte counts
let last = { bytes: 0, t: performance.now() };
let speed = 0;

function render(s) {
  const pct = s.total_bytes ? Math.min(100, (s.downloaded_bytes / s.total_bytes) * 100) : 0;

  $("status").textContent = s.status;
  $("status").className = `badge ${s.status}`;
  document.body.classList.toggle("done", s.status === "done");

  $("overall-fill").style.width = `${pct}%`;
  $("overall-pct").textContent = `${pct.toFixed(1)}%`;
  $("overall-bytes").textContent = `${fmtBytes(s.downloaded_bytes)} / ${fmtBytes(s.total_bytes)}`;

  $("files").textContent = `${s.completed} / ${s.total_files}`;
  $("size").textContent = fmtBytes(s.downloaded_bytes);
  $("skipped").textContent = s.skipped;
  $("failed").textContent = s.failed;

  const now = performance.now();
  const dt = (now - last.t) / 1000;
  if (dt > 0) {
    const inst = Math.max(0, (s.downloaded_bytes - last.bytes) / dt);
    speed = speed ? speed * 0.7 + inst * 0.3 : inst;
  }
  last = { bytes: s.downloaded_bytes, t: now };
  if (s.status !== "downloading") speed = 0;

  $("speed").textContent = `${fmtBytes(speed)}/s`;
  $("eta").textContent = speed > 1 ? fmtTime((s.total_bytes - s.downloaded_bytes) / speed) : "--";

  const active = $("active");
  active.replaceChildren();
  if (!s.active.length) {
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent = s.status === "scanning" ? "Scanning chat..." : "Nothing downloading";
    active.append(p);
  }
  for (const a of s.active) {
    const p = a.total ? (a.current / a.total) * 100 : 0;
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `
      <div class="row"><span class="name"></span><span class="meta"></span></div>
      <div class="bar"><div class="fill"></div></div>`;
    item.querySelector(".name").textContent = a.name;
    item.querySelector(".meta").textContent = `${fmtBytes(a.current)} / ${fmtBytes(a.total)} (${p.toFixed(0)}%)`;
    item.querySelector(".fill").style.width = `${p}%`;
    active.append(item);
  }

  const log = $("log");
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 8;
  log.replaceChildren();
  for (const l of s.logs) {
    const li = document.createElement("li");
    li.className = l.level;
    const t = document.createElement("span");
    t.className = "t";
    t.textContent = l.t;
    const msg = document.createElement("span");
    msg.textContent = l.text;
    li.append(t, msg);
    log.append(li);
  }
  if (atBottom) log.scrollTop = log.scrollHeight;
}

// --- Controls ---
let running = false;
let formInit = false;
let wantedChat = null;

function showError(msg) { $("form-error").textContent = msg || ""; }

async function api(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.error || r.statusText);
  return data;
}

function updateControls() {
  for (const id of ["chat", "refresh", "dir", "browse", "workers"]) $(id).disabled = running;
  $("start").disabled = running || !$("chat").value;
  $("stop").disabled = !running;
}

async function loadChats() {
  const sel = $("chat");
  sel.replaceChildren(new Option("Loading chats...", ""));
  showError("");
  try {
    const r = await fetch("/api/chats");
    if (!r.ok) throw new Error("Could not load chats");
    const chats = await r.json();
    sel.replaceChildren(new Option("Select a chat", ""));
    for (const c of chats) {
      const o = new Option(`${c.name} (${c.type})`, c.id);
      o.dataset.name = c.name;
      sel.add(o);
    }
    if (wantedChat !== null) sel.value = String(wantedChat);
  } catch (err) {
    sel.replaceChildren(new Option("Failed to load chats", ""));
    showError(err.message);
  }
  updateControls();
}

function syncForm(s) {
  running = s.status === "scanning" || s.status === "downloading";
  if (!formInit) {
    $("dir").value = s.config.output_dir;
    $("workers").value = s.config.workers;
    wantedChat = s.config.chat_id;
    formInit = true;
    loadChats();
  }
  updateControls();
}

$("chat").addEventListener("change", updateControls);
$("refresh").addEventListener("click", loadChats);

$("browse").addEventListener("click", async () => {
  showError("");
  try {
    const { path } = await api("/api/browse", { initial: $("dir").value });
    if (path) $("dir").value = path;
  } catch (err) { showError(err.message); }
});

$("start").addEventListener("click", async () => {
  showError("");
  const opt = $("chat").selectedOptions[0];
  try {
    await api("/api/start", {
      chat_id: $("chat").value,
      chat_name: opt.dataset.name || "",
      output_dir: $("dir").value,
      workers: $("workers").value,
    });
    running = true;
    updateControls();
  } catch (err) { showError(err.message); }
});

$("stop").addEventListener("click", async () => {
  try { await api("/api/stop"); } catch (err) { showError(err.message); }
});

const es = new EventSource("/api/events");
es.onmessage = (e) => {
  const s = JSON.parse(e.data);
  syncForm(s);
  render(s);
};
es.onerror = () => { $("status").textContent = "disconnected"; $("status").className = "badge"; };
