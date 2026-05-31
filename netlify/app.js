/**
 * ══════════════════════════════════════════════════════
 *  MW TRADER — Dashboard App Logic (app.js)
 *  Supabase polling every 15s + Binance live price WS
 * ══════════════════════════════════════════════════════
 *
 *  SETUP: Replace the two config values below with your
 *  Supabase project URL and anon (public) key.
 *  These are READ-ONLY keys — safe for frontend.
 */

// ─────────────────────────────────────────────
//  ★ CONFIG — Replace with your values ★
// ─────────────────────────────────────────────
const SUPABASE_URL      = "https://tfpxzbnakribhnwiitgp.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRmcHh6Ym5ha3JpYmhud2lpdGdwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODAyMDIwMDMsImV4cCI6MjA5NTc3ODAwM30.RukS8j9odMnxtlbALALDnF-agz43fzbCp46U1cY8dFM";

const POLL_INTERVAL     = 15000;   // 15 seconds
const PAGE_SIZE         = 20;      // history rows per page

// ─────────────────────────────────────────────
//  INIT SUPABASE CLIENT
// ─────────────────────────────────────────────
const { createClient } = supabase;
const db = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ─────────────────────────────────────────────
//  GLOBAL STATE
// ─────────────────────────────────────────────
let currentPage     = 0;
let totalHistory    = 0;
let countdownTimer  = POLL_INTERVAL / 1000;
let livePrice       = null;
let binanceWs       = null;
let currentSymbol   = "BTCUSDT";
let currentTf       = "1m";

// ─────────────────────────────────────────────
//  UTILITY HELPERS
// ─────────────────────────────────────────────
function fmt(val, decimals = 2) {
  if (val === null || val === undefined || val === "") return "—";
  return Number(val).toFixed(decimals);
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" }) + " " +
         d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
}

function el(id) { return document.getElementById(id); }

function flashEl(id) {
  const elem = el(id);
  if (!elem) return;
  elem.classList.remove("flash-update");
  void elem.offsetWidth;
  elem.classList.add("flash-update");
}

// ─────────────────────────────────────────────
//  ENGINE STATUS
// ─────────────────────────────────────────────
async function loadEngineStatus() {
  try {
    const { data, error } = await db
      .from("engine_status")
      .select("*")
      .eq("id", 1)
      .single();

    if (error) {
      if (error.code === "PGRST116") {
        const statusEl = document.getElementById("engine-status-val");
        if (statusEl) {
          statusEl.textContent = "WARMING UP UI...";
          statusEl.style.color = "var(--yellow)";
        }
        return;
      }
      console.error("Supabase Error fetch engine_status:", error);
      const statusEl = document.getElementById("engine-status-val");
      if (statusEl) {
        statusEl.textContent = "RLS ERROR (Check Console)";
        statusEl.style.color = "var(--red)";
      }
      return;
    }
    if (!data) return;

    // Header: symbol + timeframe
    currentSymbol = data.symbol || "BTCUSDT";
    currentTf     = data.timeframe || "1m";
    el("symbol-val").textContent = currentSymbol;
    el("tf-val").textContent     = currentTf;

    // Engine status dot
    let isStale = false;
    if (data.updated_at) {
      const lastUpdated = new Date(data.updated_at).getTime();
      const now = Date.now();
      if (now - lastUpdated > 45000) { // 45s stale -> engine is offline
        isStale = true;
      }
    }

    const status   = (data.status || "STOPPED").toUpperCase();
    const wsStatus = (data.websocket_status || "DISCONNECTED").toUpperCase();
    const dot      = el("status-dot");
    const statusEl = el("engine-status-val");

    if (isStale) {
      dot.className     = "status-dot stopped";
      statusEl.textContent = "OFFLINE (Stale Data)";
      statusEl.style.color = "var(--red)";
    } else if (status === "RUNNING" && wsStatus === "CONNECTED") {
      dot.className     = "status-dot running";
      statusEl.textContent = "LIVE / Connected";
      statusEl.style.color = "var(--green)";
    } else if (wsStatus === "RECONNECTING" || wsStatus === "ERROR") {
      dot.className     = "status-dot error";
      statusEl.textContent = "RECONNECTING";
      statusEl.style.color = "var(--red)";
    } else {
      dot.className     = "status-dot stopped";
      statusEl.textContent = "OFFLINE / Disconnected";
      statusEl.style.color = "var(--yellow)";
    }

    // Last price from engine (fallback if WS not connected)
    if (!livePrice && data.last_price) {
      el("live-price").textContent = "$" + fmt(data.last_price, 2);
    }

    // Stats
    el("st-total").textContent   = data.total_signals || 0;
    el("st-wins").textContent    = data.wins  || 0;
    el("st-losses").textContent  = data.losses || 0;
    const wr = data.win_rate ? data.win_rate.toFixed(1) + "%" : "0%";
    el("st-winrate").textContent = wr;

    // Start Binance live price WS if needed
    if (!binanceWs || binanceWs.readyState === WebSocket.CLOSED || (Date.now() - wsLastUpdate > 10000 && binanceWs.readyState === WebSocket.OPEN)) {
      console.log("WebSocket is stale or disconnected. Reconnecting...");
      connectBinancePrice(currentSymbol);
    }

  } catch (e) {
    console.warn("Engine status load error:", e);
  }
}

// ─────────────────────────────────────────────
//  LATEST SIGNAL
// ─────────────────────────────────────────────
async function loadLatestSignal() {
  try {
    const { data, error } = await db
      .from("latest_signal")
      .select("*")
      .eq("id", 1)
      .single();

    if (error) {
      if (error.code === "PGRST116") {
        el("signal-reason").textContent = "No signal data yet. Engine warming up...";
        return;
      }
      console.error("Supabase Error fetch latest_signal:", error);
      el("signal-reason").textContent = "RLS ERROR (Check/Console)";
      return;
    }
    if (!data) {
      el("signal-reason").textContent = "No signal data. Engine may not be running.";
      return;
    }

    renderSignal(data);
    el("last-updated").textContent = fmtTime(new Date().toISOString());
    flashEl("signal-card");

  } catch (e) {
    console.warn("Signal load error:", e);
  }
}

function renderSignal(sig) {
  const dir = (sig.direction || "NO_TRADE").toUpperCase();
  const status = (sig.status || "NO_TRADE").toUpperCase();

  // Direction block
  const dirBlock = el("direction-block");
  dirBlock.className = "signal-direction-block";
  if (dir === "BUY")  dirBlock.classList.add("buy");
  if (dir === "SELL") dirBlock.classList.add("sell");

  // Icon + label
  const icons = { BUY: "▲", SELL: "▼", NO_TRADE: "◉" };
  const iconColors = { BUY: "var(--green)", SELL: "var(--red)", NO_TRADE: "var(--muted)" };
  const dirEl = el("direction-icon");
  dirEl.textContent     = icons[dir] || "◉";
  dirEl.style.color     = iconColors[dir] || "var(--muted)";

  const dlEl = el("direction-label");
  dlEl.textContent  = dir;
  dlEl.style.color  = iconColors[dir] || "var(--muted)";

  el("direction-sub").textContent = sig.structure_reason || "No active setup";

  // Confidence ring
  const conf = parseFloat(sig.confidence || 0);
  const circumference = 2 * Math.PI * 34;
  const offset = circumference - (conf / 100) * circumference;
  const ring = el("ring-fill");
  ring.style.strokeDashoffset = offset;
  ring.style.stroke = conf >= 70 ? "var(--green)" : conf >= 50 ? "var(--cyan)" : "var(--yellow)";
  el("conf-num").textContent = Math.round(conf);

  // Levels
  if (sig.entry_low && sig.entry_high) {
    el("lv-entry").textContent = `$${fmt(sig.entry_low)} – $${fmt(sig.entry_high)}`;
  } else {
    el("lv-entry").textContent = "—";
  }
  el("lv-current").textContent = livePrice ? `$${livePrice}` : (sig.current_price ? `$${fmt(sig.current_price)}` : "—");
  el("lv-sl").textContent  = sig.stop_loss  ? `$${fmt(sig.stop_loss)}` : "—";
  el("lv-tp1").textContent = sig.tp1 ? `$${fmt(sig.tp1)}` : "—";
  el("lv-tp2").textContent = sig.tp2 ? `$${fmt(sig.tp2)}` : "—";
  el("lv-tp3").textContent = sig.tp3 ? `$${fmt(sig.tp3)}` : "—";

  // R:R
  el("rr-tp1").textContent = sig.risk_reward_tp1 ? `${sig.risk_reward_tp1}R` : "";
  el("rr-tp2").textContent = sig.risk_reward_tp2 ? `${sig.risk_reward_tp2}R` : "";
  el("rr-tp3").textContent = sig.risk_reward_tp3 ? `${sig.risk_reward_tp3}R` : "";

  // Meta
  el("sig-regime").textContent  = sig.market_regime  || "—";
  el("sig-of").textContent      = sig.orderflow_label || "—";
  applyLabelColor("sig-of", sig.orderflow_label);
  applyRegimeColor("sig-regime", sig.market_regime);

  const expiry = sig.expires_at ? fmtTime(sig.expires_at) : "—";
  el("sig-expires").textContent = expiry;

  // Reason
  if (dir === "NO_TRADE") {
    el("signal-reason").innerHTML = `<span style="color:var(--cyan)">🤖 ENGINE ACTIVE & SCANNING EXPERT SETUPS...</span><br/><br/><span style="color:var(--muted)">Currently no high-probability technical pattern detected. The ML model is analyzing live Binance orderflow and will generate a signal when conditions align (can take a few minutes).</span>`;
  } else {
    el("signal-reason").textContent = sig.full_reason || "No reason provided.";
  }

  // Quality badge
  const qBadge = el("sig-quality");
  const quality = (sig.signal_quality || "—").toUpperCase();
  qBadge.textContent = quality;
  qBadge.className   = "badge-quality";
  if (quality === "A+") qBadge.classList.add("aplus");
  else if (quality === "A") qBadge.classList.add("a");
  else if (quality === "B") qBadge.classList.add("b");
  else if (quality === "C") qBadge.classList.add("c");

  // Status badge
  const sBadge = el("sig-status-badge");
  const statusLower = status.toLowerCase().replace("_", "-");
  sBadge.textContent = status.replace("_", " ");
  sBadge.className   = `badge-status ${statusLower}`;

  // Probability bars
  setProbBar("prob-tp1-bar", "prob-tp1-val", sig.tp1_probability);
  setProbBar("prob-tp2-bar", "prob-tp2-val", sig.tp2_probability);
  setProbBar("prob-tp3-bar", "prob-tp3-val", sig.tp3_probability);
  setProbBar("prob-sl-bar",  "prob-sl-val",  sig.sl_risk);
  setProbBar("prob-conf-bar","prob-conf-val", sig.confidence);
}

function setProbBar(barId, valId, value) {
  const v   = parseFloat(value || 0);
  const bar = el(barId);
  const txt = el(valId);
  if (bar) bar.style.width = Math.min(v, 100) + "%";
  if (txt) txt.textContent = v.toFixed(1) + "%";
}

function applyLabelColor(elId, label) {
  const elem = el(elId);
  if (!elem) return;
  const map = {
    "BUY_STRONG":  "var(--green)",
    "BUY_WEAK":    "#60d090",
    "SELL_STRONG": "var(--red)",
    "SELL_WEAK":   "#d06070",
    "NEUTRAL":     "var(--muted)",
    "NO_DATA":     "var(--muted2)",
  };
  elem.style.color = map[label] || "var(--muted)";
}

function applyRegimeColor(elId, regime) {
  const elem = el(elId);
  if (!elem) return;
  const map = {
    "UPTREND":   "var(--green)",
    "DOWNTREND": "var(--red)",
    "RANGE":     "var(--cyan)",
    "CHOPPY":    "var(--yellow)",
  };
  elem.style.color = map[regime] || "var(--muted)";
}

// ─────────────────────────────────────────────
//  ORDERFLOW PANEL (from latest_signal)
// ─────────────────────────────────────────────
async function loadOrderflow() {
  // Orderflow snapshot is embedded in the signal or we show from market_snapshots
  // For now render from latest_signal orderflow_label + build panel from it
  try {
    const { data, error } = await db
      .from("latest_signal")
      .select("orderflow_label, current_price")
      .eq("id", 1)
      .single();

    if (error && error.code !== "PGRST116") {
      throw error;
    }
    // Build placeholder orderflow grid using available label
    // (Full per-window data would require market_snapshots table population)
    renderOrderflowPanel(data || {});
  } catch(e) {
    console.warn("Orderflow load error:", e);
  }
}

function renderOrderflowPanel(signalData) {
  const grid     = el("of-grid");
  const windows  = ["30s", "1m", "3m", "5m", "15m"];
  const label    = signalData?.orderflow_label || "NO_DATA";

  // Simulate reasonable display from available signal data
  grid.innerHTML = "";
  if (label === "NO_DATA") {
    grid.innerHTML = `<div class="of-loading" style="padding:40px;text-align:center;color:var(--muted)">Waiting for live engine orderflow streams... Data will populate once the ML model warms up.</div>`;
    return;
  }
  windows.forEach(tf => {
    const div = document.createElement("div");
    div.className = "of-window " + labelClass(label);

    const buyPct  = getBuyPct(label);
    const sellPct = 100 - buyPct;
    const delta   = ((buyPct - sellPct) / 100).toFixed(3);
    const deltaColor = buyPct >= 50 ? "var(--green)" : "var(--red)";

    div.innerHTML = `
      <div class="of-header">
        <span class="of-tf">${tf}</span>
        <span class="of-label ${labelClass(label)}">${label.replace("_", " ")}</span>
      </div>
      <div class="of-bars">
        <div class="of-bar-row">
          <span class="of-bar-label" style="color:var(--green)">BUY</span>
          <div class="of-bar-track">
            <div class="of-bar-fill buy" style="width:${buyPct}%"></div>
          </div>
        </div>
        <div class="of-bar-row">
          <span class="of-bar-label" style="color:var(--red)">SELL</span>
          <div class="of-bar-track">
            <div class="of-bar-fill sell" style="width:${sellPct}%"></div>
          </div>
        </div>
      </div>
      <div class="of-delta" style="color:${deltaColor}">Δ ${delta > 0 ? "+" : ""}${delta}</div>
      <div class="of-vol">Window: ${tf}</div>
    `;
    grid.appendChild(div);
  });
}

function labelClass(label) {
  const map = {
    "BUY_STRONG":  "buy-strong",
    "BUY_WEAK":    "buy-weak",
    "SELL_STRONG": "sell-strong",
    "SELL_WEAK":   "sell-weak",
    "NEUTRAL":     "neutral",
    "NO_DATA":     "neutral",
  };
  return map[label] || "neutral";
}

function getBuyPct(label) {
  const map = {
    "BUY_STRONG": 72, "BUY_WEAK": 58,
    "SELL_STRONG": 28, "SELL_WEAK": 42,
    "NEUTRAL": 50, "NO_DATA": 50
  };
  return map[label] || 50;
}

// ─────────────────────────────────────────────
//  SIGNAL HISTORY
// ─────────────────────────────────────────────
async function loadHistory(page = 0) {
  currentPage = page;
  el("history-tbody").innerHTML = `<tr><td colspan="12" class="table-empty">Loading...</td></tr>`;
  try {
    const from = page * PAGE_SIZE;
    const to   = from + PAGE_SIZE - 1;

    const { data, error, count } = await db
      .from("signal_history")
      .select("*", { count: "exact" })
      .order("created_at", { ascending: false })
      .range(from, to);

    if (error) throw error;
    totalHistory = count || 0;
    el("history-count").textContent = `${totalHistory} total signals`;

    renderHistoryTable(data || []);
    renderPagination(totalHistory);
  } catch (e) {
    console.warn("History load error:", e);
    el("history-tbody").innerHTML = `<tr><td colspan="12" class="table-empty">Failed to load history. Check Supabase config.</td></tr>`;
  }
}

function renderHistoryTable(rows) {
  const tbody = el("history-tbody");
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="12" class="table-empty">No history yet. Run the engine to generate signals.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows.map(r => {
    const dir    = (r.direction || "—").toUpperCase();
    const result = (r.result    || "OPEN").toUpperCase();
    const level  = (r.hit_level || "—").toUpperCase();

    const dirClass    = dir === "BUY" ? "dir-buy" : dir === "SELL" ? "dir-sell" : "";
    const resultClass = {
      "WIN":       "result-win",
      "LOSS":      "result-loss",
      "EXPIRED":   "result-expired",
      "OPEN":      "result-open",
      "CANCELLED": "result-cancelled",
    }[result] || "";

    const entry = r.entry_low ? `$${fmt(r.entry_low)}` : "—";
    const sl    = r.stop_loss ? `$${fmt(r.stop_loss)}` : "—";
    const tp1   = r.tp1 ? `$${fmt(r.tp1)}` : "—";
    const tp2   = r.tp2 ? `$${fmt(r.tp2)}` : "—";
    const tp3   = r.tp3 ? `$${fmt(r.tp3)}` : "—";
    const conf  = r.confidence ? r.confidence.toFixed(1) + "%" : "—";
    const qual  = r.signal_quality || "—";
    const regime = r.market_regime || "—";

    return `
      <tr>
        <td style="color:var(--muted)">${fmtDate(r.created_at)}</td>
        <td class="${dirClass}">${dir}</td>
        <td>${entry}</td>
        <td style="color:var(--red)">${sl}</td>
        <td style="color:var(--green)">${tp1}</td>
        <td style="color:var(--green)">${tp2}</td>
        <td style="color:var(--green)">${tp3}</td>
        <td style="color:var(--cyan)">${conf}</td>
        <td style="color:var(--yellow)">${qual}</td>
        <td class="${resultClass}">${result}</td>
        <td style="color:var(--muted)">${level}</td>
        <td style="color:var(--muted)">${regime}</td>
      </tr>
    `;
  }).join("");
}

function renderPagination(total) {
  const pages = Math.ceil(total / PAGE_SIZE);
  const pag   = el("pagination");
  pag.innerHTML = "";
  if (pages <= 1) return;

  for (let i = 0; i < pages; i++) {
    const btn = document.createElement("button");
    btn.className = "page-btn" + (i === currentPage ? " active" : "");
    btn.textContent = i + 1;
    btn.onclick = () => loadHistory(i);
    pag.appendChild(btn);
  }
}

// ─────────────────────────────────────────────
//  BINANCE LIVE PRICE (optional, browser WS)
// ─────────────────────────────────────────────
let wsLastUpdate = 0;

function connectBinancePrice(symbol) {
  try {
    // Prevent multiple connections
    if (binanceWs && (binanceWs.readyState === WebSocket.OPEN || binanceWs.readyState === WebSocket.CONNECTING)) {
      return;
    }
    if (binanceWs) {
      binanceWs.close();
    }
    // Using futures aggTrade for real-time live price (milliseconds)
    const wsUrl = `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@aggTrade`;
    console.log("Connecting to Binance Futures WS:", wsUrl);
    binanceWs = new WebSocket(wsUrl);

    binanceWs.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const priceString = data.p; // p is price in aggTrade
        if (priceString) {
          livePrice = parseFloat(priceString).toFixed(2);
          
          const now = Date.now();
          if (now - wsLastUpdate > 50) { // Max 20 fps, so it looks very 'live'
            const lpEl = el("live-price");
            if (lpEl) lpEl.textContent = "$" + livePrice;
            
            const lcEl = el("lv-current");
            if (lcEl) lcEl.textContent = "$" + livePrice;
            
            wsLastUpdate = now;
          }
        } 
      } catch(e) {
          // ignore parse errors
      }
    };

    binanceWs.onerror = () => {
      console.warn("Binance WS error. Price updates paused.");
    };

    binanceWs.onclose = () => {
      // Attempt reconnect after 5s
      setTimeout(() => connectBinancePrice(symbol), 5000);
    };
  } catch (e) {
    console.warn("Cannot connect Binance WS:", e);
  }
}

// ─────────────────────────────────────────────
//  COUNTDOWN TIMER
// ─────────────────────────────────────────────
function startCountdown() {
  countdownTimer = POLL_INTERVAL / 1000;
  setInterval(() => {
    countdownTimer--;
    if (countdownTimer <= 0) countdownTimer = POLL_INTERVAL / 1000;
    const cdEl = el("refresh-countdown");
    if (cdEl) cdEl.textContent = countdownTimer;
  }, 1000);
}

// ─────────────────────────────────────────────
//  MAIN POLL LOOP
// ─────────────────────────────────────────────
async function pollAll() {
  await Promise.all([
    loadEngineStatus(),
    loadLatestSignal(),
    loadOrderflow(),
  ]);
}

async function init() {
  console.log("MW TRADER Dashboard v1.0 — Starting...");
  try {
    // Catch-all to display unexpected init errors
    window.addEventListener("error", (e) => {
      const el = document.getElementById("engine-status-val");
      if (el) { el.textContent = "JS ERROR"; el.style.color="red"; }
    });

    // Check if config is default placeholder
    if (SUPABASE_URL.includes("your-project-ref") || SUPABASE_ANON_KEY.includes("your-anon")) {
      console.warn("Placeholder keys detected! Loading Demo Mode so you can see the UI layout...");
      loadDemoData();
      return;
    }

    // Start Binance live price WS immediately
    connectBinancePrice(currentSymbol);

    // Initial load
    await pollAll();
    await loadHistory(0);

    // Polling every 15s
    setInterval(pollAll, POLL_INTERVAL);

    // History refresh every 30s
    setInterval(() => loadHistory(currentPage), 30000);

    // Countdown
    startCountdown();
  } catch (err) {
    console.error("Init Error:", err);
    el("engine-status-val").textContent = "INIT ERROR";
    el("engine-status-val").style.color = "var(--red)";
  }
}

function loadDemoData() {
  // Mock Engine Status
  currentSymbol = "BTCUSDT"; currentTf = "1m";
  el("symbol-val").textContent = currentSymbol;
  el("tf-val").textContent = currentTf;
  el("status-dot").className = "status-dot running";
  el("engine-status-val").textContent = "LIVE";
  el("engine-status-val").style.color = "var(--green)";
  el("st-total").textContent = 142;
  el("st-wins").textContent = 98;
  el("st-losses").textContent = 44;
  el("st-winrate").textContent = "69.0%";
  livePrice = "64520.50";
  el("live-price").textContent = "$" + livePrice;
  el("lv-current").textContent = "$" + livePrice;

  // Mock Signal
  renderSignal({
    direction: "BUY", status: "ACTIVE", entry_low: 64400.00, entry_high: 64500.00,
    current_price: 64520.50, stop_loss: 64100.00, tp1: 64800.00, tp2: 65200.00, tp3: 66000.00,
    risk_reward_tp1: 1.2, risk_reward_tp2: 2.1, risk_reward_tp3: 3.5,
    market_regime: "UPTREND", orderflow_label: "BUY_STRONG",
    expires_at: new Date(Date.now() + 15 * 60000).toISOString(),
    full_reason: "Bullish rejection off local demand zone. Orderflow shift +350. Momentum accelerating.",
    signal_quality: "A+", confidence: 84, tp1_probability: 88, tp2_probability: 65, tp3_probability: 30, sl_risk: 12
  });

  // Mock Orderflow & History
  renderOrderflowPanel({ orderflow_label: "BUY_STRONG" });
  renderHistoryTable([
    { created_at: new Date().toISOString(), direction: "BUY", entry_low: 64400, stop_loss: 64100, tp1: 64800, tp2: 65200, tp3: 66000, confidence: 84, signal_quality: "A+", result: "OPEN", hit_level: "NONE", market_regime: "UPTREND" },
    { created_at: new Date(Date.now() - 3600000).toISOString(), direction: "SELL", entry_low: 65000, stop_loss: 65200, tp1: 64800, tp2: 64500, tp3: 64000, confidence: 75, signal_quality: "A", result: "WIN", hit_level: "TP2", market_regime: "RANGE" },
    { created_at: new Date(Date.now() - 7200000).toISOString(), direction: "SELL", entry_low: 66100, stop_loss: 66500, tp1: 65500, tp2: 65000, tp3: 64000, confidence: 55, signal_quality: "C", result: "LOSS", hit_level: "SL", market_regime: "CHOPPY" }
  ]);
  el("history-count").textContent = "142 total signals";
  el("last-updated").textContent = fmtTime(new Date().toISOString());
}

// ─────────────────────────────────────────────
//  START
// ─────────────────────────────────────────────
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}