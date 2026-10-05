"""
Local dashboard for the price tracker.

    python3 gui.py      -> opens http://127.0.0.1:8765 in your browser

Shows the pipeline stage, a live log, tracked items and alerts.
"Run tracker" runs main.main(); "Simulate price drop" runs main.simulate_price_drop(),
which emails a mock claim to MY_EMAIL instead of the retailer.
"""

import json
import threading
import traceback
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import main as tracker

HOST, PORT = "127.0.0.1", 8765

# ----------------------------------------------------------------------------- event bus

events: list[dict] = []
cond = threading.Condition()
job_lock = threading.Lock()
current_job: str | None = None


def push(event: dict) -> None:
    with cond:
        event["seq"] = len(events)
        events.append(event)
        cond.notify_all()


tracker.LISTENERS.append(push)


def run_job(name: str, fn) -> bool:
    """Run fn in a background thread unless a job is already running."""
    global current_job
    if not job_lock.acquire(blocking=False):
        return False
    current_job = name

    def worker():
        global current_job
        push({"type": "job", "job": name, "running": True})
        try:
            fn()
            push({"type": "job", "job": name, "running": False, "ok": True})
        except Exception as e:
            traceback.print_exc()                     # full detail stays in the terminal
            tracker.log(f"  ✗ {type(e).__name__}: {e}")
            push({"type": "stage", "stage": "error"})
            push({"type": "job", "job": name, "running": False, "ok": False, "error": str(e)})
        finally:
            current_job = None
            job_lock.release()

    threading.Thread(target=worker, daemon=True).start()
    return True


def snapshot() -> dict:
    state = tracker.load_state()
    checked, alerted = state.get("last_checked", {}), state.get("alerted", {})
    items = [{**it, "last": checked.get(key), "alerted_at": alerted.get(key), "days_left": tracker.days_left(it)}
             for key, it in state["items"].items()]
    items.sort(key=lambda x: x["order_date"], reverse=True)
    return {
        "items": items,
        "retailers": [{"name": r["name"], "url": r["orders_url"], "window_days": r["window_days"]}
                      for r in tracker.RETAILERS],
        "min_drop": tracker.MIN_DROP_GBP,
        "auto_send": tracker.AUTO_SEND_CLAIMS,
        "email": tracker.MY_EMAIL,
        "job": current_job,
    }

# ----------------------------------------------------------------------------- http

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep the terminal quiet
        pass

    def send_body(self, body: bytes, content_type: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, obj, code=200):
        self.send_body(json.dumps(obj).encode(), "application/json", code)

    def do_GET(self):
        if self.path == "/":
            self.send_body(PAGE.encode(), "text/html; charset=utf-8")
        elif self.path == "/state":
            self.send_json(snapshot())
        elif self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            sent = 0
            try:
                while True:
                    with cond:
                        cond.wait_for(lambda: len(events) > sent, timeout=15)
                        batch = events[sent:]
                    if not batch:
                        self.wfile.write(b": ping\n\n")
                    for ev in batch:
                        self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                    sent += len(batch)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/run":
            ok = run_job("run", tracker.main)
        elif self.path == "/simulate":
            ok = run_job("simulate", tracker.simulate_price_drop)
        else:
            return self.send_error(404)
        self.send_json({"started": ok, "job": current_job}, 200 if ok else 409)

# ----------------------------------------------------------------------------- page

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Price Drop Tracker</title>
<style>
  :root {
    --bg: #f6f7f9; --panel: #ffffff; --ink: #16181d; --muted: #6b7280; --line: #e5e7eb;
    --accent: #0f766e; --accent-ink: #ffffff; --warn: #b45309; --bad: #b91c1c; --good: #15803d;
    --console: #0f1115; --console-ink: #d6dae1; --chip: #eef2f1;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0e1013; --panel: #16191e; --ink: #e8eaee; --muted: #9aa1ad; --line: #262a31;
      --accent: #2dd4bf; --accent-ink: #062320; --warn: #f59e0b; --bad: #f87171; --good: #4ade80;
      --console: #0a0b0d; --console-ink: #cfd4dc; --chip: #1d2a28;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
         font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, sans-serif; }
  .wrap { max-width: 1180px; margin: 0 auto; padding: 24px 16px 48px; }
  header { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; justify-content: space-between; }
  h1 { font-size: 20px; margin: 0; letter-spacing: -0.01em; }
  .sub { color: var(--muted); font-size: 13px; }
  .btns { display: flex; gap: 8px; flex-wrap: wrap; }
  button { font: inherit; font-weight: 600; border-radius: 8px; padding: 9px 14px; cursor: pointer;
           border: 1px solid var(--line); background: var(--panel); color: var(--ink); }
  button.primary { background: var(--accent); color: var(--accent-ink); border-color: transparent; }
  button:disabled { opacity: .5; cursor: not-allowed; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 16px; }
  .steps { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin: 20px 0; }
  .step { border: 1px solid var(--line); background: var(--panel); border-radius: 10px; padding: 12px;
          display: flex; gap: 10px; align-items: center; transition: all .2s; }
  .step .dot { width: 26px; height: 26px; border-radius: 50%; display: grid; place-items: center;
               font-size: 12px; font-weight: 700; background: var(--chip); color: var(--muted); flex: none; }
  .step b { display: block; font-size: 13px; } .step span { color: var(--muted); font-size: 12px; }
  .step.active { border-color: var(--accent); box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 18%, transparent); }
  .step.active .dot { background: var(--accent); color: var(--accent-ink); animation: pulse 1.2s infinite; }
  .step.done .dot { background: var(--good); color: #fff; }
  @keyframes pulse { 50% { transform: scale(1.12); } }
  .grid { display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr); gap: 16px; }
  h2 { font-size: 14px; margin: 0 0 10px; display: flex; justify-content: space-between; align-items: center; }
  h2 small { color: var(--muted); font-weight: 400; }
  .console { background: var(--console); color: var(--console-ink); border-radius: 10px; padding: 12px;
             font: 12px/1.55 ui-monospace, SFMono-Regular, Menlo, monospace; height: 460px; overflow: auto;
             white-space: pre-wrap; word-break: break-word; }
  .console .ok { color: #4ade80; } .console .bad { color: #f87171; } .console .alert { color: #fbbf24; font-weight: 700; }
  .console .head { color: #7dd3fc; font-weight: 700; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; font-size: 12px; }
  td.num { font-variant-numeric: tabular-nums; white-space: nowrap; }
  a { color: var(--accent); text-decoration: none; }
  .empty { color: var(--muted); padding: 18px 0; text-align: center; }
  .pill { display: inline-block; font-size: 11px; padding: 1px 8px; border-radius: 999px; background: var(--chip); color: var(--muted); }
  .down { color: var(--good); font-weight: 600; }
  .alerts { display: grid; gap: 8px; margin-top: 16px; }
  .alertcard { border: 1px solid color-mix(in srgb, var(--warn) 45%, var(--line)); border-left: 4px solid var(--warn);
               border-radius: 10px; padding: 10px 12px; background: var(--panel); }
  .alertcard b { font-size: 14px; } .alertcard .sub { margin-top: 2px; }
  .meta { display: flex; gap: 16px; flex-wrap: wrap; margin-top: 12px; color: var(--muted); font-size: 12px; }
  .status { font-size: 13px; color: var(--muted); }
  .status.running { color: var(--accent); font-weight: 600; }
  @media (max-width: 820px) {
    .grid { grid-template-columns: 1fr; }
    .steps { grid-template-columns: repeat(2, 1fr); }
    .console { height: 340px; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>Price Drop Tracker</h1>
      <div class="sub" id="retailers">Loading…</div>
    </div>
    <div class="btns">
      <span class="status" id="status">Idle</span>
      <button id="simBtn" title="Fake price drop – emails a mock claim to you, not the retailer">Simulate price drop</button>
      <button id="runBtn" class="primary">Run tracker</button>
    </div>
  </header>

  <div class="steps" id="steps">
    <div class="step" data-s="scan"><div class="dot">1</div><div><b>Scan orders</b><span>Read order history</span></div></div>
    <div class="step" data-s="check"><div class="dot">2</div><div><b>Check prices</b><span>Current price per item</span></div></div>
    <div class="step" data-s="compare"><div class="dot">3</div><div><b>Compare</b><span>Paid vs. now</span></div></div>
    <div class="step" data-s="notify"><div class="dot">4</div><div><b>Notify</b><span>Email the claim</span></div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <h2>Tracked items <small id="count"></small></h2>
      <table>
        <thead><tr><th>Item</th><th>Ordered</th><th>Paid</th><th>Now</th><th>Window</th></tr></thead>
        <tbody id="items"></tbody>
      </table>
      <div class="alerts" id="alerts"></div>
      <div class="meta" id="meta"></div>
    </div>
    <div class="panel">
      <h2>Live activity <small><a href="#" id="clear">clear</a></small></h2>
      <div class="console" id="console"></div>
    </div>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const order = ["scan", "check", "compare", "notify"];
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const gbp = n => n == null ? "–" : "£" + Number(n).toFixed(2);

function setStage(s) {
  const idx = order.indexOf(s);
  document.querySelectorAll(".step").forEach((el, i) => {
    el.classList.toggle("active", i === idx);
    el.classList.toggle("done", s === "done" ? true : idx > -1 && i < idx);
  });
  if (s === "idle" || s === "error") document.querySelectorAll(".step").forEach(el => el.classList.remove("active", "done"));
}

function addLine(line) {
  const c = $("#console");
  const atBottom = c.scrollHeight - c.scrollTop - c.clientHeight < 40;
  const div = document.createElement("div");
  const body = line.replace(/^\[[\d:]+\]\s?/, "");
  if (/✓/.test(line)) div.className = "ok";
  else if (/✗|Traceback|Error|failed|rejected/i.test(line)) div.className = "bad";
  else if (/ALERT|SIMULATION/.test(line)) div.className = "alert";
  else if (!/^\s/.test(body)) div.className = "head";
  div.textContent = line;
  c.appendChild(div);
  if (atBottom) c.scrollTop = c.scrollHeight;
}

function addAlert(a) {
  const el = document.createElement("div");
  el.className = "alertcard";
  el.innerHTML = `<b>${esc(a.name)}</b> ${a.simulated ? '<span class="pill">simulated</span>' : ""}
    <div class="sub">${esc(a.retailer)} · paid ${gbp(a.paid)} → now ${gbp(a.now)} ·
    <span class="down">${gbp(a.drop)} back</span></div>
    <div class="sub">${a.simulated ? "Mock claim emailed to " + esc(a.to)
      : a.sent ? "Claim sent to " + esc(a.to) : "Claim drafted – review it in your inbox, then send to " + esc(a.to)}</div>`;
  $("#alerts").prepend(el);
}

function setRunning(job) {
  $("#runBtn").disabled = $("#simBtn").disabled = !!job;
  $("#status").textContent = job === "run" ? "Running tracker…" : job === "simulate" ? "Simulating…" : "Idle";
  $("#status").classList.toggle("running", !!job);
}

async function refresh() {
  const s = await (await fetch("/state")).json();
  $("#retailers").textContent = s.retailers.map(r => `${r.name} · ${r.window_days}-day window`).join("  |  ");
  $("#count").textContent = s.items.length ? `${s.items.length} total` : "";
  $("#items").innerHTML = s.items.length ? s.items.map(it => {
    const now = it.last ? it.last.price : null;
    const diff = now != null ? it.price_paid - now : null;
    const win = it.days_left == null ? "–" : it.days_left < 0 ? '<span class="pill">expired</span>' : `${it.days_left}d left`;
    return `<tr>
      <td><a href="${esc(it.product_url)}" target="_blank" rel="noopener">${esc(it.name)}</a>
          <div class="sub">${esc(it.retailer)} · ${esc(it.order_id)}</div></td>
      <td class="num">${esc(it.order_date)}</td>
      <td class="num">${gbp(it.price_paid)}</td>
      <td class="num">${gbp(now)}${diff > 0 ? `<div class="down">−${gbp(diff)}</div>` : ""}</td>
      <td class="num">${win}</td></tr>`;
  }).join("") : `<tr><td colspan="5" class="empty">No orders tracked yet. Click <b>Run tracker</b>.</td></tr>`;
  $("#meta").innerHTML = `<span>Alerts go to ${esc(s.email)}</span><span>Min drop ${gbp(s.min_drop)}</span>
    <span>Auto-send claims: ${s.auto_send ? "on" : "off"}</span>`;
  setRunning(s.job);
}

async function start(path) {
  setStage("idle");
  const r = await fetch(path, { method: "POST" });
  if (r.status === 409) addLine("[--:--:--] Something is already running – wait for it to finish.");
}

$("#runBtn").onclick = () => start("/run");
$("#simBtn").onclick = () => start("/simulate");
$("#clear").onclick = e => { e.preventDefault(); $("#console").innerHTML = ""; };

const es = new EventSource("/events");
es.onmessage = m => {
  const ev = JSON.parse(m.data);
  if (ev.type === "log") addLine(ev.line);
  else if (ev.type === "stage") { setStage(ev.stage); if (ev.stage === "done") refresh(); }
  else if (ev.type === "alert") addAlert(ev);
  else if (ev.type === "job") { setRunning(ev.running ? ev.job : null); if (!ev.running) refresh(); }
};

refresh();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    url = f"http://{HOST}:{PORT}"
    print(f"Dashboard running at {url}  (Ctrl+C to stop)")
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
