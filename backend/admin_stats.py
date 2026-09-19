"""
Admin stats + event log for dispatch-cad. Used by BOTH fire.py and police.py.

Setup (in each backend file, e.g. police.py):

    from admin_stats import admin_bp
    ...
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(admin_bp)

Then add a line to that server's .env file:

    ADMIN_KEY=some-long-random-string

What this module does
  * Samples CPU / memory / swap / load every 10s in the background (8h kept).
  * Logs "events" with a kind, so the dashboard can explain spikes:
      page_load, stream_open, stats_poll, scroll_load, api_request,
      new_call, radio, error, crash, restart, log
  * Detects crashes: a heartbeat file is written every 10s. If the process
    starts and the last shutdown wasn't clean, it records a "crash" event
    with the memory/swap/load numbers from just before it went down.
  * Captures unhandled route exceptions as "error" events with context.
  * Events are saved to disk so crashes/errors survive restarts.
  * print() is patched so your existing print() lines land in the log too.

Endpoints (all need ?key=... or an X-Admin-Key header)
  /admin/stats?window=30   current numbers, history, events, problems
  /admin/logs?since=...    scrollable log lines

Note: with a single gunicorn worker, everything here reflects that one
process. Multiple workers would each keep their own counters.
"""

import atexit
import builtins
import hmac
import json
import math
import os
import shutil
import threading
import time
from collections import defaultdict, deque

import psutil
from flask import Blueprint, jsonify, request
from werkzeug.exceptions import HTTPException

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

# Set ADMIN_KEY in the server's .env. Never hardcode it in this file:
# the repo is public.
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

SAMPLE_SEC = 10                      # background sampling interval
RETENTION_SEC = 8 * 3600             # keep 8 hours of history/events
MAX_POINTS = 360                     # max chart points sent to the browser

DATA_DIR = os.environ.get(
    "ADMIN_DATA_DIR", os.path.join(os.path.expanduser("~"), ".cad-admin")
)
os.makedirs(DATA_DIR, exist_ok=True)
EVENTS_FILE = os.path.join(DATA_DIR, "events.jsonl")
STATE_FILE = os.path.join(DATA_DIR, "state.json")

# High-frequency kinds are kept in memory only (not written to disk)
NO_PERSIST = {"stats_poll", "api_request", "scroll_load"}
PROBLEM_KINDS = {"error", "crash", "restart"}

_start_time = time.time()
_request_counts = defaultdict(int)
_history = deque(maxlen=RETENTION_SEC // SAMPLE_SEC + 10)
_events = deque(maxlen=20000)
_problems = deque(maxlen=500)
_lock = threading.RLock()
_current = {"snapshot": None}

_original_print = builtins.print


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

def _classify(msg):
    m = msg.lower()
    if any(w in m for w in ("error", "failed", "traceback", "exception",
                            "timed out", "timeout", "returned nothing",
                            "not set")):
        return "error"
    if "saved" in m and "broadcast" in m:
        return "new_call"
    if any(w in m for w in ("captured", "transcrib", "trimming",
                            "download_segments done", "uploading audio",
                            "parsing")):
        return "radio"
    return "log"


def _pressure(sample):
    """List of reasons the box was 'backed up', from a sample dict."""
    if not sample:
        return []
    out = []
    if sample.get("mem_percent", 0) >= 90:
        out.append(f"memory {sample['mem_percent']:.0f}%")
    if sample.get("swap_percent", 0) >= 70:
        out.append(f"swap {sample['swap_percent']:.0f}%")
    cores = sample.get("cpu_cores") or 1
    if sample.get("load1", 0) > cores:
        out.append(f"load {sample['load1']} on {cores} core(s)")
    return out


def _why_error(msg, ctx):
    m = msg.lower()
    if "429" in m or "rate limit" in m:
        why = "Groq/API rate limit was hit"
    elif "timed out" in m or "timeout" in m:
        why = "A network call timed out (audio stream, Groq, or Supabase)"
    elif "curl failed" in m or "playlist fetch" in m or "segment fetch" in m:
        why = "Couldn't download the audio stream (Broadcast audio or network problem)"
    elif "ffmpeg" in m or "ffprobe" in m:
        why = "Audio conversion failed (bad/short audio or ffmpeg problem)"
    elif any(w in m for w in ("supabase", "postgrest", "audio upload",
                              "save error", "purge error", "stats error")):
        why = "A database/storage call to Supabase failed"
    elif "transcription" in m or "groq" in m:
        why = "The transcription (Groq) call failed"
    elif "parse error" in m:
        why = "The AI response couldn't be read as JSON"
    elif "returned nothing" in m:
        why = "The stream capture came back empty, so it retried"
    elif "not set" in m:
        why = "A required setting is missing from .env"
    elif m.startswith("unhandled"):
        why = "A request handler crashed with an unexpected exception"
    else:
        why = "No known pattern. Read the message for details"
    pressure = _pressure((ctx or {}).get("sample"))
    if pressure:
        why += (" | Server was backed up at the time ("
                + ", ".join(pressure) + "), which may have caused it")
    return why


def _context():
    """Snapshot of system state + the last few events. Caller holds _lock."""
    now = time.time()
    sample = _current["snapshot"]
    recent = [
        {"t": e["timestamp"], "kind": e["kind"], "msg": e["msg"][:160]}
        for e in list(_events)[-60:]
        if now - e["timestamp"] <= 30 and e["kind"] not in ("stats_poll", "log")
    ][-8:]
    lean = None
    if sample:
        lean = {k: sample[k] for k in (
            "cpu_percent", "mem_percent", "swap_percent", "swap_used_mb",
            "swap_total_mb", "load1", "cpu_cores")}
    return {"sample": lean, "recent": recent}


def _persist(entry):
    try:
        with open(EVENTS_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # never print here: print is patched and would recurse


def log_event(msg, kind=None, ts=None, **extra):
    """Add an event. Call from anywhere: log_event("stream reconnected")."""
    msg = str(msg)
    kind = kind or _classify(msg)
    entry = {"timestamp": ts or time.time(), "kind": kind, "msg": msg[:500]}
    entry.update(extra)
    with _lock:
        if kind == "error" and "why" not in entry:
            ctx = _context()
            entry["ctx"] = ctx
            entry["why"] = _why_error(msg, ctx)
        _events.append(entry)
        if kind in PROBLEM_KINDS:
            _problems.append(entry)
        _prune()
    if kind not in NO_PERSIST:
        _persist(entry)
    return entry


def _prune():
    cutoff = time.time() - RETENTION_SEC
    while _events and _events[0]["timestamp"] < cutoff:
        _events.popleft()
    while _problems and _problems[0]["timestamp"] < cutoff:
        _problems.popleft()


def _tee_print(*args, **kwargs):
    """Mirror every print() into the admin log (still prints normally)."""
    _original_print(*args, **kwargs)
    try:
        sep = kwargs.get("sep", " ")
        msg = sep.join(str(a) for a in args)
        if msg.strip():
            log_event(msg)
    except Exception:
        pass


if builtins.print is not _tee_print:
    builtins.print = _tee_print


# ─────────────────────────────────────────────
# Sampling, heartbeat, crash detection
# ─────────────────────────────────────────────

def _snapshot():
    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = shutil.disk_usage("/")
    load1, load5, load15 = psutil.getloadavg()
    return {
        "timestamp": time.time(),
        "cpu_percent": cpu,
        "cpu_cores": psutil.cpu_count() or 1,
        "mem_percent": mem.percent,
        "mem_used_mb": round(mem.used / 1024 / 1024),
        "mem_total_mb": round(mem.total / 1024 / 1024),
        "swap_percent": swap.percent,
        "swap_used_mb": round(swap.used / 1024 / 1024),
        "swap_total_mb": round(swap.total / 1024 / 1024),
        "disk_percent": round(disk.used / disk.total * 100, 1),
        "load1": round(load1, 2),
        "load5": round(load5, 2),
        "load15": round(load15, 2),
        "uptime_sec": round(time.time() - _start_time),
    }


def _write_state(clean):
    snap = _current["snapshot"]
    last = None
    if snap:
        last = {k: snap[k] for k in (
            "cpu_percent", "mem_percent", "swap_percent", "swap_used_mb",
            "swap_total_mb", "load1", "cpu_cores")}
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"alive_ts": time.time(), "clean": clean, "last": last}, f)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def _sampler_loop():
    psutil.cpu_percent(interval=None)  # prime; first call always returns 0
    time.sleep(1)
    while True:
        try:
            snap = _snapshot()
            with _lock:
                _current["snapshot"] = snap
                _history.append({
                    k: snap[k] for k in (
                        "timestamp", "cpu_percent", "mem_percent",
                        "swap_percent", "swap_used_mb", "load1")})
            _write_state(clean=False)
        except Exception:
            pass
        time.sleep(SAMPLE_SEC)


def _load_saved_events():
    """Reload the last 8h of events from disk and rewrite the file trimmed."""
    cutoff = time.time() - RETENTION_SEC
    kept = []
    try:
        with open(EVENTS_FILE) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("timestamp", 0) >= cutoff:
                    kept.append(e)
    except FileNotFoundError:
        return
    except Exception:
        return
    with _lock:
        for e in kept:
            _events.append(e)
            if e.get("kind") in PROBLEM_KINDS:
                _problems.append(e)
    try:
        with open(EVENTS_FILE, "w") as f:
            for e in kept:
                f.write(json.dumps(e) + "\n")
    except Exception:
        pass


def _startup():
    _load_saved_events()
    prev = None
    try:
        with open(STATE_FILE) as f:
            prev = json.load(f)
    except Exception:
        pass

    now = time.time()
    if prev is None:
        log_event("Server started for the first time on this box",
                  kind="restart", why="First run of the monitor.")
    elif prev.get("clean"):
        log_event("Server restarted", kind="restart",
                  why="Normal shutdown/restart (deploy, systemctl restart, "
                      "or reboot).")
    else:
        alive = prev.get("alive_ts", now)
        last = prev.get("last")
        gap = max(0, now - alive)
        pressure = _pressure(last)
        if pressure:
            why = ("Most likely ran out of resources: " + ", ".join(pressure)
                   + " at the last check before it went down.")
        else:
            why = ("Went down without a clean shutdown, and resources looked "
                   "normal at the last check. Could be a crash, reboot, or "
                   "power loss. Check: sudo dmesg | grep -i 'killed process' "
                   "and sudo journalctl -u <service> --since '-1 hour'")
        log_event(
            f"Server crashed or lost power (down ~{gap:.0f}s, "
            f"back at {time.strftime('%H:%M:%S')})",
            kind="crash", ts=alive, why=why,
            ctx={"sample": last, "recent": []})
    _write_state(clean=False)


def _mark_clean():
    _write_state(clean=True)


_startup()
atexit.register(_mark_clean)
threading.Thread(target=_sampler_loop, daemon=True).start()

if not ADMIN_KEY:
    _original_print("[admin] ADMIN_KEY is not set in .env: /admin endpoints "
                    "will reject every request.", flush=True)


# ─────────────────────────────────────────────
# Request tracking + error capture
# ─────────────────────────────────────────────

@admin_bp.before_app_request
def _track_request():
    if request.method == "OPTIONS":
        return
    path = request.path
    if path.startswith("/admin") or path == "/ping":
        return
    # Fire endpoints may be under /fire/...; normalise for classification
    p = path[5:] if path.startswith("/fire/") else path
    _request_counts[path] += 1

    if p == "/incidents":
        off = request.args.get("offset", 0, type=int)
        if off == 0:
            log_event("Someone loaded the page (first incidents fetch)",
                      kind="page_load")
        else:
            log_event(f"Someone scrolled and loaded older incidents "
                      f"(offset {off})", kind="scroll_load")
    elif p == "/stream":
        log_event("A live viewer connected (stream opened)",
                  kind="stream_open")
    elif p == "/stats":
        log_event("A page polled /stats", kind="stats_poll")
    else:
        log_event(f"{request.method} {path}", kind="api_request")


@admin_bp.app_errorhandler(Exception)
def _on_exception(e):
    if isinstance(e, HTTPException):
        return e  # 404s etc. are not server errors
    log_event(f"Unhandled {type(e).__name__} on {request.path}: {e}",
              kind="error")
    return jsonify({"error": "internal error"}), 500


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

def _check_auth():
    if not ADMIN_KEY:
        return False
    key = request.args.get("key") or request.headers.get("X-Admin-Key") or ""
    return hmac.compare_digest(key.encode(), ADMIN_KEY.encode())


def _downsample(hist):
    """Reduce to <= MAX_POINTS by bucketing, keeping the PEAK of each bucket
    so spikes are never averaged away. Each point gets t_start so the
    dashboard knows which time span it covers."""
    if not hist:
        return []
    step = max(1, math.ceil(len(hist) / MAX_POINTS))
    out = []
    for i in range(0, len(hist), step):
        chunk = hist[i:i + step]
        peak = dict(chunk[-1])
        for k in ("cpu_percent", "mem_percent", "swap_percent",
                  "swap_used_mb", "load1"):
            peak[k] = max(c[k] for c in chunk)
        peak["t_start"] = chunk[0]["timestamp"] - SAMPLE_SEC
        out.append(peak)
    return out


@admin_bp.route("/stats")
def stats():
    if not _check_auth():
        return jsonify({"error": "unauthorized"}), 401

    window = min(max(request.args.get("window", 30, type=int), 5), 480)
    now = time.time()
    cutoff = now - window * 60

    with _lock:
        _prune()
        snap = dict(_current["snapshot"] or _snapshot())
        snap["uptime_sec"] = round(now - _start_time)
        snap["requests_by_endpoint"] = dict(_request_counts)
        hist = [h for h in _history if h["timestamp"] >= cutoff]
        events = [
            {"t": e["timestamp"], "kind": e["kind"], "msg": e["msg"][:200]}
            for e in _events
            if e["timestamp"] >= cutoff and e["kind"] != "log"
        ][-3000:]
        problems = sorted(_problems, key=lambda e: e["timestamp"],
                          reverse=True)[:300]
        problems = [dict(p) for p in problems]

    return jsonify({
        "current": snap,
        "history": _downsample(hist),
        "events": events,
        "problems": problems,
        "server_time": now,
    })


@admin_bp.route("/logs")
def logs():
    if not _check_auth():
        return jsonify({"error": "unauthorized"}), 401
    since = request.args.get("since", type=float, default=0)
    with _lock:
        _prune()
        entries = [
            {"timestamp": e["timestamp"], "kind": e["kind"], "msg": e["msg"]}
            for e in _events
            if e["timestamp"] > since and e["kind"] != "stats_poll"
        ]
        total = len(_events)
    entries.sort(key=lambda e: e["timestamp"])
    return jsonify({"entries": entries[-500:], "total_buffered": total})
