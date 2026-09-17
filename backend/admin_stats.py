"""
Admin stats endpoint for dispatch-cad.

Setup:
  1. pip install psutil
  2. In your main app.py:

       from admin_stats import admin_bp
       app.register_blueprint(admin_bp)

  3. Change ADMIN_KEY below to a long random string (not the placeholder).
  4. Visit /admin/stats?key=YOUR_KEY to confirm it returns JSON.
  5. Visit /admin/logs?key=YOUR_KEY to see the scrollable event log.

Note on gunicorn workers: your gunicorn is running with a single worker
process (gevent), so counters here reflect that one process. If you
ever scale to multiple workers, request counts and logs will only
reflect whichever worker answered, not a true combined total — that
would need a shared store (SQLite/Redis) instead of the in-memory
structures below.

# new
print() capture: on import, this module patches the builtin print()
function so every print() statement anywhere in the app (your
[fire-scanner]/[fire] lines, etc.) is mirrored into the same log
buffer that /admin/logs serves — in addition to still printing to
the real terminal/journalctl as before. No changes needed to your
existing print() calls. Note: this only catches print() itself, not
raw sys.stdout.write() calls or unhandled tracebacks (those still go
to journalctl only).
"""

# new
import time
import shutil
import threading
import psutil
from collections import defaultdict, deque
from flask import Blueprint, jsonify, request

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ADMIN_KEY = "DizzStudios!"

_start_time = time.time()
_request_counts = defaultdict(int)
_history = deque(maxlen=180)  # ~15 min of history at 5s polling

LOG_RETENTION_SEC = 8 * 3600   # keep 8 hours of scrollback
_log = deque(maxlen=20000)     # hard cap as a memory safety valve
_log_lock = threading.Lock()


def _prune_log():
    cutoff = time.time() - LOG_RETENTION_SEC
    while _log and _log[0]["timestamp"] < cutoff:
        _log.popleft()


def log_event(msg):
    """Call this from anywhere in the app (e.g. fire.py) to add a line
    to the scrollable admin log. Example:

        from admin_stats import admin_bp, log_event
        log_event("stream reconnected")

    You usually don't need to call this directly for print() output —
    stdout/stderr are captured automatically (see _TeeStream below).
    """
    with _log_lock:
        _log.append({"timestamp": time.time(), "msg": msg})
        _prune_log()


# new
import builtins

_original_print = builtins.print


def _tee_print(*args, **kwargs):
    """Patches the print() builtin directly so every print() call in
    the app (fire.py's [fire-scanner]/[fire] lines, etc.) is mirrored
    into the admin log. This is more reliable than wrapping sys.stdout
    under gevent, since gevent's own monkey-patching can interfere
    with stdout redirection depending on timing."""
    _original_print(*args, **kwargs)
    sep = kwargs.get("sep", " ")
    msg = sep.join(str(a) for a in args)
    if msg.strip():
        log_event(msg)


def _capture_print():
    if builtins.print is not _tee_print:
        builtins.print = _tee_print


_capture_print()


@admin_bp.before_app_request
def _count_request():
    path = request.path
    # Only track your actual API traffic, not the admin page polling itself
    if path.startswith("/fire/"):
        _request_counts[path] += 1
        log_event(f"{request.method} {path}")


def _check_auth():
    key = request.args.get("key") or request.headers.get("X-Admin-Key")
    return key == ADMIN_KEY


@admin_bp.route("/stats")
def stats():
    if not _check_auth():
        return jsonify({"error": "unauthorized"}), 401

    cpu = psutil.cpu_percent(interval=0.3)
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = shutil.disk_usage("/")
    load1, load5, load15 = psutil.getloadavg()

    snapshot = {
        "timestamp": time.time(),
        "cpu_percent": cpu,
        "cpu_cores": psutil.cpu_count(),
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
        "requests_by_endpoint": dict(_request_counts),
    }
    _history.append(snapshot)

    return jsonify({
        "current": snapshot,
        "history": list(_history),
    })


@admin_bp.route("/logs")
def logs():
    if not _check_auth():
        return jsonify({"error": "unauthorized"}), 401
    with _log_lock:
        _prune_log()
        since = request.args.get("since", type=float, default=0)
        entries = [e for e in _log if e["timestamp"] > since]
        total = len(_log)
    return jsonify({"entries": entries, "total_buffered": total})
