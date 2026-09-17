"""
Admin stats endpoint for dispatch-cad.

Setup:
  1. pip install psutil
  2. In your main app.py:

       from admin_stats import admin_bp
       app.register_blueprint(admin_bp)

  3. Change ADMIN_KEY below to a long random string (not the placeholder).
  4. Visit /admin/stats?key=YOUR_KEY to confirm it returns JSON.

Note on gunicorn workers: your gunicorn is running with multiple worker
processes (you had 2 in your `ss` output). Each worker keeps its own
counters in memory, so request counts below reflect only whichever
worker happened to answer /admin/stats, not a true combined total.
For a small dispatch tool this is usually good enough to eyeball trends,
but if you want exact combined counts later, that needs a shared store
(a small SQLite file or Redis) instead of the in-memory dict below --
happy to add that if the per-worker skew becomes a problem.
"""

import time
import shutil
import psutil
from collections import defaultdict, deque
from flask import Blueprint, jsonify, request

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ADMIN_KEY = "DizzStudios!"

_start_time = time.time()
_request_counts = defaultdict(int)
_history = deque(maxlen=180)  # ~15 min of history at 5s polling


@admin_bp.before_app_request
def _count_request():
    path = request.path
    # Only track your actual API traffic, not the admin page polling itself
    if path.startswith("/fire/"):
        _request_counts[path] += 1


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
