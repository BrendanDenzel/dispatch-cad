# gunicorn.conf.py
import threading
import os

# Single worker — scanner loop uses one persistent stream connection,
# multiple workers would create duplicate scanners and split the SSE clients list.
workers = 1
worker_class = "sync"
worker_connections = 1000
timeout = 120
keepalive = 5

bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"

# Increase timeout for SSE streaming connections
graceful_timeout = 30

def post_fork(server, worker):
    """Runs in the worker process after fork. Start the scanner here
    so it lives in the actual worker, not the pre-fork master."""
    print(f"[gunicorn] Worker forked (PID {os.getpid()}), starting scanner...", flush=True)
    from main import start_scanner
    start_scanner()
