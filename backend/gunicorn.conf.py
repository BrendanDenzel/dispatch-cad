import os

workers = 1
worker_class = "gevent"
worker_connections = 100
timeout = 120

def post_fork(server, worker):
    import threading
    import main
    print(f"[gunicorn] Worker forked (PID {os.getpid()}), starting scanner...", flush=True)
    t = threading.Thread(target=main.scanner_loop, daemon=True)
    t.start()
