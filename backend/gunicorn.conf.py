import os

workers = 1
worker_class = "sync"
timeout = 120
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

def post_fork(server, worker):
    import threading
    import main
    print(f"[gunicorn] Worker forked (PID {os.getpid()}), starting scanner...", flush=True)
    t = threading.Thread(target=main.scanner_loop, daemon=True)
    t.start()
