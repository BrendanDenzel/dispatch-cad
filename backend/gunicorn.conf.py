import os

workers = 1
worker_class = "sync"
timeout = 120
bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"

def post_fork(server, worker):
    import threading
    from main import scanner_loop
    print(f"[gunicorn] Worker forked (PID {os.getpid()}), starting scanner...", flush=True)
    t = threading.Thread(target=scanner_loop, daemon=True)
    t.start()
