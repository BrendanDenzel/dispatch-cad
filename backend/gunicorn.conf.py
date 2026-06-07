# gunicorn.conf.py
import threading
import os

# Single worker — scanner loop uses one persistent stream connection,
# multiple workers would create duplicate scanners and split the SSE clients list.
workers = 1
worker_class = "sync"
timeout = 120
bind = f"0.0.0.0:{__import__('os').environ.get('PORT', '5000')}"

# Increase timeout for SSE streaming connections
graceful_timeout = 30
