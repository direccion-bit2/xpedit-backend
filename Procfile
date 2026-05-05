web: uvicorn main:app --host 0.0.0.0 --port $PORT --workers 4 --proxy-headers
worker: RUN_SCHEDULER=true python worker.py
