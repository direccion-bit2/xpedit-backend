"""
Cron worker process — runs APScheduler jobs without serving HTTP.

Provisioned as a second Railway service so the web service can run with
`--workers N` without each worker spawning duplicate copies of the scheduler
(every cron job firing N times). Web sets RUN_SCHEDULER=false; this process
sets it to true via its start command and is the sole owner of the cron
loop.

Architecture:
- main.py reads RUN_SCHEDULER and conditionally starts the AsyncIOScheduler
  in @app.on_event("startup"). When the flag is false the startup hooks
  return early; the FastAPI app still functions normally for HTTP traffic.
- This worker imports the same FastAPI `app` (which triggers the module
  side-effects) and explicitly drives the startup lifecycle so that the
  scheduler hooks run as if uvicorn had started — but without binding any
  HTTP port.
- The process then idles forever; cron callbacks run on the scheduler's
  event loop.

Run locally:
    RUN_SCHEDULER=true python worker.py

Run in Railway:
    Service: xpedit-worker
    Start command: RUN_SCHEDULER=true python worker.py
    Same repo as the web service; deploys together.
"""
import asyncio
import logging
import os
import signal
import sys

# Force-on the scheduler flag here as a defence in depth — if the env var
# was forgotten in Railway, this process would otherwise idle without doing
# anything useful and silently break crons. Better to assume the operator's
# intent: a process named worker exists to run the scheduler.
os.environ.setdefault("RUN_SCHEDULER", "true")

# Importing main triggers FastAPI app construction, scheduler instantiation,
# and registers the startup hooks. We invoke them manually below.
from main import app, social_scheduler  # noqa: E402

logger = logging.getLogger("xpedit.worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def _drive_startup_hooks():
    """Mimic the FastAPI/uvicorn startup phase so the registered
    @app.on_event('startup') handlers (start_social_scheduler,
    start_monitoring_jobs) execute. Without this, importing main alone is not
    enough — those hooks only fire when uvicorn boots the server.
    """
    # FastAPI 0.10x exposes router lifecycle handlers via app.router.startup;
    # newer Starlette uses lifespan. Both are present on app.router.
    handlers = list(getattr(app.router, "on_startup", []) or [])
    for handler in handlers:
        try:
            result = handler()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception("startup handler %s raised", getattr(handler, "__name__", handler))


async def _idle_forever():
    """Block until SIGTERM. Railway sends SIGTERM on redeploy / shutdown."""
    stop_event = asyncio.Event()

    def _on_signal(*_args):
        logger.info("worker: shutdown signal received, stopping scheduler")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            # Windows / odd hosts — fall back to default handlers.
            pass

    await stop_event.wait()


async def _main():
    if os.getenv("RUN_SCHEDULER", "true").lower() == "false":
        logger.error("worker started but RUN_SCHEDULER=false — refusing to run; check Railway env vars")
        sys.exit(1)

    logger.info("worker: bootstrapping scheduler from main.app startup hooks")
    await _drive_startup_hooks()

    if not social_scheduler.running:
        logger.error("worker: scheduler did NOT start — startup hooks may have skipped due to env config")
        sys.exit(2)

    job_ids = [j.id for j in social_scheduler.get_jobs()]
    logger.info("worker: scheduler running with %d jobs: %s", len(job_ids), job_ids)

    try:
        await _idle_forever()
    finally:
        if social_scheduler.running:
            social_scheduler.shutdown(wait=False)
            logger.info("worker: scheduler stopped")


if __name__ == "__main__":
    asyncio.run(_main())
