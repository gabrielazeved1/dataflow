from celery import Celery

from processing.config import settings

celery_app = Celery(
    "processing",
    broker=settings.redis_url,
    backend=None,
    include=["processing.tasks"],
)

celery_app.conf.update(
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    beat_schedule={
        "reap-stuck-executions": {
            "task": "processing.reap_stuck_executions",
            "schedule": 60.0,
        },
        "reconcile-bronzes": {
            "task": "processing.reconcile_bronzes",
            "schedule": 300.0,
        },
    },
)
