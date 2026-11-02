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
)
