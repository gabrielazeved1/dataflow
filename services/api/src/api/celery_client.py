from celery import Celery

from api.config import settings

celery_client = Celery("api_client", broker=settings.redis_url)
