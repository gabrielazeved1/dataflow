from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.config import settings
from dataflow_common.models import Base

engine = create_engine(settings.postgres_dsn)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
