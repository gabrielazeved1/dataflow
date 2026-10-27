from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from processing.config import settings

engine = create_engine(settings.postgres_dsn)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
