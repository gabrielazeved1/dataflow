
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from api.config import settings
#ler a instancia de config que busca a str do banco de .env

engine = create_engine(settings.postgres_dsn)

SessionLocal = sessionmaker(autocommit=False, autoflush=False,bind=engine) 

class Base(DeclarativeBase):
    pass

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        