from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_dsn: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str = "dataflow"
    redis_url: str
    execution_timeout_minutes: int = 30
    # reconciler (ver v5e.md, D3)
    reconcile_after_minutes: int = 10  # Bronze sem Silver há mais que isso
    reconcile_max_age_hours: int = 24  # não ressuscita Bronze muito antiga
    reconcile_max_attempts: int = 3  # tentativas de Silver por Bronze


settings = Settings()
