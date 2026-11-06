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
    reconcile_after_minutes: int = 10  # espera antes de dar a Bronze como perdida
    reconcile_max_age_hours: int = 24  # nao ressuscita Bronze antiga
    reconcile_max_attempts: int = 3  # tentativas de Silver por Bronze


settings = Settings()
