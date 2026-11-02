from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_dsn: str
    minio_endpoint: str = ""
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "dataflow"
    max_file_size_mb: int = 100
    execution_timeout_minutes: int = 30
    redis_url: str = ""


settings = Settings()
