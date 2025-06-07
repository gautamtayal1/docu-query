import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
  AWS_ACCESS_KEY_ID: str
  AWS_SECRET_ACCESS_KEY: str
  AWS_REGION: str = "blr1"
  AWS_ENDPOINT: str
  S3_BUCKET: str = "app-uploads"
  S3_QUARANTINE_BUCKET: str = "app-quarantine"

  REDIS_URL: str = "redis://localhost:6379/0"
  REDIS_QUEUE_NAME: str = "document_parse_queue"

  DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/docuquery"

  TIKA_SERVER_URL: str = "http://localhost:9998"
  PADDLE_OCR_URL: str = "http://localhost:8000/ocr" 
  TESSERACT_CMD: str = "tesseract" 
  MAX_RETRIES: int = 3
  PROMETHEUS_PORT: int = 8001  
  SENTRY_DSN: str = None

  model_config = {
    "env_file": ".env",
    "env_file_encoding": "utf-8",
    "case_sensitive": True
  }

settings = Settings()