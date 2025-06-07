import os
from pydantic import BaseSettings

class Settings(BaseSettings):
  AWS_ACCESS_KEY_ID: str
  AWS_SECRET_ACCESS_KEY: str
  AWS_REGION: str = "us-east-1"
  S3_BUCKET: str = "app-uploads"
  S3_QUARANTINE_BUCKET: str = "app-quarantine"

  REDIS_URL: str = "redis://localhost:6379/0"
  REDIS_QUEUE_NAME: str = "document_parse_queue"

  DATABASE_URL: str  # e.g. postgres://user:pass@host:5432/dbname

  # Tika
  TIKA_SERVER_URL: str = "http://localhost:9998"

  # OCR
  PADDLE_OCR_URL: str = "http://localhost:8000/ocr"  # GPU-accelerated microservice
  TESSERACT_CMD: str = "tesseract"  # assume installed

  # Retry/backoff
  MAX_RETRIES: int = 3

  # Prometheus
  PROMETHEUS_PORT: int = 8001  # if you run an HTTP server for metrics

  # Sentry
  SENTRY_DSN: str = None