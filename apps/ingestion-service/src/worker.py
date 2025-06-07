from ast import Pass
from logging import exception
import os
import io
import time
import json
import uuid
import boto3
import hashlib
import requests
import redis
import sentry_sdk
import pdfplumber
import subprocess

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, update
from models import Base, RawDocument, RawChunk
from prometheus_client import Counter, Histogram, start_http_server
from config import settings

if setting.SENTRY_DSN:
  sentry_sdk.init(dsn=settings.SENTRY_DSN)
  
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

s3 = boto3.client(
  "s3",
  aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
  aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
  region_name=settings.AWS_REGION,
)

PARSE_SUCCESS = Counter(
  "documents_parsed_success_total",
  "Total number of successfully parsed documents"
)
PARSE_DURATION = Histogram(
  "documents_processing_duration_seconds",
  "Time taken to parse on document"
)

def download_from_s3(s3_uri: str) -> bytes:
  """Stream object bytes from S3 into memory"""
  parts = s3_uri.replace("s3://", "").split("/", 1)
  bucket, key = parts[0], parts[1]
  obj = s3.get_object(Bucket=bucket, Key=key)
  return obj["Body"].read()

def recompute_sha256(data: bytes) -> str:
  h = hashlib.sha256()
  h.update(data)
  return h.hexdigest()

def is_scanned_pdf(data: bytes) -> bool:
  """
  Simple check: use pdfplumber to see if any text is extractable.
  If no text on first page, assume scanned
  """
  try:
    with pdfplumber.open(io.BytesIO(data)) as pdf:
      first_page = pdf.pages[0]
      text = first_page.extract_text() or ""
      return len(text.strip()) == 0
      
  except Exception:
    return True
  
def extract_text_from_pdf(data: bytes) -> str:
  """Try pdfplumber; fallback to Tika REST"""
  text = ""
  try:
    with pdfplumber.open(io.BytesIO(data)) as pdf:
      for page in pdf.pages:
        page_text = page.extract_text() or ""
        text += page_text + "\n"
    if text.strip():
      return text
  except Exception:
    pass
  
  try:
    resp = requests.put(
      f"{settings.TIKA_SERVER_URL}/tika",
      data=data,
      headers={"Accept": "text/plain"},
      timeout=30
    )
    if resp.status_code == 200 and resp.text.strip():
      return resp.text
  except Exception:
    pass
  
  return ""

def ocr_image(data: bytes) -> str:
  """Try PaddleOCR via REST; fallback to Tesseract locally"""
  try:
    resp = requests.post(
      settings.PADDLE_OCR_URL,
      files={"file": data},
      timeout=60
    )
    if resp.status_code == 200 and resp.json().get("text"):
      return resp.json()["text"]
  except Exception:
    pass
  
  try:
    tmp_id = str(uuid.uuid4())
    tmp_img = f"/tmp/{tmp_id}.png"
    with open(tmp_img, "wb") as f:
      f.write(data)
      
    result = subprocess.run(
      [settings.TESSERACT_CMD, tmp_img, "stdout"],
      capture_output=True,
      text=True,
      timeout=30
    )
    os.remove(tmp_img)
    return result.stdout
  except Exception:
    return ""

def process_document(payload: dict):
  """
  payload: {
    "doc_id": str,
    "source_uri": str, 
    "file_type": "pdf" | "image",
    "file_hash": str
  }
  """
  start_time = time.time()
  db = SessionLocal()
  doc_id = payload["doc_id"]
  
  try:
    doc_row = db.query(RawDocument).filter(RawDocument.doc_id == doc_id).one()
    data = download_from_s3(doc_row.source_uri)
    
    if recompute_sha256(data) != doc_row.file_hash:
      # Quarantine
      quarantine_key = f"{doc_id}.pdf"
      s3.copy_object(
        Bucket=settings.S3_QUARANTINE_BUCKET,
        CopySource={"Bucket": settings.S3_BUCKET, "Key": quarantine_key},
        Key=quarantine_key
      )
      db.execute(
        update(RawDocument)
        .where(RawDocument.doc_id == doc_id)
        .values(parse_status="failed", error_message="Hash mismatch")
      )
      db.commit()
      return

    is_scanned = False
    if doc_row.file_type == "pdf":
      is_scanned = is_scanned_pdf(data)
      if is_scanned:
        doc_row.file_type = "scanned"
        db.add(doc_row)
        db.commit()

    # 5) Extract text
    extracted_text = ""
    if doc_row.file_type == "pdf":
      extracted_text = extract_text_from_pdf(data)
    else:
      extracted_text = ocr_image(data)

    # 6) Validate extraction
    if not extracted_text.strip() or len(extracted_text) < 50:
      # treat as retry or failure
      new_retry = doc_row.retry_count + 1
      if new_retry < settings.MAX_RETRIES:
        # increment retry_count, parse_status back to pending with backoff
        db.execute(
          update(RawDocument)
          .where(RawDocument.doc_id == doc_id)
          .values(retry_count=new_retry, parse_status="pending")
        )
        db.commit()
      else:
        # move to quarantine
        quarantine_key = f"{doc_id}.pdf"
        s3.copy_object(
          Bucket=settings.S3_QUARANTINE_BUCKET,
          CopySource={"Bucket": settings.S3_BUCKET, "Key": quarantine_key},
          Key=quarantine_key
        )
        db.execute(
          update(RawDocument)
          .where(RawDocument.doc_id == doc_id)
          .values(parse_status="failed", error_message="Extraction failed after retries")
        )
        db.commit()
      return

    # 7) Insert into raw_chunks (single row, no chunking)
    chunk = RawChunk(
      chunk_id=uuid.uuid4(),
      doc_id=uuid.UUID(doc_id),
      page_number=None,
      start_token=None,
      end_token=None,
      chunk_text=extracted_text
    )
    db.add(chunk)

    # 8) Mark document success
    db.execute(
      update(RawDocument)
      .where(RawDocument.doc_id == doc_id)
      .values(parse_status="success", error_message=None)
    )
    db.commit()

    # 9) Metrics
    file_type = "scanned" if is_scanned else "pdf"
    PARSE_SUCCESS.labels(file_type=file_type).inc()
    PARSE_DURATION.observe(time.time() - start_time)

  except Exception as e:
    # report to Sentry
    if settings.SENTRY_DSN:
      sentry_sdk.capture_exception(e)
    # mark as failed
    db.execute(
      update(RawDocument)
      .where(RawDocument.doc_id == doc_id)
      .values(parse_status="failed", error_message=str(e))
    )
    db.commit()
  finally:
    db.close()

if __name__ == "__main__":
  # optional: start Prometheus HTTP server
  start_http_server(settings.PROMETHEUS_PORT)

  while True:
    try:
      # BRPOP blocks until an element is available
      result = r.brpop(settings.REDIS_QUEUE_NAME, timeout=5)
      if result:
        _, raw_payload = result
        payload = json.loads(raw_payload)
        process_document(payload)
      else:
        time.sleep(1)
    except Exception:
      time.sleep(1)