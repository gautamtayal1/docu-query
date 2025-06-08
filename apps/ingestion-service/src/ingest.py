import hashlib
import uuid
import aiofiles
import asyncio
import os
from pathlib import Path
from typing import List, Dict, Any
import uvicorn
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
import boto3
from botocore.exceptions import ClientError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from models import Base, RawDocument
from config import settings

engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

s3 = boto3.client(
    "s3",
    endpoint_url=settings.AWS_ENDPOINT,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION,
)

app = FastAPI(title="Document Ingestion Service", version="1.0.0")

ALLOWED_EXTENSIONS = {".pdf"} #, ".png", ".jpg", ".jpeg", ".tiff", ".bmp"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
CHUNK_SIZE = 8192

class FileProcessingError(Exception):
  """Custom exception for file processing errors"""
  pass

@asynccontextmanager
async def get_db_session():
  """Async context manager for database sessions"""
  db = SessionLocal()
  try:
    yield db
  except SQLAlchemyError as e:
    db.rollback()
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Database error: {str(e)}"
    )
  finally:
    db.close

async def validate_file(uploaded_file: UploadFile) -> None:
  """Validate uploaded file"""
  if not uploaded_file.filename:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Filename is required"
    )

  file_ext = Path(uploaded_file.filename).suffix.lower()
  if file_ext not in ALLOWED_EXTENSIONS:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"File type {file_ext} not allowed. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}"
    )
    
  if uploaded_file.size and uploaded_file.size > MAX_FILE_SIZE:
    raise HTTPException(
      status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
      detail=f"file size exceeds maximum allowed size of {MAX_FILE_SIZE}"
    )

async def save_uploaded_file(uploaded_file: UploadFile, temp_path: str) -> int:
  """Save uploaded file to temporary location and return file size"""
  file_size = 0
  async with aiofiles.open(temp_path, "wb") as temp_file:
    while chunk := await uploaded_file.read(CHUNK_SIZE):
      await temp_file.write(chunk)
      file_size += len(chunk)
  return file_size

async def compute_sha256_async(file_path: str) -> str:
  """Compute SHA256 hash of file asynchronously"""
  def _compute_hash():
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
      for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
        sha256.update(chunk)
    return sha256.hexdigest()
  
  loop = asyncio.get_event_loop()
  return await loop.run_in_executor(None, _compute_hash)

async def upload_to_s3_async(file_path: str, s3_key: str) -> datetime:
  """Upload file to S3 and return LastModified timestamp"""
  def _upload(): 
    try:
      s3.upload_file(file_path, settings.S3_BUCKET, s3_key)
      head_response = s3.head_object(Bucket=settings.S3_BUCKET, Key=s3_key)
      return head_response["LastModified"]
    except ClientError as e:
      raise FileProcessingError(f"S3 upload failed: {str(e)}")

  loop = asyncio.get_event_loop()
  return await loop.run_in_executor(None, _upload)

def determine_file_type(filename: str) -> str:
  """Determine file type based on extension"""
  ext = Path(filename).suffix.lower()
  if ext == ".pdf":
    return "pdf"
  elif ext in {".png", ".jpg", ".jpeg", ".tiff", ".bmp"}:
    return "image"
  else:
    return "unknown"

async def process_single_file(uploaded_file: UploadFile, db: Session) -> Dict[str, Any]:
  """Process a single uploaded file"""
  await validate_file(uploaded_file)
  
  doc_id = uuid.uuid4()
  file_ext = Path(uploaded_file.filename).suffix.lower()
  temp_filename = f"/tmp/{doc_id}{file_ext}"
  s3_key = f"{doc_id}{file_ext}"
  
  try:
    file_size = await save_uploaded_file(uploaded_file, temp_filename)
    file_hash = await compute_sha256_async(temp_filename)
    last_modified = await upload_to_s3_async(temp_filename, s3_key)
    
    doc = RawDocument(
      doc_id=doc_id,
      source_uri=f"s3://{settings.S3_BUCKET}/{s3_key}",
      file_hash=file_hash,
      file_size=file_size,
      file_type=determine_file_type(uploaded_file.filename),
      last_modified=last_modified,
      parse_status="pending",
      retry_count=0,
    )
    db.add(doc)
    db.commit()
    
    return {
      "doc_id": str(doc.doc_id),
      "source_uri": doc.source_uri,
      "file_name": uploaded_file.filename,
      "file_size": file_size,
      "file_hash": file_hash,
      "status": "accepted"
    }
    
  except FileProcessingError:
    raise
  except Exception as e:
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"File processing failed: {str(e)}"
    )
  finally:
    if os.path.exists(temp_filename):
      try:
        os.remove(temp_filename)
      except OSError:
        pass

@app.post("/ingest/", status_code=status.HTTP_202_ACCEPTED)
async def ingest_files(files: list[UploadFile] = File(...)):
  """
  Ingest multiple files (PDFs/images).
  
  - Validates each file
  - Uploads to S3
  - Stores metadata in database
  - Returns processing status for each file
  """
  if not files: 
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Atleast one file must be provided"
    )
  
  if len(files) > 10:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Too many files. Maximum 10 files per request"
    )
    
  responses = []
  errors = []
  
  async with get_db_session() as db:
    for i, uploaded_file in enumerate(files):
      try:
        result = await process_single_file(uploaded_file, db)
        responses.append(result)
      except HTTPException as e:
        error_info = {
          "file_index": i, 
          "filename": uploaded_file.filename,
          "error": f"Unexpected error: {str(e)}",
          "status_code": 500
        }
        errors.append(error_info)
  
  response_data = {
    "processed_files": len(responses),
    "failed_files": len(errors),
    "files": responses
  }
  
  if errors:
    response_data["errors"] = errors
    
  if responses:
    return JSONResponse(
      status_code=status.HTTP_202_ACCEPTED,
      content=response_data
    )
  else:
    return JSONResponse(
      status_code=status.HTTP_400_BAD_REQUEST,
      content=response_data
    )
    
if __name__ == "__main__":
  uvicorn.run(
    app,
    host="0.0.0.0",
    port=8080,
    log_level="info",
    access_log=True
  )