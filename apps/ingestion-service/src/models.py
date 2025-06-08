import uuid
from sqlalchemy import (
  Column,
  String,
  BigInteger,
  Integer,
  Text,
  TIMESTAMP,
  text,
  Enum,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RawDocument(Base):
  __tablename__ = "raw_documents"

  doc_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
  source_uri = Column(Text, nullable=False)
  file_hash = Column(String(64), nullable=False)
  file_size = Column(BigInteger, nullable=False)
  file_type = Column(String(10), nullable=False)
  last_modified = Column(TIMESTAMP, nullable=False)
  ingest_ts = Column(TIMESTAMP, server_default=text("NOW()"), nullable=False)
  parse_status = Column(String(10), nullable=False, server_default="pending")
  retry_count = Column(Integer, nullable=False, default=0)
  error_message = Column(Text, nullable=True)

class RawChunk(Base):
  __tablename__ = "raw_chunks"

  chunk_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
  doc_id = Column(UUID(as_uuid=True), nullable=False)
  page_number = Column(Integer, nullable=True)
  start_token = Column(Integer, nullable=True)
  end_token = Column(Integer, nullable=True)
  chunk_text = Column(Text, nullable=False)
  ingest_ts = Column(TIMESTAMP, server_default=text("NOW()"), nullable=False)
