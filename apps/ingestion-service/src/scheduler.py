import time
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exc, select, update
from models import RawDocument
from config import settings
import redis

engine = create_engine(setting.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

r = redis.Redis.from_url(settings.REDIS_URL)
BATCH_SIZE = 50

def enqueue_pending():
  db = SessionLocal()
  try:
    stmt = select(RawDocument).where(RawDocument.parse_status == "pending").limit(BATCH_SIZE)
    pending_docs = db.execute(stmt).scalars().all()

    if not pending_docs:
      return
    
    for doc in pending_docs:
      payload = {
        "doc_id": str(doc.doc_id),
        "source_uri": doc.source_uri,
        "file_type": doc.file_type,
        "file_hash": doc.file_hash
      }
      r.rpush(settings.REDIS_QUEUE_NAME, json.dumps(payload))
      
    doc.ids = [doc.doc_id for doc in pending_docs]
    upd = (
      update(RawDocument)
      .where(RawDocument.doc_id.in_(doc_ids))
      .values(parse_status="queued")
    )
    db.execute(upd)
    db.commit()
  finally:
    db.close()

if __name__ == "__main__":
  while True:
    try:
      enqueue_pending()
    except Exception as e:
      print(f"[scheduler] Error: {e}")
    time.sleep(10)