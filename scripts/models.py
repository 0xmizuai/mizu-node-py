from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class WetMetadata(BaseModel):
    id: str
    type: str = "wet"
    batch: str
    filename: str
    chunk: int
    chunk_size: int
    bytesize: int
    decompressed_bytesize: int
    created_at: datetime = datetime.now()
    md5: str


class ClientJobRecord(BaseModel):
    id: str = Field(alias="_id")
    batch: str
    job_type: str
    classifier_id: str
    job_id: str
    metadata_type: str
    finished_at: Optional[datetime] = None
    created_at: int
