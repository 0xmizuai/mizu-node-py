from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class WetMetadata(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
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
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="_id")
    batch: str
    job_type: str
    classifier_id: str
    job_id: str
    metadata_type: str
    finished_at: Optional[datetime] = None
    created_at: int


class WetRecord(object):
    def __init__(self, record: any):
        self.languages = (
            record.rec_headers.get_header("WARC-Identified-Content-Language") or ""
        ).split(",")
        self.content_length = record.rec_headers.get_header("Content-Length")
        self.uri = record.rec_headers.get_header("WARC-Target-URI")
        self.warc_id = record.rec_headers.get_header("WARC-Record-ID")
        record_date = record.rec_headers.get_header("WARC-Date")
        self.crawled_at = int(
            round(datetime.strptime(record_date, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        )
        self.text = record.content_stream().read().decode("utf-8")
