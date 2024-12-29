from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Enum


class QueryStatus(int, Enum):
    pending = 0
    publishing = 1
    processing = 2
    processed = 3


class Dataset(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(alias="id")
    name: str = Field(alias="name")
    data_type: str = Field(alias="data_type")
    language: str = Field(alias="language")
    total_objects: int = Field(alias="totalObjects")
    total_bytes: int = Field(alias="totalBytes")
    created_at: str = Field(alias="createdAt")
    crawled_at: str = Field(alias="crawledAt")
    source: str = Field(alias="source")
    source_link: str = Field(alias="sourceLink")


class DataRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(alias="id")
    dataset_id: int = Field(alias="datasetId")
    md5: str = Field(alias="md5")
    num_of_records: int = Field(alias="numOfRecords")
    decompressed_byte_size: int = Field(alias="decompressedByteSize")
    byte_size: int = Field(alias="byteSize")
    source: str = Field(alias="source")
    created_at: str = Field(alias="createdAt")


class DataQuery(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(alias="id")
    dataset: Dataset = Field(alias="dataset")
    model: str = Field(alias="model")
    query_text: str = Field(alias="queryText")
    last_record_published: int = Field(alias="lastRecordPublished")
    user: str = Field(alias="user")
    status: QueryStatus = Field(alias="status")
    created_at: str = Field(alias="createdAt")
