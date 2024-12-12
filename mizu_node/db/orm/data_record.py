from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text
from .base import Base


class DataRecord(Base):
    __tablename__ = "data_records"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, nullable=False)
    md5 = Column(String(32), nullable=False, unique=True)
    num_of_records = Column(Integer, default=0)
    decompressed_byte_size = Column(BigInteger, default=0)
    byte_size = Column(BigInteger, default=0)
    source = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    def __repr__(self):
        return f"<DataRecord(dataset_id='{self.dataset_id}', md5='{self.md5}')>"
