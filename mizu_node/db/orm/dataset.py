from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Text
from .base import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    language = Column(String(10), nullable=False, default="unknown")
    data_type = Column(String(50), nullable=False)
    r2_key = Column(Text, nullable=False)
    md5 = Column(String(32), nullable=False, unique=True)
    num_of_records = Column(Integer, default=0)
    decompressed_byte_size = Column(BigInteger, default=0)
    byte_size = Column(BigInteger, default=0)
    source = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    def __repr__(self):
        return f"<Dataset(name='{self.name}', language='{self.language}', data_type='{self.data_type}')>"
