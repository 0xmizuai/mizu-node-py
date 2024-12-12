from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint, Index
from sqlalchemy.sql import func
from mizu_node.db.orm.base import Base


class Dataset(Base):
    __tablename__ = "dataset"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    language = Column(String(10), nullable=False)
    data_type = Column(String(50), nullable=False)
    total_objects = Column(Integer, default=0)
    total_bytes = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint(language, data_type, name, name="unique_constraint_name"),
        Index("idx_dataset_name", name),
        Index("idx_dataset_language", language),
        Index("idx_dataset_data_type", data_type),
        Index("idx_dataset_name_language_data_type", name, language, data_type),
        Index("idx_dataset_created_at", created_at),
    )

    def __repr__(self):
        return f"<Dataset(id={self.id}, name='{self.name}', language='{self.language}', data_type='{self.data_type}')>"
