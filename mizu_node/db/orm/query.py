from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from mizu_node.db.orm.base import Base


class Query(Base):
    __tablename__ = "queries"

    id = Column(Integer, primary_key=True)
    query_text = Column(Text, nullable=False)
    dataset_id = Column(Integer, nullable=False)
    model = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    status = Column(String(50), default="pending")
    last_data_id_published = Column(Integer, default=0)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(datetime.UTC)
    )

    def __repr__(self):
        return f"<Query(id={self.id}, query_text='{self.query_text[:50]}...', dataset_id='{self.dataset_id}', model='{self.model}', user='{self.user}')>"
