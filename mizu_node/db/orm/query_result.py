from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from .base import Base


class QueryResult(Base):
    __tablename__ = "query_results"

    id = Column(Integer, primary_key=True)
    query_id = Column(
        Integer, ForeignKey("queries.id", ondelete="CASCADE"), nullable=False
    )
    job_id = Column(String(255), nullable=False)
    data_id = Column(Integer, nullable=False)
    result = Column(JSON)
    status = Column(String(20), default="pending", nullable=False)
    finished_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    # Relationship
    query = relationship("Query", back_populates="results")

    def __repr__(self):
        return f"<QueryResult(id={self.id}, query_id={self.query_id}, job_id={self.job_id})>"
