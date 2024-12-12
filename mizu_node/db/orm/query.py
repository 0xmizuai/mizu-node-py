from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from .base import Base


class Query(Base):
    __tablename__ = "queries"

    id = Column(Integer, primary_key=True)
    query_text = Column(Text, nullable=False)
    dataset = Column(String(255), nullable=False)
    language = Column(String(10), nullable=False)
    model = Column(String(255), nullable=False)
    user = Column(String(255), nullable=False)
    status = Column(String(50), default="pending")
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(datetime.UTC)
    )

    # Relationship to QueryResult
    results = relationship(
        "QueryResult", back_populates="query", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Query(id={self.id}, query_text='{self.query_text[:50]}...', dataset='{self.dataset}'), language='{self.language}', model='{self.model}', user='{self.user}')>"
