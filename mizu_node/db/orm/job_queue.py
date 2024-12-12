from sqlalchemy import BigInteger, Column, Integer, String, JSON, Index
from mizu_node.db.orm.base import Base


class JobQueue(Base):
    __tablename__ = "job_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False, default=0)
    ctx = Column(JSON, nullable=False)
    publisher = Column(String(255))
    published_at = Column(
        BigInteger, nullable=False, server_default="EXTRACT(EPOCH FROM NOW())::BIGINT"
    )
    assigned_at = Column(BigInteger, nullable=False, default=0)
    lease_expired_at = Column(BigInteger, nullable=False, default=0)
    result = Column(JSON)
    finished_at = Column(BigInteger, nullable=False, default=0)
    worker = Column(String(255))
    retry = Column(Integer, nullable=False, default=0)

    # Recreate the indexes from the SQL schema
    __table_args__ = (
        Index("idx_job_type", "job_type"),
        Index("idx_status", "status"),
        Index("idx_published_at", "published_at"),
        Index("idx_lease_expired_at", "lease_expired_at"),
        Index("idx_worker", "worker"),
    )

    def __repr__(self):
        return (
            f"<JobQueue(id={self.id}, job_type={self.job_type}, status={self.status})>"
        )
