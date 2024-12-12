CREATE TABLE IF NOT EXISTS job_queue (
    "id" SERIAL PRIMARY KEY,
    "job_type" INTEGER NOT NULL,
    "status" INTEGER NOT NULL DEFAULT 0,
    "ctx" JSONB NOT NULL,
    "publisher" VARCHAR(255),
    "published_at" BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    "assigned_at" BIGINT NOT NULL DEFAULT 0,
    "lease_expired_at" BIGINT NOT NULL DEFAULT 0,
    "result" JSONB,
    "finished_at" BIGINT NOT NULL DEFAULT 0,
    "worker" VARCHAR(255),
    "reference_id" INTEGER,
    "retry" INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_job_type ON job_queue (job_type);
CREATE INDEX idx_status ON job_queue (status);
CREATE INDEX idx_published_at ON job_queue (published_at);
CREATE INDEX idx_lease_expired_at ON job_queue (lease_expired_at);
CREATE INDEX idx_worker ON job_queue (worker);
CREATE INDEX idx_reference_id ON job_queue (reference_id);

-- Trigger function to update finished_at when status changes to completed/failed
CREATE OR REPLACE FUNCTION update_finished_at()
RETURNS TRIGGER AS $$
BEGIN
    -- Assuming status values: completed = 2, failed = 3
    IF (NEW.status IN (2, 3) AND OLD.status NOT IN (2, 3)) THEN
        NEW.finished_at = EXTRACT(EPOCH FROM NOW())::BIGINT;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function to reset lease-related fields when job is requeued
CREATE OR REPLACE FUNCTION reset_lease_on_requeue()
RETURNS TRIGGER AS $$
BEGIN
    -- If status changes back to pending (0)
    IF (NEW.status = 0 AND OLD.status != 0) THEN
        NEW.lease_expired_at = 0;
        NEW.assigned_at = 0;
        NEW.worker = NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
DROP TRIGGER IF EXISTS trg_update_finished_at ON job_queue;
CREATE TRIGGER trg_update_finished_at
    BEFORE UPDATE ON job_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_finished_at();

DROP TRIGGER IF EXISTS trg_reset_lease_on_requeue ON job_queue;
CREATE TRIGGER trg_reset_lease_on_requeue
    BEFORE UPDATE ON job_queue
    FOR EACH ROW
    EXECUTE FUNCTION reset_lease_on_requeue();