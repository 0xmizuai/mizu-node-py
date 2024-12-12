CREATE TABLE IF NOT EXISTS query_results (
    "id" SERIAL PRIMARY KEY,
    "query_id" INTEGER NOT NULL REFERENCES queries(id),
    "job_id" INTEGER NOT NULL,
    "result" JSONB,
    "status" VARCHAR(20) DEFAULT 'pending' CHECK ("status" IN ('pending', 'processed', 'error')),
    "finished_at" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE("job_id")
);

CREATE INDEX idx_query_results_query_id ON query_results(query_id);
CREATE INDEX idx_query_results_job_id ON query_results(job_id);
CREATE INDEX idx_query_results_status ON query_results(status);
CREATE INDEX idx_query_results_created_at ON query_results(created_at);
