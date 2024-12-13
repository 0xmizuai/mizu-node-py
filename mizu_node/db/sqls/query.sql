-- Queries table (updated)
CREATE TABLE IF NOT EXISTS queries (
    "id" SERIAL PRIMARY KEY,
    "dataset_id" INTEGER NOT NULL REFERENCES datasets(id),
    "query_text" TEXT NOT NULL,
    "model" VARCHAR(255) NOT NULL,
    "user" VARCHAR(255) NOT NULL,
    "last_record_published" INTEGER DEFAULT 0,
    "status" INTEGER DEFAULT 0 CHECK ("status" IN (0, 1, 2)),
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Updated indexes for queries
CREATE INDEX idx_queries_user ON queries("user");
CREATE INDEX idx_queries_dataset_id ON queries(dataset_id);
CREATE INDEX idx_queries_user_dataset_id_status ON queries("user", dataset_id, status);
CREATE INDEX idx_queries_user_created_at ON queries("user", created_at);