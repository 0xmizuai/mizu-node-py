-- Queries table (updated)
CREATE TABLE IF NOT EXISTS queries (
    "id" SERIAL PRIMARY KEY,
    "dataset_id" INTEGER NOT NULL,
    "query_text" TEXT NOT NULL,
    "model" VARCHAR(255) NOT NULL,
    "user" VARCHAR(255) NOT NULL,
    "last_data_id_published" INTEGER DEFAULT 0,
    "status" VARCHAR(20) DEFAULT 'pending' CHECK ("status" IN ('pending', 'published', 'processed')),
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Updated indexes for queries
CREATE INDEX idx_queries_user ON queries("user");
CREATE INDEX idx_queries_dataset_id ON queries(dataset_id);
CREATE INDEX idx_queries_user_dataset_id_status ON queries("user", dataset_id, status);
CREATE INDEX idx_queries_user_created_at ON queries("user", created_at);
