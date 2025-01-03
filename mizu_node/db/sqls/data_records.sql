-- Datasets table
CREATE TABLE IF NOT EXISTS data_records (
    "id" SERIAL PRIMARY KEY,
    "dataset_id" INTEGER NOT NULL REFERENCES datasets(id),
    "md5" CHAR(32) NOT NULL,
    "num_of_records" INTEGER DEFAULT 0,
    "decompressed_byte_size" BIGINT DEFAULT 0,
    "byte_size" BIGINT DEFAULT 0,
    "source" TEXT DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(md5)
);

-- Indexes for datasets
CREATE INDEX idx_data_record_dataset_id ON data_records(dataset_id);
CREATE INDEX idx_data_record_md5 ON data_records(md5);
CREATE INDEX idx_data_record_created_at ON data_records(created_at);