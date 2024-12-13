-- Dataset metadata table
CREATE TABLE IF NOT EXISTS datasets (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255) NOT NULL,
    "language" VARCHAR(10) NOT NULL,
    "data_type" VARCHAR(50) NOT NULL,
    "total_objects" INTEGER DEFAULT 0,
    "total_bytes" BIGINT DEFAULT 0,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE("language", "data_type", "name")
);

-- Indexes for dataset_metadata
CREATE INDEX idx_dataset_name ON datasets(name);
CREATE INDEX idx_dataset_language ON datasets(language);
CREATE INDEX idx_dataset_data_type ON datasets(data_type);
CREATE INDEX idx_dataset_name_language_data_type ON datasets(name, language, data_type);
CREATE INDEX idx_dataset_created_at ON datasets(created_at);