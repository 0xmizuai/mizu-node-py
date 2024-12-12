-- Dataset metadata table
CREATE TABLE IF NOT EXISTS dateset (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    total_objects INTEGER DEFAULT 0,
    total_bytes BIGINT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(language, data_type, name)
);

-- Indexes for dataset_metadata
CREATE INDEX idx_dataset_name ON dataset(name);
CREATE INDEX idx_dataset_language ON dataset(language);
CREATE INDEX idx_dataset_name_language ON dataset(name, language);
CREATE INDEX idx_dataset_created_at ON dataset(created_at);
