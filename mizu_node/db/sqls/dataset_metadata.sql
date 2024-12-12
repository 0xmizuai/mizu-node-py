-- Dataset metadata table
CREATE TABLE IF NOT EXISTS dataset_metadata (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    total_objects INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(language, data_type, name)
);

-- Indexes for dataset_metadata
CREATE INDEX idx_dataset_metadata_name ON dataset_metadata(name);
CREATE INDEX idx_dataset_metadata_language ON dataset_metadata(language);
CREATE INDEX idx_dataset_metadata_name_language ON dataset_metadata(name, language);
CREATE INDEX idx_dataset_metadata_created_at ON dataset_metadata(created_at);
