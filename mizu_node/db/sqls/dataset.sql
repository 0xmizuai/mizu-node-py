-- Datasets table
CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL DEFAULT 'unknown',
    data_type VARCHAR(50) NOT NULL,
    md5 CHAR(32) NOT NULL,
    num_of_records INTEGER DEFAULT 0,
    decompressed_byte_size BIGINT DEFAULT 0,
    byte_size BIGINT DEFAULT 0,
    source TEXT DEFAULT '',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(md5)
);

-- Indexes for datasets
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_language ON datasets(language);
CREATE INDEX idx_datasets_name_language ON datasets(name, language);
CREATE INDEX idx_datasets_md5 ON datasets(md5);
CREATE INDEX idx_datasets_byte_size ON datasets(byte_size);
CREATE INDEX idx_datasets_created_at ON datasets(created_at);
