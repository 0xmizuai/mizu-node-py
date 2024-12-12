-- Datasets table
CREATE TABLE IF NOT EXISTS data_record (
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
CREATE INDEX idx_data_record_name ON data_record(name);
CREATE INDEX idx_data_record_language ON data_record(language);
CREATE INDEX idx_data_record_name_language ON data_record(name, language);
CREATE INDEX idx_data_record_md5 ON data_record(md5);
CREATE INDEX idx_data_record_byte_size ON data_record(byte_size);
CREATE INDEX idx_data_record_created_at ON data_record(created_at);
