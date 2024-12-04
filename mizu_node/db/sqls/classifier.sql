CREATE TABLE IF NOT EXISTS classifier_config (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    embedding_model VARCHAR(255) NOT NULL,
    labels JSONB NOT NULL,
    publisher VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_classifier_config_updated_at
    BEFORE UPDATE ON classifier_config
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();