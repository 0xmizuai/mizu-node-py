CREATE TABLE IF NOT EXISTS api_key (
    id SERIAL PRIMARY KEY,
    token VARCHAR(64) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(token)
);

-- Indexes for better query performance
CREATE INDEX idx_api_key_user ON api_key(user_id);
CREATE INDEX idx_api_key_active ON api_key(is_active);
CREATE INDEX idx_api_key_created ON api_key(created_at);

-- Function to update last_used_at
CREATE OR REPLACE FUNCTION update_api_key_last_used()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE api_key 
    SET last_used_at = CURRENT_TIMESTAMP 
    WHERE token = OLD.token;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- INSERT INTO api_key (api_key, user_id, description)
-- VALUES (generate_api_key(), 'user123', 'Development API key'); 