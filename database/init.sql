-- Database initialization for SEVIRI Pipeline
-- This script runs automatically when PostgreSQL container starts for the first time

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- File manifest table - tracks all files through the pipeline
CREATE TABLE file_manifest (
    id SERIAL PRIMARY KEY,
    file_id VARCHAR(255) UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    satellite VARCHAR(50),
    product_type VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    download_attempt INTEGER DEFAULT 0,
    processing_attempt INTEGER DEFAULT 0,
    error_message TEXT,
    skip_reason VARCHAR(255),
    
    -- Timestamps
    queued_at TIMESTAMP,
    download_started_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    processing_started_at TIMESTAMP,
    processed_at TIMESTAMP,
    
    -- Paths
    raw_file_path TEXT,
    output_file_path TEXT,
    
    -- Quality metrics
    quality_score FLOAT,
    missing_data_pct FLOAT,
    saturation_pct FLOAT,
    mean_solar_zenith FLOAT,
    
    -- Metadata
    file_size_mb FLOAT,
    download_duration_sec FLOAT,
    processing_duration_sec FLOAT,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_file_id ON file_manifest(file_id);
CREATE INDEX idx_status ON file_manifest(status);
CREATE INDEX idx_timestamp ON file_manifest(timestamp DESC);
CREATE INDEX idx_queued_at ON file_manifest(queued_at);

-- Processing metrics table - detailed timing per stage
CREATE TABLE processing_metrics (
    id SERIAL PRIMARY KEY,
    file_id VARCHAR(255) REFERENCES file_manifest(file_id) ON DELETE CASCADE,
    stage VARCHAR(50) NOT NULL,
    duration_seconds FLOAT NOT NULL,
    memory_mb FLOAT,
    cpu_percent FLOAT,
    status VARCHAR(50),
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_metrics_file_id ON processing_metrics(file_id);
CREATE INDEX idx_metrics_stage ON processing_metrics(stage);

-- System health checks table
CREATE TABLE health_checks (
    id SERIAL PRIMARY KEY,
    service VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    response_time_ms FLOAT,
    timestamp TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_health_service ON health_checks(service);
CREATE INDEX idx_health_timestamp ON health_checks(timestamp DESC);

-- API query log - tracks all EUMETSAT API queries
CREATE TABLE api_query_log (
    id SERIAL PRIMARY KEY,
    query_timestamp TIMESTAMP NOT NULL,
    files_found INTEGER,
    files_new INTEGER,
    query_duration_sec FLOAT,
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_api_query_timestamp ON api_query_log(query_timestamp DESC);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_file_manifest_updated_at
    BEFORE UPDATE ON file_manifest
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- View for quick status overview
CREATE VIEW pipeline_status AS
SELECT 
    status,
    COUNT(*) as count,
    MIN(timestamp) as oldest_file,
    MAX(timestamp) as newest_file
FROM file_manifest
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY status;

-- View for processing performance
CREATE VIEW processing_performance AS
SELECT 
    DATE_TRUNC('hour', processed_at) as hour,
    COUNT(*) as files_processed,
    AVG(processing_duration_sec) as avg_duration_sec,
    AVG(quality_score) as avg_quality_score,
    SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
FROM file_manifest
WHERE processed_at IS NOT NULL
GROUP BY hour
ORDER BY hour DESC;

-- Insert initial health check
INSERT INTO health_checks (service, status, message) 
VALUES ('database', 'healthy', 'Database initialized successfully');

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO seviri;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO seviri;
