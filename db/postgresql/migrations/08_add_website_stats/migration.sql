-- Create enum type for aggregation periods
DO $$ BEGIN
    CREATE TYPE aggregation_period AS ENUM ('daily', 'weekly', 'monthly');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create website_stats table
CREATE TABLE IF NOT EXISTS website_stats (
    stats_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    website_id UUID NOT NULL REFERENCES website(website_id),
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    period aggregation_period NOT NULL,
    pageviews INTEGER NOT NULL DEFAULT 0,
    sessions INTEGER NOT NULL DEFAULT 0,
    bounces INTEGER NOT NULL DEFAULT 0,
    total_duration INTEGER NOT NULL DEFAULT 0,
    unique_visitors INTEGER NOT NULL DEFAULT 0,
    desktop_views INTEGER NOT NULL DEFAULT 0,
    mobile_views INTEGER NOT NULL DEFAULT 0,
    tablet_views INTEGER NOT NULL DEFAULT 0,
    referrer_data JSONB,
    url_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT website_stats_website_id_start_date_period_key UNIQUE (website_id, start_date, period)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_website_stats_website_date ON website_stats(website_id, start_date);
CREATE INDEX IF NOT EXISTS idx_website_stats_period ON website_stats(period);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_website_stats_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_website_stats_updated_at ON website_stats;
CREATE TRIGGER update_website_stats_updated_at
    BEFORE UPDATE ON website_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_website_stats_updated_at();

-- Down Migration
-- CREATE OR REPLACE FUNCTION down_migration() RETURNS void AS $$
-- BEGIN
--     DROP TRIGGER IF EXISTS update_website_stats_updated_at ON website_stats;
--     DROP FUNCTION IF EXISTS update_website_stats_updated_at();
--     DROP TABLE IF EXISTS website_stats;
--     DROP TYPE IF EXISTS aggregation_period;
-- END;
-- $$ LANGUAGE plpgsql;