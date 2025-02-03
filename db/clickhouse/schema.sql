-- Create database if not exists
DROP DATABASE IF EXISTS umami;
CREATE DATABASE IF NOT EXISTS umami;


-- User table
CREATE TABLE IF NOT EXISTS umami.user (
    user_id UUID,
    username String,
    password String,
    role String,
    logo_url Nullable(String),
    display_name Nullable(String),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    deleted_at Nullable(DateTime('UTC')),
    PRIMARY KEY (user_id)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
-- Team table
CREATE TABLE IF NOT EXISTS umami.team (
    team_id UUID,
    name String,
    access_code Nullable(String),
    logo_url Nullable(String),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    deleted_at Nullable(DateTime('UTC')),
    PRIMARY KEY (team_id)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY team_id;
-- Team User table
CREATE TABLE IF NOT EXISTS umami.team_user (
    team_id UUID,
    user_id UUID,
    team_user_id UUID,
    role String,
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    PRIMARY KEY (team_id, user_id) -- Changed to match ORDER BY
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (team_id, user_id);
-- Website table
CREATE TABLE IF NOT EXISTS umami.website (
    website_id UUID,
    name String,
    domain Nullable(String),
    share_id Nullable(String),
    reset_at Nullable(DateTime('UTC')),
    user_id Nullable(UUID),
    team_id Nullable(UUID),
    created_by Nullable(UUID),
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    deleted_at Nullable(DateTime('UTC')),
    PRIMARY KEY (website_id)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY website_id;


-- Report table

CREATE TABLE IF NOT EXISTS umami.report (
    report_id UUID,
    user_id UUID,
    website_id UUID,
    type String,
    name String,
    description String,
    parameters String,
    created_at DateTime('UTC') DEFAULT now(),
    updated_at DateTime('UTC') DEFAULT now(),
    PRIMARY KEY (report_id)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (report_id);


-- Create Event
CREATE TABLE IF NOT EXISTS umami.website_event (
    website_id UUID,
    session_id UUID,
    visit_id UUID,
    event_id UUID,
    --sessions
    hostname LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    device LowCardinality(String),
    screen LowCardinality(String),
    language LowCardinality(String),
    country LowCardinality(String),
    subdivision1 LowCardinality(String),
    subdivision2 LowCardinality(String),
    city String,
    --pageviews
    url_path String,
    url_query String,
    referrer_path String,
    referrer_query String,
    referrer_domain String,
    page_title String,
    --events
    event_type UInt32,
    event_name String,
    tag String,
    created_at DateTime('UTC'),
    job_id Nullable(UUID)
) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (
        toStartOfHour(created_at),
        website_id,
        session_id,
        visit_id,
        created_at
    ) PRIMARY KEY (
        toStartOfHour(created_at),
        website_id,
        session_id,
        visit_id
    ) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS umami.event_data (
    website_id UUID,
    session_id UUID,
    event_id UUID,
    url_path String,
    event_name String,
    data_key String,
    string_value Nullable(String),
    number_value Nullable(Decimal64(4)),
    date_value Nullable(DateTime('UTC')),
    data_type UInt32,
    created_at DateTime('UTC'),
    job_id Nullable(UUID)
) ENGINE = MergeTree
ORDER BY (website_id, event_id, data_key, created_at) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS umami.session_data (
    website_id UUID,
    session_id UUID,
    data_key String,
    string_value Nullable(String),
    number_value Nullable(Decimal64(4)),
    date_value Nullable(DateTime('UTC')),
    data_type UInt32,
    created_at DateTime('UTC'),
    job_id Nullable(UUID)
) ENGINE = ReplacingMergeTree
ORDER BY (website_id, session_id, data_key) SETTINGS index_granularity = 8192;
-- stats hourly
CREATE TABLE IF NOT EXISTS umami.website_event_stats_hourly (
    website_id UUID,
    session_id UUID,
    visit_id UUID,
    hostname LowCardinality(String),
    browser LowCardinality(String),
    os LowCardinality(String),
    device LowCardinality(String),
    screen LowCardinality(String),
    language LowCardinality(String),
    country LowCardinality(String),
    subdivision1 LowCardinality(String),
    city String,
    entry_url AggregateFunction(argMin, String, DateTime('UTC')),
    exit_url AggregateFunction(argMax, String, DateTime('UTC')),
    url_path SimpleAggregateFunction(groupArrayArray, Array(String)),
    url_query SimpleAggregateFunction(groupArrayArray, Array(String)),
    referrer_domain SimpleAggregateFunction(groupArrayArray, Array(String)),
    page_title SimpleAggregateFunction(groupArrayArray, Array(String)),
    event_type UInt32,
    event_name SimpleAggregateFunction(groupArrayArray, Array(String)),
    views SimpleAggregateFunction(sum, UInt64),
    min_time SimpleAggregateFunction(min, DateTime('UTC')),
    max_time SimpleAggregateFunction(max, DateTime('UTC')),
    tag SimpleAggregateFunction(groupArrayArray, Array(String)),
    created_at Datetime('UTC')
) ENGINE = AggregatingMergeTree PARTITION BY toYYYYMM(created_at)
ORDER BY (
        website_id,
        event_type,
        toStartOfHour(created_at),
        cityHash64(visit_id),
        visit_id
    ) SAMPLE BY cityHash64(visit_id);
CREATE MATERIALIZED VIEW IF NOT EXISTS umami.website_event_stats_hourly_mv TO umami.website_event_stats_hourly AS
SELECT website_id,
    session_id,
    visit_id,
    hostname,
    browser,
    os,
    device,
    screen,
    language,
    country,
    subdivision1,
    city,
    entry_url,
    exit_url,
    url_paths as url_path,
    url_query,
    referrer_domain,
    page_title,
    event_type,
    event_name,
    views,
    min_time,
    max_time,
    tag,
    timestamp as created_at
FROM (
        SELECT website_id,
            session_id,
            visit_id,
            hostname,
            browser,
            os,
            device,
            screen,
            language,
            country,
            subdivision1,
            city,
            argMinState(url_path, created_at) entry_url,
            argMaxState(url_path, created_at) exit_url,
            arrayFilter(x->x != '', groupArray(url_path)) as url_paths,
            arrayFilter(x->x != '', groupArray(url_query)) url_query,
            arrayFilter(x->x != '', groupArray(referrer_domain)) referrer_domain,
            arrayFilter(x->x != '', groupArray(page_title)) page_title,
            event_type,
            if(event_type = 2, groupArray(event_name), []) event_name,
            sumIf(1, event_type = 1) views,
            min(created_at) min_time,
            max(created_at) max_time,
            arrayFilter(x->x != '', groupArray(tag)) tag,
            toStartOfHour(created_at) timestamp
        FROM umami.website_event
        GROUP BY website_id,
            session_id,
            visit_id,
            hostname,
            browser,
            os,
            device,
            screen,
            language,
            country,
            subdivision1,
            city,
            event_type,
            timestamp
    );


-- Active Users View

DROP VIEW IF EXISTS umami.active_users_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS umami.active_users_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, session_id) 
AS SELECT 
    website_id,
    session_id,
    count() as visit_count,
    min(created_at) as first_seen,
    max(created_at) as last_seen,
    max(created_at) as max_created_at  -- Added for partitioning
FROM umami.website_event
GROUP BY 
    website_id,
    session_id;


-- Page Views Stats View

DROP VIEW IF EXISTS umami.pageviews_stats_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS umami.pageviews_stats_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, url_path) 
AS SELECT 
    website_id,
    url_path,
    count() as views,
    uniqExact(session_id) as visitors,
    any(page_title) as page_title,
    min(created_at) as first_seen,
    max(created_at) as last_seen,
    max(created_at) as max_created_at
FROM umami.website_event
WHERE event_type = 1
GROUP BY website_id, url_path;


-- Referrer Stats View
DROP VIEW IF EXISTS umami.referrer_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS umami.referrer_stats_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, referrer_domain) AS
SELECT website_id,
    referrer_domain,
    count() as visits,
    uniqExact(session_id) as visitors,
    min(created_at) as first_seen,
    max(created_at) as last_seen,
    max(created_at) as max_created_at
FROM umami.website_event
WHERE referrer_domain != ''
GROUP BY website_id, referrer_domain;

-- Browser Stats View
DROP VIEW IF EXISTS umami.browser_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS umami.browser_stats_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, browser) AS
SELECT website_id,
    browser,
    count() as visits,
    uniqExact(session_id) as visitors,
    max(created_at) as max_created_at
FROM umami.website_event
GROUP BY website_id, browser;

-- OS Stats View
DROP VIEW IF EXISTS umami.os_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS umami.os_stats_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, os) AS
SELECT website_id,
    os,
    count() as visits,
    uniqExact(session_id) as visitors,
    max(created_at) as max_created_at
FROM umami.website_event
GROUP BY website_id, os;

-- Device Stats View
DROP VIEW IF EXISTS umami.device_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS umami.device_stats_mv 
ENGINE = AggregatingMergeTree 
PARTITION BY toYYYYMM(max_created_at)
ORDER BY (website_id, device) AS
SELECT website_id,
    device,
    count() as visits,
    uniqExact(session_id) as visitors,
    max(created_at) as max_created_at
FROM umami.website_event
GROUP BY website_id, device;


-- projections
ALTER TABLE umami.website_event
ADD PROJECTION website_event_url_path_projection (
        SELECT *
        ORDER BY toStartOfDay(created_at),
            website_id,
            url_path,
            created_at
    );
ALTER TABLE umami.website_event MATERIALIZE PROJECTION website_event_url_path_projection;
ALTER TABLE umami.website_event
ADD PROJECTION website_event_referrer_domain_projection (
        SELECT *
        ORDER BY toStartOfDay(created_at),
            website_id,
            referrer_domain,
            created_at
    );
ALTER TABLE umami.website_event MATERIALIZE PROJECTION website_event_referrer_domain_projection;
-- Create default indexes
ALTER TABLE umami.website_event
ADD INDEX website_event_browser_index browser TYPE minmax GRANULARITY 4;
ALTER TABLE umami.website_event
ADD INDEX website_event_os_index os TYPE minmax GRANULARITY 4;
ALTER TABLE umami.website_event
ADD INDEX website_event_device_index device TYPE minmax GRANULARITY 4;
ALTER TABLE umami.website_event
ADD INDEX website_event_country_index country TYPE minmax GRANULARITY 4;