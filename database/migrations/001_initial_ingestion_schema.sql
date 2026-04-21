-- food-data-ingestion v1
-- 目標：
-- 1. 第一層 request cache，減少直接打真實 API / 頁面的次數
-- 2. 通用 ingestion schema，可先支撐美食資料蒐集，後續泛化到其他主題

CREATE SCHEMA IF NOT EXISTS ingestion;

-- ---------------------------------------------------------
-- 1. 第一層 request cache
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.api_request_cache (
    id                      BIGSERIAL PRIMARY KEY,
    cache_key               TEXT NOT NULL,
    provider                TEXT NOT NULL,
    resource_type           TEXT NOT NULL,
    cache_version           TEXT NOT NULL DEFAULT 'v1',
    request_fingerprint     TEXT,
    request_params          JSONB NOT NULL DEFAULT '{}'::jsonb,
    normalized_url          TEXT,
    status_code             INTEGER,
    response_headers        JSONB,
    response_body           JSONB,
    response_text           TEXT,
    content_hash            TEXT,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refresh_after           TIMESTAMPTZ,
    expires_at              TIMESTAMPTZ NOT NULL,
    last_accessed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hit_count               BIGINT NOT NULL DEFAULT 0,
    is_error                BOOLEAN NOT NULL DEFAULT FALSE,
    error_message           TEXT,
    source_meta             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cache_key),
    CHECK (expires_at >= fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_api_cache_provider_resource
    ON ingestion.api_request_cache(provider, resource_type);

CREATE INDEX IF NOT EXISTS idx_ingestion_api_cache_expires_at
    ON ingestion.api_request_cache(expires_at);

CREATE INDEX IF NOT EXISTS idx_ingestion_api_cache_fetched_at
    ON ingestion.api_request_cache(fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_api_cache_fingerprint
    ON ingestion.api_request_cache(request_fingerprint);

-- ---------------------------------------------------------
-- 2. Source targets / crawl jobs
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.source_targets (
    id                      BIGSERIAL PRIMARY KEY,
    platform                TEXT NOT NULL,
    target_type             TEXT NOT NULL,
    target_value            TEXT NOT NULL,
    region                  TEXT,
    language                TEXT,
    enabled                 BOOLEAN NOT NULL DEFAULT TRUE,
    priority                INTEGER NOT NULL DEFAULT 100,
    crawl_policy            JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_meta             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_source_targets_lookup
    ON ingestion.source_targets(platform, target_type, enabled, priority);

CREATE TABLE IF NOT EXISTS ingestion.crawl_jobs (
    id                      BIGSERIAL PRIMARY KEY,
    source_target_id        BIGINT REFERENCES ingestion.source_targets(id) ON DELETE SET NULL,
    platform                TEXT NOT NULL,
    job_type                TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'success', 'failed', 'partial', 'skipped', 'cancelled')),
    scheduled_at            TIMESTAMPTZ,
    started_at              TIMESTAMPTZ,
    finished_at             TIMESTAMPTZ,
    attempt_count           INTEGER NOT NULL DEFAULT 0,
    worker_name             TEXT,
    request_meta            JSONB NOT NULL DEFAULT '{}'::jsonb,
    stats                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message           TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_crawl_jobs_status
    ON ingestion.crawl_jobs(status, scheduled_at, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_crawl_jobs_source_target
    ON ingestion.crawl_jobs(source_target_id, created_at DESC);

-- ---------------------------------------------------------
-- 3. Raw layer
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.raw_documents (
    id                      BIGSERIAL PRIMARY KEY,
    crawl_job_id            BIGINT REFERENCES ingestion.crawl_jobs(id) ON DELETE SET NULL,
    source_target_id        BIGINT REFERENCES ingestion.source_targets(id) ON DELETE SET NULL,
    cache_entry_id          BIGINT REFERENCES ingestion.api_request_cache(id) ON DELETE SET NULL,
    platform                TEXT NOT NULL,
    document_type           TEXT NOT NULL,
    source_url              TEXT,
    canonical_url           TEXT,
    external_id             TEXT,
    parent_external_id      TEXT,
    http_status             INTEGER,
    observed_at             TIMESTAMPTZ,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash            TEXT,
    parse_status            TEXT NOT NULL DEFAULT 'pending'
                            CHECK (parse_status IN ('pending', 'parsed', 'failed', 'skipped')),
    raw_html                TEXT,
    raw_text                TEXT,
    raw_json                JSONB,
    response_headers        JSONB,
    source_meta             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_raw_documents_platform_external
    ON ingestion.raw_documents(platform, external_id);

CREATE INDEX IF NOT EXISTS idx_ingestion_raw_documents_fetched_at
    ON ingestion.raw_documents(fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_raw_documents_content_hash
    ON ingestion.raw_documents(content_hash);

CREATE INDEX IF NOT EXISTS idx_ingestion_raw_documents_parse_status
    ON ingestion.raw_documents(parse_status, created_at DESC);

CREATE TABLE IF NOT EXISTS ingestion.raw_assets (
    id                      BIGSERIAL PRIMARY KEY,
    raw_document_id         BIGINT NOT NULL REFERENCES ingestion.raw_documents(id) ON DELETE CASCADE,
    asset_type              TEXT NOT NULL,
    asset_url               TEXT NOT NULL,
    local_path              TEXT,
    mime_type               TEXT,
    checksum                TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_raw_assets_document
    ON ingestion.raw_assets(raw_document_id, asset_type);

-- ---------------------------------------------------------
-- 4. Structured entity layer
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.restaurants (
    id                      BIGSERIAL PRIMARY KEY,
    canonical_name          TEXT NOT NULL,
    normalized_name         TEXT NOT NULL,
    branch_name             TEXT,
    country                 TEXT,
    city                    TEXT,
    district                TEXT,
    address                 TEXT,
    latitude                NUMERIC(10,7),
    longitude               NUMERIC(10,7),
    phone                   TEXT,
    website                 TEXT,
    price_level             TEXT,
    average_rating          NUMERIC(4,2),
    rating_count            INTEGER,
    business_hours          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_closed               BOOLEAN NOT NULL DEFAULT FALSE,
    source_meta             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_restaurants_name
    ON ingestion.restaurants(normalized_name);

CREATE INDEX IF NOT EXISTS idx_ingestion_restaurants_region
    ON ingestion.restaurants(city, district);

CREATE TABLE IF NOT EXISTS ingestion.restaurant_external_refs (
    id                      BIGSERIAL PRIMARY KEY,
    restaurant_id           BIGINT NOT NULL REFERENCES ingestion.restaurants(id) ON DELETE CASCADE,
    platform                TEXT NOT NULL,
    external_id             TEXT NOT NULL,
    external_url            TEXT,
    ref_type                TEXT,
    is_primary              BOOLEAN NOT NULL DEFAULT FALSE,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform, external_id)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_restaurant_external_refs_restaurant
    ON ingestion.restaurant_external_refs(restaurant_id, platform);

CREATE TABLE IF NOT EXISTS ingestion.restaurant_aliases (
    id                      BIGSERIAL PRIMARY KEY,
    restaurant_id           BIGINT NOT NULL REFERENCES ingestion.restaurants(id) ON DELETE CASCADE,
    alias_name              TEXT NOT NULL,
    normalized_alias        TEXT NOT NULL,
    alias_type              TEXT NOT NULL DEFAULT 'other',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (restaurant_id, normalized_alias)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_restaurant_aliases_lookup
    ON ingestion.restaurant_aliases(normalized_alias);

-- ---------------------------------------------------------
-- 5. Structured content layer
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.contents (
    id                      BIGSERIAL PRIMARY KEY,
    raw_document_id         BIGINT REFERENCES ingestion.raw_documents(id) ON DELETE SET NULL,
    platform                TEXT NOT NULL,
    content_type            TEXT NOT NULL,
    external_content_id     TEXT,
    author_name             TEXT,
    author_handle           TEXT,
    author_profile_url      TEXT,
    published_at            TIMESTAMPTZ,
    title                   TEXT,
    text_content            TEXT,
    language                TEXT,
    rating_value            NUMERIC(4,2),
    like_count              INTEGER,
    comment_count           INTEGER,
    share_count             INTEGER,
    media_count             INTEGER,
    source_url              TEXT,
    canonical_url           TEXT,
    source_meta             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_contents_platform_published
    ON ingestion.contents(platform, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_contents_external_id
    ON ingestion.contents(platform, external_content_id);

CREATE TABLE IF NOT EXISTS ingestion.content_restaurant_links (
    id                      BIGSERIAL PRIMARY KEY,
    content_id              BIGINT NOT NULL REFERENCES ingestion.contents(id) ON DELETE CASCADE,
    restaurant_id           BIGINT NOT NULL REFERENCES ingestion.restaurants(id) ON DELETE CASCADE,
    match_method            TEXT NOT NULL,
    confidence_score        NUMERIC(5,4),
    mention_text            TEXT,
    is_primary              BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (content_id, restaurant_id)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_content_restaurant_links_restaurant
    ON ingestion.content_restaurant_links(restaurant_id, is_primary, confidence_score DESC);

CREATE TABLE IF NOT EXISTS ingestion.review_aspects (
    id                      BIGSERIAL PRIMARY KEY,
    content_id              BIGINT NOT NULL REFERENCES ingestion.contents(id) ON DELETE CASCADE,
    restaurant_id           BIGINT REFERENCES ingestion.restaurants(id) ON DELETE CASCADE,
    aspect_name             TEXT NOT NULL,
    sentiment               TEXT CHECK (sentiment IN ('positive', 'neutral', 'negative')),
    score                   NUMERIC(5,2),
    evidence_text           TEXT,
    extracted_by            TEXT NOT NULL DEFAULT 'rule',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_review_aspects_content
    ON ingestion.review_aspects(content_id, aspect_name);

-- ---------------------------------------------------------
-- 6. Tags / taxonomy
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.tags (
    id                      BIGSERIAL PRIMARY KEY,
    tag_name                TEXT NOT NULL,
    tag_type                TEXT NOT NULL,
    parent_tag_id           BIGINT REFERENCES ingestion.tags(id) ON DELETE SET NULL,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tag_name, tag_type)
);

CREATE TABLE IF NOT EXISTS ingestion.restaurant_tags (
    id                      BIGSERIAL PRIMARY KEY,
    restaurant_id           BIGINT NOT NULL REFERENCES ingestion.restaurants(id) ON DELETE CASCADE,
    tag_id                  BIGINT NOT NULL REFERENCES ingestion.tags(id) ON DELETE CASCADE,
    score                   NUMERIC(5,2),
    source                  TEXT NOT NULL DEFAULT 'rule',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (restaurant_id, tag_id, source)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_restaurant_tags_restaurant
    ON ingestion.restaurant_tags(restaurant_id, tag_id);

CREATE TABLE IF NOT EXISTS ingestion.content_tags (
    id                      BIGSERIAL PRIMARY KEY,
    content_id              BIGINT NOT NULL REFERENCES ingestion.contents(id) ON DELETE CASCADE,
    tag_id                  BIGINT NOT NULL REFERENCES ingestion.tags(id) ON DELETE CASCADE,
    score                   NUMERIC(5,2),
    source                  TEXT NOT NULL DEFAULT 'rule',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (content_id, tag_id, source)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_content_tags_content
    ON ingestion.content_tags(content_id, tag_id);

-- ---------------------------------------------------------
-- 7. Observability
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion.ingestion_logs (
    id                      BIGSERIAL PRIMARY KEY,
    crawl_job_id            BIGINT REFERENCES ingestion.crawl_jobs(id) ON DELETE SET NULL,
    level                   TEXT NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
    event_type              TEXT NOT NULL,
    message                 TEXT NOT NULL,
    details                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_logs_job
    ON ingestion.ingestion_logs(crawl_job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_logs_level_event
    ON ingestion.ingestion_logs(level, event_type, created_at DESC);
