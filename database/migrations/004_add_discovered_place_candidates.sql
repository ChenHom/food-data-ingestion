CREATE TABLE IF NOT EXISTS ingestion.discovered_place_candidates (
    id                  BIGSERIAL PRIMARY KEY,
    source_platform     TEXT NOT NULL,
    source_url          TEXT NOT NULL,
    source_name         TEXT NOT NULL,
    candidate_name      TEXT NOT NULL,
    address             TEXT,
    phone               TEXT,
    opening_hours       TEXT,
    confidence_score    NUMERIC(5,4) NOT NULL DEFAULT 0,
    extraction_method   TEXT,
    parser_profile      TEXT,
    article_type        TEXT,
    raw_document_id     BIGINT REFERENCES ingestion.raw_documents(id) ON DELETE SET NULL,
    source_meta         JSONB NOT NULL DEFAULT '{}'::jsonb,
    candidate_key       TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_discovered_place_candidates_platform_created
    ON ingestion.discovered_place_candidates(source_platform, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_discovered_place_candidates_raw_document
    ON ingestion.discovered_place_candidates(raw_document_id);

CREATE INDEX IF NOT EXISTS idx_ingestion_discovered_place_candidates_name
    ON ingestion.discovered_place_candidates(candidate_name);
