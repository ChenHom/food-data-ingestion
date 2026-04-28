-- 005: 在 discovered_place_candidates 加上 Google Places 匹配回灌欄位
-- 目的：enrichment 流程能把每個 candidate 標成 pending/matched/no_match/ambiguous，
--       並記錄已匹配到的 place_id 與 restaurants.id，下次不重打。

ALTER TABLE ingestion.discovered_place_candidates
    ADD COLUMN IF NOT EXISTS match_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (match_status IN ('pending', 'matched', 'no_match', 'ambiguous', 'skipped', 'failed')),
    ADD COLUMN IF NOT EXISTS matched_place_id TEXT,
    ADD COLUMN IF NOT EXISTS matched_restaurant_id BIGINT
        REFERENCES ingestion.restaurants(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS last_match_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS match_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS match_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_ingestion_candidates_match_status
    ON ingestion.discovered_place_candidates(match_status, last_match_attempt_at NULLS FIRST);

CREATE INDEX IF NOT EXISTS idx_ingestion_candidates_matched_place
    ON ingestion.discovered_place_candidates(matched_place_id)
    WHERE matched_place_id IS NOT NULL;
