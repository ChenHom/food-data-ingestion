-- food-data-ingestion v1 後續調整
-- 目標：為 raw_documents 預留 parser_version，支援未來 parser 改版後的重跑與追蹤

ALTER TABLE ingestion.raw_documents
    ADD COLUMN IF NOT EXISTS parser_version TEXT;
