-- food-data-ingestion v1 follow-up
-- 目標：補強 raw_documents 與 contents 的資料品質保護

ALTER TABLE ingestion.raw_documents
    ADD COLUMN IF NOT EXISTS parsed_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_contents_platform_external
    ON ingestion.contents(platform, external_content_id)
    WHERE external_content_id IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_raw_has_content'
          AND conrelid = 'ingestion.raw_documents'::regclass
    ) THEN
        ALTER TABLE ingestion.raw_documents
            ADD CONSTRAINT chk_raw_has_content
            CHECK (
                raw_html IS NOT NULL
                OR raw_text IS NOT NULL
                OR raw_json IS NOT NULL
            );
    END IF;
END $$;
