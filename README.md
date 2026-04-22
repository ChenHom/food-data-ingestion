# food-data-ingestion

獨立的資料蒐集專案，先聚焦於：
- 第一層 request cache
- 原始資料落地
- 結構化內容抽取
- 店家主檔與內容關聯

目前第一版目標：
1. 減少直接打真實 API / 頁面的次數
2. 為美食資料蒐集建立可擴充的 DDL
3. 後續可接 Google Places、Google Maps、Instagram、Threads、部落格 / 論壇等來源

資料庫：
- PostgreSQL container: postgres-db
- database name: food_ingestion

migration 檔案：
- database/migrations/001_initial_ingestion_schema.sql
- database/migrations/002_add_raw_document_parser_version.sql
- database/migrations/003_add_content_dedup_and_raw_constraints.sql

## 專案文件

- `docs/implementation-spec.md`
  - 第一版工程化實作規格：模組責任、資料契約、執行流程、驗證與實作順序
  - 也是第一版**首波交付邊界**的唯一準則（Phase 1–5）

- `docs/overview.md`
  - 專案目的、範圍、來源分級與核心設計原則
  - 已補充說明：`contents / linking / tags / aspects` 目前屬 schema 預留與後續 phase

- `docs/crawling-rules.md`
  - 怎樣爬資料、來源分級、cache 規則、頻率控制、可追溯性規則
  - TTL 固定值已收斂引用 `implementation-spec.md`

- `docs/data-model.md`
  - 結構化資料怎麼存、每張表的責任、raw 到 structured 的流向
  - 已補 `request_fingerprint`、`crawl_policy`、`parent_external_id`、`parsed_at`

- `docs/architecture.md`
  - 架構圖、模組分工、標準資料流、PostgreSQL / Redis 職責切分
  - 已區分首波交付與後續 phase

## 目前已完成

- 獨立專案資料夾
- 獨立 database：`food_ingestion`
- 第一版 ingestion DDL
- PostgreSQL adapter / `PsycopgSession`
- `api_request_cache` / `crawl_jobs` / `raw_documents` / `restaurants` / `restaurant_external_refs` 對應 repository
- Google Places connector + TTL policy helper
- Google Places parser + restaurant persistence
- `IngestionService` 與 `run_google_places_sync` CLI
- Phase 1–5 單元測試已接上

## 目前可執行的入口

### CLI

```bash
python -m food_data_ingestion.jobs.run_google_places_sync --place-id <PLACE_ID>
```

輸出欄位：
- `cache_hit`
- `job_id`
- `raw_document_id`
- `restaurant_id`

### 環境需求

至少需要：
- 可連線的 PostgreSQL（預設 `food_ingestion`）
- `GOOGLE_PLACES_API_KEY`（若要真的打外部 API）

## Smoke check 狀態

已完成一輪**真實 DB smoke check**：
- 使用真實 PostgreSQL container / schema
- 透過 `IngestionService` 走完整鏈路
- 第一次執行：cache miss，成功寫入 `crawl_jobs`、`raw_documents`、`restaurants`、`restaurant_external_refs`
- 第二次執行：同一 `place_id` 發生 cache hit，未再重打 client

補充：目前本機環境未配置 `GOOGLE_PLACES_API_KEY`，因此 smoke check 採用 fake client 餵固定 Places detail payload，但 repository / parser / service / DB 寫入都是真實接線驗證。

## 下一步建議

1. 補一個可重複執行的 smoke script / integration test
2. 接 advisory lock 與 target-level `crawl_policy`
3. 補真實 Google Places API 路徑驗證（在有 API key 的環境）
4. 再擴到 article scraper / IG / Threads PoC
