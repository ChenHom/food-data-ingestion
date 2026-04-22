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
- advisory lock + `source_targets.crawl_policy` 已接上 service / connector / CLI
- Phase 1–5 單元測試已接上

## 目前可執行的入口

### CLI

```bash
python -m food_data_ingestion.jobs.run_google_places_sync --place-id <PLACE_ID> [--source-target-id <ID>]
```

補充：
- 若有傳 `--source-target-id`，service 會讀取 `ingestion.source_targets.crawl_policy` 作為 target-level override
- 在進入真實 request 前，會先對 `platform + resource_type + identifier` 取得 PostgreSQL advisory lock，避免同 target 併發抓取

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

## 可重複執行的 smoke / integration 驗證

### 1. 直接跑 smoke script

```bash
python -m food_data_ingestion.smoke.google_places --place-id smoke_place_001
```

這個 smoke script 會：
- 先清掉同一個 `place_id` 既有的 smoke 資料，確保可重跑
- 用 fake Google Places client 餵固定 payload
- 真實寫入 PostgreSQL
- 連跑兩次 ingestion，驗證第一次 miss、第二次 hit
- 輸出 JSON summary（包含 `connector_call_count` 與各資料表筆數）

### 2. 跑 integration test

```bash
RUN_FOOD_DB_SMOKE=1 python -m pytest tests/test_google_places_db_smoke.py -q
```

這個測試會真的打到本機 PostgreSQL，預設情況下會 skip；只有在你明確設 `RUN_FOOD_DB_SMOKE=1` 時才執行。

## 下一步建議

1. 補真實 Google Places API 路徑驗證（在有 API key 的環境）
2. 擴 `source_targets` 調度入口，讓 job 不只靠手動 place id / source target id 觸發
3. 再擴到 article scraper / IG / Threads PoC
