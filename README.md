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
- 第一層 `api_request_cache` 表
- Python 專案骨架
- cache repository 基本測試

## 下一步建議

1. 實作 PostgreSQL adapter
2. 將 cache repository 接上真實 DB
3. 先接 Google Places connector
4. 補 raw parser pipeline
