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

## 第一版執行策略（免費優先）

第一版採 **免費優先** 原則，優先把付費 API 留給「補齊店家主檔」而不是「大量找店」。

### 核心策略

1. **`place_id` 為主入口，不以 keyword search 為預設入口**
   - Google Places 主要負責補 `restaurants` 主檔
   - 預設流程只要求 `fetch_place_detail(place_id)`
   - `searchText` / `Nearby Search` 僅保留為少量 fallback

2. **先從免費來源拿候選店家，再做後續處理**
   - seed 名單 / URL / source target
   - 公開文章 / 論壇 / 美食頁面
   - 已知 Google Maps 分享連結或可回推出店家的公開頁面

3. **先查 cache，再打真實 API**
   - 同一 `place_id` 不應在短時間內重複打外部 API
   - 真實 request 一旦發生，就必須落 `api_request_cache` 與 `raw_documents`

4. **免費來源 discovery 與 paid enrichment 分離**
   - discovery 先落 `ingestion.discovered_place_candidates`
   - 後續再做 entity resolution、Places enrichment 或人工 review

資料庫：
- PostgreSQL container: postgres-db
- database name: food_ingestion

migration 檔案：
- `database/migrations/001_initial_ingestion_schema.sql`
- `database/migrations/002_add_raw_document_parser_version.sql`
- `database/migrations/003_add_content_dedup_and_raw_constraints.sql`
- `database/migrations/004_add_discovered_place_candidates.sql`

## 專案文件

- `docs/implementation-spec.md`
  - 第一版工程化實作規格：模組責任、資料契約、執行流程、驗證與實作順序
- `docs/overview.md`
  - 專案目的、範圍、來源分級與核心設計原則
- `docs/crawling-rules.md`
  - 抓取規則、來源分級、頻率控制與 cache / budget 原則
- `docs/data-model.md`
  - schema 層次、各表責任、raw / staging / structured 資料流
- `docs/architecture.md`
  - 架構圖、模組邊界、標準資料流與第一版建議實作順序

## 目前已完成

- 獨立專案資料夾與獨立 database：`food_ingestion`
- PostgreSQL adapter / `PsycopgSession`
- `api_request_cache` / `crawl_jobs` / `raw_documents` / `restaurants` / `restaurant_external_refs` 對應 repository
- Google Places connector + parser + ingestion service
- advisory lock + `source_targets.crawl_policy` 已接上 Google Places ingestion 路徑
- discovery staging table：`ingestion.discovered_place_candidates`
- candylife discovery flow：`job policy → parser profile → normalized candidate → unified ingestion`
- `run_google_places_sync` CLI
- `run_candylife_discovery` CLI
- pytest 與 DB smoke 測試

## 可執行入口

### Google Places sync CLI

```bash
python -m food_data_ingestion.jobs.run_google_places_sync --place-id <PLACE_ID> [--source-target-id <ID>]
```

補充：
- 若有傳 `--source-target-id`，service 會讀取 `ingestion.source_targets.crawl_policy` 作為 target-level override
- 進入真實 request 前，會先對 `platform + resource_type + identifier` 取得 PostgreSQL advisory lock，避免同 target 併發抓取

輸出欄位：
- `cache_hit`
- `job_id`
- `raw_document_id`
- `restaurant_id`

### Candylife discovery CLI

乾跑：

```bash
PYTHONPATH=src python -m food_data_ingestion.jobs.run_candylife_discovery --min-year 2025 --limit 5
```

寫入 PostgreSQL：

```bash
PYTHONPATH=src python -m food_data_ingestion.jobs.run_candylife_discovery --min-year 2025 --limit 5 --write-db
```

從 `source_targets` 入口執行：

```bash
PYTHONPATH=src python -m food_data_ingestion.jobs.run_candylife_discovery --source-target-id <ID> --write-db
```

## 環境需求

至少需要：
- 可連線的 PostgreSQL（預設 `food_ingestion`）
- `GOOGLE_PLACES_API_KEY`（若要真的打外部 API）
- Python 3.11+

## Smoke / 驗證

### 1. Google Places DB smoke script

```bash
python -m food_data_ingestion.smoke.google_places --place-id smoke_place_001
```

這個 smoke script 會：
- 清掉同一個 `place_id` 舊資料，確保可重跑
- 用 fake Google Places client 餵固定 payload
- 真實寫入 PostgreSQL
- 連跑兩次 ingestion，驗證第一次 miss、第二次 hit

### 2. DB smoke tests

```bash
RUN_FOOD_DB_SMOKE=1 python -m pytest tests/test_google_places_db_smoke.py -q
```

### 3. 全量測試

```bash
pytest -q
```

## 下一步建議

1. 把 discovery candidate staging 接到 entity resolution / place matching
2. 補 candidate → Google Places enrichment 的正式後續鏈路
3. 擴更多 source-target / crawl policy / parser profile
4. 再評估 article / IG / Threads / Maps 的後續 PoC
