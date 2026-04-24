# food-data-ingestion 實作規格 v1

> 本文件不是概念說明，而是第一版可直接落地的工程實作規格。重點是把「要做什麼、放哪裡、資料怎麼流、怎麼驗證」寫清楚，讓後續實作不需要再靠猜。

---

## 1. 文件目標

本規格定義 `food-data-ingestion` 第一版的：

1. **交付邊界**：哪些功能要做、哪些先不做。
2. **模組責任**：每個 package / file 應承擔什麼責任。
3. **資料契約**：connector、repository、parser 之間傳什麼資料。
4. **執行流程**：從 target → crawl job → cache → raw → structured 的標準流程。
5. **驗證方式**：每一層至少要有什麼測試與 smoke check。
6. **落地順序**：第一版應依什麼順序實作，避免亂接。

---

## 2. 第一版交付範圍

### 2.1 In Scope

第一版必須交付以下能力：

- PostgreSQL 連線設定與 adapter
- `ingestion.api_request_cache` 的實際 DB 接線
- 單一來源 connector（優先 `Google Places`，且以 `place_id` 驅動的 place detail 為主）
- raw landing：把外部回應寫進 `ingestion.raw_documents`
- 第一版 parser：將 places detail 轉成 `restaurants` 與 `restaurant_external_refs`
- 基本 job 記錄：建立 / 更新 `crawl_jobs`
- 可重複執行的 smoke test 與 pytest 測試

補充說明：
- `contents`、`content_restaurant_links`、`review_aspects`、`tags` 相關資料表屬於第一版 schema 預留範圍
- 但它們**不屬於首波 Phase 1–5 驗收交付內容**；首波仍以 Google Places → restaurant ingestion 鏈為主
- 第一版採 **免費優先**：先從免費來源做 discovery，再用 Google Places detail 補 restaurant 主檔

### 2.2 Out of Scope

以下不在本次第一版交付內：

- Redis 正式接線
- IG / Threads / Google Maps 大量抓取
- Places keyword / nearby search 作為預設主流程
- 前端介面
- 排程器 daemon / worker pool
- embedding、向量搜尋、推薦排序模型
- 大規模分散式爬蟲

---

## 3. 目前已存在的基礎

### 3.1 已存在檔案

- `database/migrations/001_initial_ingestion_schema.sql`
- `src/food_data_ingestion/config.py`
- `src/food_data_ingestion/models/cache.py`
- `src/food_data_ingestion/storage/cache_repository.py`
- `tests/test_settings.py`
- `tests/test_cache_repository.py`

### 3.2 已存在資料表

DDL 已建立以下主要表：

- `ingestion.api_request_cache`
- `ingestion.source_targets`
- `ingestion.crawl_jobs`
- `ingestion.raw_documents`
- `ingestion.raw_assets`
- `ingestion.restaurants`
- `ingestion.restaurant_external_refs`
- `ingestion.restaurant_aliases`
- `ingestion.contents`
- `ingestion.content_restaurant_links`
- `ingestion.review_aspects`
- `ingestion.tags`
- `ingestion.restaurant_tags`
- `ingestion.content_tags`
- `ingestion.ingestion_logs`
- `ingestion.discovered_place_candidates`

### 3.3 現況限制

目前專案已經有：

- `GooglePlacesConnector` 與對應 parser / ingestion service / smoke command
- `RawDocumentRepository` / `RestaurantRepository` / `CrawlJobRepository`
- `CandylifeArticleDiscoveryService`
- `CandylifeDiscoveryPolicy` / `CandylifeParserProfile`
- `DiscoveredArticle` / `DiscoveredPlaceCandidate`
- `UnifiedDiscoveryIngestionService`
- `DiscoveredPlaceCandidateRepository`
- 可直接跑的 `run_candylife_discovery` CLI

目前尚在收尾 / 後續主缺口：

- discovery candidate → entity resolution / Places enrichment 的後續鏈路
- 更多 source-target / crawl policy / parser profile 擴充
- `contents` / linking / tags / aspects 的後續 phase

因此這份文件後續應以「已落地的 discovery staging + Google Places ingestion 基礎 + 下一段 enrichment / matching」為主。

---

## 4. 目錄與模組責任規格

第一版程式目錄應收斂成以下結構：

```text
src/food_data_ingestion/
  __init__.py
  config.py
  db/
    __init__.py
    connection.py
    psycopg_session.py
  discovery/
    __init__.py
    models.py
    service.py
  parser_profiles/
    __init__.py
    candylife.py
  models/
    __init__.py
    cache.py
    raw_document.py
    restaurant.py
    crawl_job.py
  storage/
    __init__.py
    cache_repository.py
    raw_repository.py
    discovered_candidate_repository.py
    restaurant_repository.py
    crawl_job_repository.py
  connectors/
    __init__.py
    base.py
    google_places.py
    candylife.py
  parsers/
    __init__.py
    google_places.py
    candylife.py
    candylife_feed.py
  services/
    __init__.py
    ingestion_service.py
    article_discovery.py
  jobs/
    __init__.py
    run_google_places_sync.py
    run_candylife_discovery.py
```

### 4.1 `config.py`

責任：
- 從環境變數載入設定
- 提供 DB 與外部 API 的最小必要設定

第一版至少新增以下欄位：

- `db_host`
- `db_port`
- `db_name`
- `db_user`
- `db_password`
- `google_places_api_key`
- `default_cache_ttl_seconds`
- `default_error_cache_ttl_seconds`
- `app_env`

規格要求：
- `Settings.from_env()` 必須是唯一入口
- 不要在 connector 內直接 `os.getenv()`
- 所有 runtime 參數都要可經由 `Settings` 注入

### 4.2 `db/connection.py`

責任：
- 建立 PostgreSQL DSN
- 提供連線工廠 `create_connection(settings)`

規格要求：
- 優先用 `psycopg` v3
- 連線建立失敗時，錯誤訊息需包含 host / db_name，但不可印出密碼

### 4.3 `db/psycopg_session.py`

責任：
- 實作 repository 需要的 session 介面
- 封裝 `fetchone()` / `fetchall()` / `execute()` / `execute_returning()`

規格要求：
- 回傳資料型別統一為 `dict[str, Any]`
- repository 不直接碰 cursor 細節
- transaction scope 由 service / command 控制，不由 repository 自己隱式 commit 多次

### 4.4 `storage/cache_repository.py`

責任：
- 查詢有效 cache
- 命中後更新 hit metadata
- 寫入或覆寫 cache entry

規格要求：
- `build_cache_key()` 必須穩定且可重算
- `get_valid()` 只回傳「尚未過期」的資料
- `mark_hit()` 必須更新 `last_accessed_at` 與 `hit_count`
- `upsert()` 必須維持 `cache_key` 為唯一鍵

### 4.5 `storage/raw_repository.py`

責任：
- 把真實來源回應寫入 `ingestion.raw_documents`
- 必要時寫入 `ingestion.raw_assets`

最小介面：

```python
class RawDocumentRepository:
    def create(self, payload: RawDocumentCreate) -> int: ...
```

規格要求：
- 每次真實外部請求成功或失敗，只要有 response body / text / headers，就應盡量留下 raw
- `cache_entry_id`、`crawl_job_id`、`source_target_id` 有就帶
- `content_hash` 必須可重算
- `parser_version` 第一版允許為 `NULL`，等 parser 真正上線後開始填固定版本值
- `raw_documents` 落地時至少要有 `raw_json` / `raw_text` / `raw_html` 其中一種內容，不可寫入空殼 row

### 4.6 `storage/restaurant_repository.py`

責任：
- upsert `restaurants`
- upsert `restaurant_external_refs`
- 必要時補 `restaurant_aliases`

規格要求：
- restaurant 主表不能綁死特定平台欄位
- 外部平台 id 一律走 `restaurant_external_refs`
- parser 層先輸出 normalized model，再由 repository 決定 SQL 寫入
- upsert 前必須先查 `restaurant_external_refs`；找到對應 `restaurant_id` 才更新 `restaurants`，找不到才 insert
- 第一版不建議在 `restaurants` 主表上硬加 address/name 類 composite unique constraint，以免因地址格式差異造成誤判

### 4.7 `storage/crawl_job_repository.py`

責任：
- 建立 job
- 切換 job 狀態為 `running` / `success` / `failed`
- 記錄 attempt / stats / error

規格要求：
- status 必須只使用 DDL 允許值
- `stats` 優先記錄：`cache_hit`, `raw_document_id`, `restaurant_id_count`, `content_count`

### 4.8 `connectors/base.py`

責任：
- 定義 connector 共同介面

最小介面：

```python
class FetchResult(TypedDict):
    provider: str
    resource_type: str
    cache_key: str
    normalized_url: str | None
    request_params: dict[str, Any]
    status_code: int | None
    response_headers: dict[str, Any] | None
    response_body: dict[str, Any] | list[Any] | None
    response_text: str | None
    fetched_at: datetime
    expires_at: datetime
    refresh_after: datetime | None
    is_error: bool
    error_message: str | None
    source_meta: dict[str, Any]
```

規格要求：
- connector 只負責「怎麼拿到來源資料」
- connector 不直接寫 SQL
- connector 不直接 parse 成 restaurant DB row

### 4.9 `connectors/google_places.py`

責任：
- 封裝 Places API detail / search
- 先查 cache，miss 才打真實 API
- 回傳標準 `FetchResult`

第一版只要求先做：
- `fetch_place_detail(place_id: str)`

規格要求：
- cache key 格式：`google_places:v1:place_detail:{place_id}`
- request params 至少保存：`place_id`, `fields`, `language`
- 成功結果預設 TTL：`21600` 秒（6 小時）
- 失敗結果依 status code 套較短 TTL

### 4.10 `parsers/google_places.py`

責任：
- 把 Places detail response 轉成 restaurant normalized model

最小介面：

```python
def parse_place_detail(raw_document: dict[str, Any]) -> ParsedPlaceDetail: ...
```

輸出至少包含：
- `canonical_name`
- `normalized_name`
- `address`
- `latitude`
- `longitude`
- `average_rating`
- `rating_count`
- `business_hours`
- `external_refs[]`

規格要求：
- parser 不可直接讀環境變數
- parser 不可直接操作 DB
- parser 發生欄位缺失時，應保留可解析部分，不要整包炸掉

### 4.11 `services/ingestion_service.py`

責任：
- 串接 job lifecycle、cache、connector、raw repository、parser、restaurant repository
- 作為第一版端到端 orchestrator

第一版主要方法：

```python
def ingest_google_place_detail(place_id: str) -> IngestionResult: ...
```

規格要求：
- service 是第一個可以碰多個 repository / connector 的地方
- service 要明確回傳：是否 cache hit、job id、raw document id、restaurant id

### 4.12 `jobs/run_google_places_sync.py`

責任：
- 提供可手動執行的 entry point

建議指令：

```bash
python -m food_data_ingestion.jobs.run_google_places_sync --place-id <PLACE_ID>
```

規格要求：
- CLI 只做參數解析與呼叫 service
- 結果輸出需包含 job id / cache hit / raw document id / restaurant ids

---

## 5. 資料契約規格

## 5.1 Cache entry 契約

`ApiRequestCacheEntry` 必須與 `ingestion.api_request_cache` 對齊，欄位名稱不得另起別名。

必要欄位：
- `cache_key`
- `provider`
- `resource_type`
- `expires_at`

成功時建議欄位：
- `status_code`
- `response_headers`
- `response_body`
- `response_text`
- `content_hash`
- `fetched_at`
- `source_meta`
- `request_fingerprint`

失敗時建議欄位：
- `is_error=True`
- `error_message`
- `status_code`
- `expires_at`

### 5.1.1 `request_fingerprint` 定義

用途：
- 識別「是否為同一個邏輯請求」
- 與 `content_hash` 分工：前者代表請求等價，後者代表內容等價

第一版建議組成：
- `provider`
- `resource_type`
- canonicalized `request_params`
- `normalized_url`（若有）

規格要求：
- `request_fingerprint` 建議採 `SHA-256`
- 計算邏輯必須集中實作，不可在不同 connector 各自拼裝
- 同一 logical request 重跑時應得到相同 fingerprint

## 5.2 Raw document 契約

每筆真實外部請求落 raw 時，至少應保存：

- `platform`
- `document_type`
- `source_url`
- `external_id`
- `parent_external_id`
- `http_status`
- `fetched_at`
- `raw_json` 或 `raw_text` 或 `raw_html`
- `response_headers`
- `content_hash`
- `parser_version`
- `parsed_at`
- `cache_entry_id`（如果由 connector 流程產生）

補充說明：
- `parent_external_id` 用於表達來源內容的父子關係，例如 reply/comment 隸屬某 post、review item 隸屬某 parent entity
- 第一版 Google Places place detail ingestion 可不填，但 schema 與 repository 必須保留這個欄位的傳遞能力

### 5.2.1 `content_hash` 定義

第一版統一定義如下：

- algorithm：`SHA-256`
- 若有 `response_body`：先做 JSON canonical serialization 再 hash
- 若沒有 `response_body`、但有 `response_text` / `raw_html`：對 normalized text 做 hash
- 需排除明顯不穩定且不影響內容語意的欄位，例如 `timestamp`、`request_id`、`trace_id`

規格要求：
- `content_hash` helper 必須集中實作，不可散落在各 connector 各寫一套
- canonical serialization 必須保證 key order 穩定
- 相同語意內容在重跑時應得到相同 hash

## 5.3 Source target policy 契約

`ingestion.source_targets.crawl_policy` 是 target-level 的抓取策略覆寫欄位，用來承接來源頻率控制、cooldown 與 target 特例設定。

第一版定位：
- connector / service 有一份內建 default policy
- `crawl_policy` 作為 target-level override
- 若 `crawl_policy` 與 connector default 同時存在，以 `crawl_policy` 覆蓋

第一版建議 JSON 結構：

```json
{
  "enabled": true,
  "ttl_seconds": 21600,
  "refresh_after_seconds": 10800,
  "cooldown_seconds": 600,
  "max_retries": 3,
  "rate_limit_bucket": "google_places_default",
  "lock_scope": "platform_resource_identifier"
}
```

規格要求：
- 第一版至少需支援讀取 `ttl_seconds`、`refresh_after_seconds`、`cooldown_seconds`、`max_retries`
- `crawl_policy` 不可淪為裝飾欄位；若 schema 保留此欄位，service / connector 必須定義讀取點
- 所有 JSON key 命名採 snake_case

## 5.4 Parsed restaurant 契約

parser 輸出應是「normalized domain model」，而不是 SQL row string。

建議結構：

```python
@dataclass(frozen=True)
class ParsedRestaurant:
    canonical_name: str
    normalized_name: str
    address: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    average_rating: Decimal | None
    rating_count: int | None
    business_hours: dict[str, Any]
    source_meta: dict[str, Any]

@dataclass(frozen=True)
class ParsedExternalRef:
    platform: str
    external_id: str
    external_url: str | None
    ref_type: str | None
    is_primary: bool
    metadata: dict[str, Any]
```

---

## 6. 標準執行流程規格

## 6.1 Google Places detail

```text
Input: place_id
  ↓
建立 crawl_job(status=pending)
  ↓
切為 running
  ↓
組 cache key: google_places:v1:place_detail:{place_id}
  ↓
查 api_request_cache
  ├─ hit 且未過期 → mark_hit → 使用 cache payload
  └─ miss/expired → 發 Places API request
                    ↓
                 upsert api_request_cache
                    ↓
                 create raw_document
  ↓
parse raw/cache payload → ParsedRestaurant
  ↓
upsert restaurants
  ↓
upsert restaurant_external_refs
  ↓
更新 crawl_job(status=success, stats)
  ↓
回傳 IngestionResult
```

## 6.2 失敗流程

```text
建立/切換 crawl_job 為 running
  ↓
外部請求失敗或 parser 發生不可恢復錯誤
  ↓
若有 response，仍盡量寫 cache / raw
  ↓
更新 crawl_job(status=failed, error_message)
  ↓
拋出可定位錯誤或回傳失敗結果
```

規格要求：
- job 狀態變更必須可追
- 就算最終失敗，只要曾拿到 response，仍要盡量保留 raw
- parser 錯誤與 request 錯誤要分開記錄

## 6.3 Transaction boundary

第一版先明確切成三段，而不是把整條 ingestion 鏈打成單一大 transaction：

### Transaction A：request / cache / raw

包含：
- 建立或更新 `crawl_jobs` 為 `running`
- `api_request_cache` upsert
- `raw_documents` create

目的：
- 只要外部 request 已發生且拿到 response，就把 cache 與 raw 先安全落地

### Transaction B：structured persistence

包含：
- parser 輸出後的 `restaurants` upsert
- `restaurant_external_refs` upsert
- 後續若有 aliases，再與 restaurant persistence 同組處理

目的：
- 將 structured 寫入視為第二段獨立可重試步驟

### Transaction C：job closeout

包含：
- 更新 `crawl_jobs` 最終狀態為 `success` / `failed` / `partial`
- 寫入 `stats` / `error_message`

規格要求：
- 不要把外部 HTTP request 生命週期包在長交易內
- Transaction A 成功、Transaction B 失敗時，job 應標記為 `failed` 或 `partial`，且 raw 仍保留
- job closeout 必須永遠嘗試執行，即使 structured persistence 失敗

## 6.4 Crawl lock 策略

第一版尚未接 Redis，因此 crawl lock 明確採用 PostgreSQL advisory lock：

- lock key 輸入：`platform + resource_type + identifier`
- 以 stable hash 轉成 advisory lock 所需整數 key
- 進入真實 request 前先執行 `pg_try_advisory_lock(...)`
- 取得失敗代表已有相同 target 正在抓取，本次 job 應標記為 `skipped` 或等價狀態並留下原因
- request 流程結束後必須釋放 lock

規格要求：
- 第一版不得額外在 schema 自造 `locked_at` / `lock_owner` 機制
- advisory lock 的 key 算法必須集中實作，避免不同 connector 算出不同 key
- crawl lock 是為了避免同 target 併發抓取，不是拿來取代 queue / scheduler

---

## 7. TTL 與快取政策規格

本節是第一版 TTL 的**唯一真相來源**。其他文件若提到 TTL，只能引用本節，不應再各自定義另一套固定數字。

第一版先固定以下規則：

### 7.1 成功結果

| provider/resource | TTL | refresh_after |
|---|---:|---:|
| `google_places/place_detail` | 6 小時 | 3 小時 |
| `google_places/search_result` | 2 小時 | 1 小時 |
| `article/detail` | 6 小時 | 3 小時 |

### 7.2 錯誤結果

| 類型 | TTL |
|---|---:|
| timeout / 5xx | 60 秒 |
| 429 | 10 分鐘 |
| 403 / captcha / suspicious | 2 小時 |
| 4xx 其他 | 10 分鐘 |

規格要求：
- TTL 計算不得散落在多個 connector；應集中到 helper 或 policy function
- `expires_at` 與 `refresh_after` 以 UTC 儲存

---

## 8. 測試與驗證規格

## 8.1 pytest 單元測試

至少新增以下測試檔：

- `tests/test_db_settings.py`
- `tests/test_psycopg_session.py`
- `tests/test_raw_repository.py`
- `tests/test_google_places_connector.py`
- `tests/test_google_places_parser.py`
- `tests/test_ingestion_service.py`

### 必測案例

#### cache repository
- cache key 正規化穩定
- 未過期 cache 會被回傳
- 過期 cache 不回傳
- hit 會更新 hit_count / last_accessed_at
- upsert 會正確覆寫既有 cache_key

#### db/session
- DSN 組裝正確
- cursor row 可轉成 dict
- execute / fetchone interface 與 repository 相容

#### google places connector
- hit cache 時不發外部請求
- miss cache 時會打 API 並回傳標準 `FetchResult`
- 錯誤 status code 會套用對應 TTL

#### parser
- 可從完整 places detail 解析出 restaurant
- 缺少部分欄位時仍能輸出最小合法結果
- external ref 會正確帶出 place_id

#### ingestion service
- cache hit 路徑可走完整流程
- cache miss 路徑會寫 cache + raw + restaurant
- 失敗路徑會更新 crawl job 為 failed

## 8.2 DB smoke check

最少需要一個可重複執行的 smoke 流程：

1. 建立測試 place_id
2. 執行 CLI / service
3. 驗證以下資料確實存在：
   - `ingestion.crawl_jobs`
   - `ingestion.api_request_cache`
   - `ingestion.raw_documents`
   - `ingestion.restaurants`
   - `ingestion.restaurant_external_refs`
4. 再執行第二次，確認發生 cache hit 或至少不會重複爆資料

---

## 9. 實作順序規格

### Phase 1：DB 接線

交付：
- `config.py` 擴充
- `db/connection.py`
- `db/psycopg_session.py`
- 對應測試

完成判準：
- repository 可透過真實 psycopg session 運作
- pytest 綠燈

### Phase 2：Raw 與 Job Repository

交付：
- `models/raw_document.py`
- `models/crawl_job.py`
- `storage/raw_repository.py`
- `storage/crawl_job_repository.py`
- 對應測試

完成判準：
- 可建立 job、更新 job 狀態、寫 raw

### Phase 3：Google Places Connector

交付：
- `connectors/base.py`
- `connectors/google_places.py`
- TTL policy helper
- 對應測試

完成判準：
- cache hit / miss 行為正確
- 失敗 TTL 正確

### Phase 4：Parser 與 Restaurant Persistence

交付：
- `models/restaurant.py`
- `parsers/google_places.py`
- `storage/restaurant_repository.py`
- 對應測試

完成判準：
- places detail 可寫成 restaurant + external ref

### Phase 5：端到端 Service 與 CLI

交付：
- `services/ingestion_service.py`
- `jobs/run_google_places_sync.py`
- integration / smoke 驗證

完成判準：
- 可透過單一命令完成一次 place detail ingestion
- 可在 DB 查到完整鏈路資料

---

## 10. 驗收標準

當以下條件全部成立，才算第一版真正接上：

1. `pytest` 全數通過
2. 真實 DB 連線可用
3. 執行一次 Google Places detail ingestion 後，以下表都有資料落地：
   - `crawl_jobs`
   - `api_request_cache`
   - `raw_documents`
   - `restaurants`
   - `restaurant_external_refs`
4. 第二次執行相同 `place_id` 時，可觀察到 cache hit 或等價的快取重用效果
5. README 與文件已能指向這份實作規格，不需靠聊天室補口頭說明

---

## 11. 不可違反的工程原則

1. **Connector 不寫 SQL**
2. **Parser 不碰環境變數與 DB**
3. **Repository 不負責商業流程編排**
4. **Service 才負責串整條鏈路**
5. **raw 必須先於 structured 落地**（只要有真實 response）
6. **cache 是加速層，不是真相來源**
7. **平台特定 id 一律放 external refs，不內嵌到 restaurant 主表**
8. **每次實作都要附測試或 smoke 驗證**

---

## 12. 建議下一份文件

這份文件完成後，下一步最值得補的是：

1. `docs/google-places-connector-spec.md`
   - API 欄位 mapping
   - TTL policy 細節
   - error handling matrix

2. `docs/runtime-smoke-checks.md`
   - 本機執行指令
   - DB 查驗 SQL
   - 成功 / 失敗範例輸出

這兩份會把本規格再往「可直接照單施工」推進一層。