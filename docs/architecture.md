# 架構圖與資料流

## 一、系統架構圖

```text
                     ┌───────────────────────────┐
                     │      Source Targets       │
                     │ keyword / hashtag / url   │
                     │ account / place_id        │
                     └─────────────┬─────────────┘
                                   │
                                   ▼
                     ┌───────────────────────────┐
                     │       Scheduler / Jobs    │
                     │ crawl_jobs                │
                     │ retry / cooldown / lock   │
                     └─────────────┬─────────────┘
                                   │
                                   ▼
                     ┌───────────────────────────┐
                     │     Connector Layer       │
                     │ google_places             │
                     │ google_maps / ig / thread │
                     │ blog / forum / webpage    │
                     └─────────────┬─────────────┘
                                   │
                      先查 cache   │   再決定是否打來源
                                   ▼
                 ┌────────────────────────────────────┐
                 │      api_request_cache (DB)        │
                 │ cache_key / expires_at / body      │
                 └────────────────┬───────────────────┘
                                  │ miss / expired
                                  ▼
                     ┌───────────────────────────┐
                     │     External Sources      │
                     │ API / web pages / posts   │
                     └─────────────┬─────────────┘
                                   │
                                   ▼
                     ┌───────────────────────────┐
                     │        Raw Landing        │
                     │ raw_documents             │
                     │ raw_assets                │
                     └─────────────┬─────────────┘
                                   │
                                   ▼
                     ┌───────────────────────────┐
                     │      Parser / Matcher     │
                     │ normalize / dedupe        │
                     │ entity linking            │
                     └─────────────┬─────────────┘
                                   │
                                   ▼
         ┌────────────────────────────────────────────────────────┐
         │          Discovery Staging + Structured Storage       │
         │ discovered_place_candidates / restaurants / refs      │
         │ contents / links / aspects / tags                     │
         └────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                     ┌───────────────────────────┐
                     │     Query / Analysis      │
                     │ search / ranking / app    │
                     └───────────────────────────┘
```

---

## 二、模組分工

### 1. connectors/
責任：
- 接外部來源
- 組 request cache key
- 先查 cache，再決定是否發請求
- 取得 response 後落 raw

第一版補充：
- Google Places connector 預設只做 `place_detail(place_id)`
- 免費來源 discovery（文章 / URL / seed 名單）與 paid enrichment 應分開看待
- `searchText` / `Nearby Search` 不作為第一版預設主流程

### 2. storage/
責任：
- 封裝 DB 存取
- cache repository
- raw repository
- discovery candidate staging repository
- content / restaurant repository

### 3. parsers/
責任：
- 從 raw 抽取結構化資料
- 將不同來源轉成統一內容模型
- 做 normalize、dedupe、linking 前處理

補充：
- parser 不應直接決定 DB 寫入細節
- 不同來源的 parser 規則應顯性化成對應的 parser profile
- parser 最終應輸出統一的 `DiscoveredArticle` / `DiscoveredPlaceCandidate` 或其他標準 domain model

### 4. parser_profiles/
責任：
- 定義來源 / target 專屬規則
- 例如年份範圍、文章類型、欄位可信度、抽取方法與 normalized output mapping
- 讓 job 與 parser 各自依 profile 運作，而不是把規則散在 job 與 parser 內

### 5. jobs/
責任：
- 產生 crawl jobs
- 重試
- cooldown
- queue orchestration
- 套用各 job 自己的 source policy / target policy

### 6. discovery/
責任：
- 定義免費來源 discovery 的統一資料模型
- 提供統一候選寫入入口
- 吸收各 parser profile 轉好的標準輸出

---

## 三、標準資料流

### 流程 A：抓店家 detail

1. scheduler 建立 crawl job
2. connector 產生 cache key
3. 查 `api_request_cache`
4. 若 hit 且未過期：直接回 cache
5. 若 miss：打真實 API
6. 將 response 寫入 `raw_documents`
7. upsert `api_request_cache`
8. parser 抽成 `restaurants` 與 `restaurant_external_refs`

### 流程 B：免費來源 discovery 候選流

1. scheduler 建立 crawl job
2. connector 先查 cache 或抓真實頁面
3. raw 落 `raw_documents`
4. source parser 抽原始欄位
5. parser profile 轉成 `DiscoveredArticle` / `DiscoveredPlaceCandidate`
6. `UnifiedDiscoveryIngestionService` 寫入 `ingestion.discovered_place_candidates`
7. 後續再做 entity resolution、Places enrichment、人工 review 或正式餐廳主檔寫入

---

## 四、第一版建議實作順序

### 4.1 首波交付（以 implementation-spec Phase 1–5 為準）

1. DB adapter
2. cache service
3. `place_id` 驅動的 Google Places detail connector
4. raw parser pipeline
5. restaurant persistence / end-to-end ingestion service

### 4.2 後續 phase

6. article scraper
7. restaurant matcher
8. contents / tags / aspects pipeline
9. IG / Threads / Maps PoC connectors

補充說明：
- 本文件描述的是整體架構與後續擴充順序
- 第一版真正的交付驗收邊界仍以 `docs/implementation-spec.md` 為唯一準則

---

## 五、資料庫與快取的職責切分

### PostgreSQL
用途：
- 真相來源
- raw + structured + logs
- 可追溯資料

### Redis（第二步接）
用途：
- lock
- rate limit state
- cooldown
- queue
- 短期 query cache

第一版因為已先有 `api_request_cache` 表，所以即使 Redis 還沒接上，系統也能先運轉。

---

## 六、關鍵設計原則

1. source connector 可插拔
2. parser 與 connector 解耦
3. cache 與 raw 分離
4. restaurant master 與 content 分離
5. linking / tagging / aspect extraction 分離
6. 任何智慧分析都必須能追到 raw
