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
         │               Structured Storage                      │
         │ restaurants / external_refs / contents / links        │
         │ review_aspects / tags / restaurant_tags / content_tags│
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

### 2. storage/
責任：
- 封裝 DB 存取
- cache repository
- raw repository
- content / restaurant repository

### 3. parsers/
責任：
- 從 raw 抽取結構化資料
- 將不同來源轉成統一內容模型
- 做 normalize、dedupe、linking 前處理

### 4. jobs/
責任：
- 產生 crawl jobs
- 重試
- cooldown
- queue orchestration

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

### 流程 B：抓文章 / 貼文內容

1. scheduler 建立 crawl job
2. connector 先查 cache
3. miss 時抓真實頁面
4. raw 落 `raw_documents`
5. parser 抽成 `contents`
6. matcher 對到 `restaurants`
7. aspect / tag pipeline 補 `review_aspects` 與 `content_tags`

---

## 四、第一版建議實作順序

### 4.1 首波交付（以 implementation-spec Phase 1–5 為準）

1. DB adapter
2. cache service
3. Google Places connector
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
