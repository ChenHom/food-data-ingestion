# 資料模型與結構化存放規則

本文件說明第一版 DDL 的分層、每張表的責任，與資料應如何從 raw 流到 structured。

---

## 一、分層概念

整體資料模型分成 4 層：

1. 調度層：`source_targets`, `crawl_jobs`
2. 快取與 raw 層：`api_request_cache`, `raw_documents`, `raw_assets`
3. discovery staging 與結構化實體層：`discovered_place_candidates`, `restaurants`, `contents`, `content_restaurant_links`
4. 標註與觀測層：`review_aspects`, `tags`, `restaurant_tags`, `content_tags`, `ingestion_logs`

---

## 二、核心表說明

## 2.1 api_request_cache

用途：
- 第一層 request cache
- 降低直接打真實 API / 頁面的次數

關鍵欄位：
- `cache_key`：唯一快取鍵
- `provider`：google_places / google_maps / instagram / threads / blog ...
- `resource_type`：place_detail / search_result / post / article ...
- `request_fingerprint`：用來識別是否為同一個邏輯請求
- `request_params`：查詢參數
- `response_body` / `response_text`
- `fetched_at`
- `refresh_after`
- `expires_at`
- `last_accessed_at`
- `hit_count`
- `is_error`

規則：
- cache 只做加速，不可作為唯一真相
- 有效 cache 命中時更新 hit_count
- 失敗快取 TTL 應短於成功快取
- `request_fingerprint` 與 `content_hash` 需分開：前者識別請求等價，後者識別內容等價

## 2.2 source_targets

用途：
- 定義系統應該抓哪些目標
- 以 `crawl_policy` 保存 target-level 的抓取策略覆寫

例子：
- platform=google_places, target_type=place_id, target_value=ChIJ...
- platform=blog, target_type=url, target_value=https://...
- platform=instagram, target_type=hashtag, target_value=台北咖啡廳

`crawl_policy` 建議至少支援：
- `freshness_profile`
- `ttl_seconds`
- `refresh_after_seconds`
- `cooldown_seconds`
- `max_retries`
- `daily_budget`
- `rate_limit_bucket`

補充：
- 第一版 `google_places` 預設 target 應優先是 `place_id`
- `keyword` / `geo_area` 可存在於 `source_targets`，但第一版應優先用於免費來源 discovery，而不是直接驅動 paid Places search

## 2.3 crawl_jobs

用途：
- 記錄每次抓取任務
- 追蹤成功、失敗、重試、統計資訊

規則：
- job 不直接承載原始內容
- job 只記錄執行上下文與狀態

## 2.4 raw_documents

用途：
- 原始資料落地表
- 所有外部抓回來的 JSON / HTML / text 都先落這裡

關鍵欄位：
- `platform`
- `document_type`
- `source_url`
- `canonical_url`
- `external_id`
- `parent_external_id`
- `http_status`
- `observed_at`
- `fetched_at`
- `raw_html`
- `raw_text`
- `raw_json`
- `parser_version`
- `parsed_at`
- `cache_entry_id`

規則：
- parser 壞掉時可從 raw 重跑
- 不能只留 parse 後資料而丟 raw
- `raw_documents` 至少必須保存 `raw_html`、`raw_text`、`raw_json` 其中一種內容
- `parser_version` 第一版允許為 `NULL`，等 parser 真正上線後再填入固定版本值
- `parsed_at` 用於追蹤該筆 raw 最後一次被 parser 消化的時間
- `content_hash` 必須有統一算法，否則無法穩定 dedupe / retry / replay
- `parent_external_id` 用於表達 reply / child item 與 parent entity 的關係，第一版 place detail 流程可不填

## 2.5 raw_assets

用途：
- 存 raw document 關聯的圖片 / 影片 / 縮圖資訊

第一版可只存 URL 與 metadata，真正下載可延後。

## 2.6 discovered_place_candidates

用途：
- 保存免費來源 discovery 產出的標準候選店家
- 作為 `UnifiedDiscoveryIngestionService` 的第一版正式 staging 入口
- 提供後續 entity resolution、Google Places enrichment、人工 review 的穩定輸入

關鍵欄位：
- `source_platform`
- `source_url`
- `source_name`
- `candidate_name`
- `address`
- `phone`
- `opening_hours`
- `confidence_score`
- `extraction_method`
- `parser_profile`
- `article_type`
- `raw_document_id`
- `source_meta`
- `candidate_key`

規則：
- 第一版 discovery 不直接寫 `restaurants`，先寫入 staging table
- `source_meta` 應保留 article context 與 candidate context，避免後續 resolution 無法回推
- `candidate_key` 用於同來源同文章候選的穩定去重鍵
- `raw_document_id` 應能回推原始文章落地資料

## 2.7 restaurants

用途：
- 店家主檔
- 所有結構化內容最後都盡量對齊到 restaurant

關鍵欄位：
- `canonical_name`
- `normalized_name`
- `address`
- `latitude`, `longitude`
- `average_rating`
- `rating_count`
- `business_hours`

## 2.8 restaurant_external_refs

用途：
- 對映同一家店在不同平台的 external id

例子：
- Google Places place_id
- Google Maps 頁面 id
- IG profile / location ref

規則：
- `UNIQUE(platform, external_id)`
- restaurant master 與外部平台應分離，不要寫死在 restaurants 主表

## 2.9 restaurant_aliases

用途：
- 處理店名變體、支店名稱、俗稱、縮寫

## 2.10 contents

用途：
- 將 review / social post / article 抽成統一內容表

關鍵欄位：
- `content_type`
- `external_content_id`
- `author_name`
- `published_at`
- `title`
- `text_content`
- `rating_value`
- `like_count`, `comment_count`, `share_count`
- `source_url`
- `raw_document_id`

規則：
- 一律能回推 raw_document
- 不直接在 contents 表綁單一 restaurant
- 若 `external_content_id` 存在，第一版應以 `(platform, external_content_id)` 視為內容唯一識別，用來避免重複落地

## 2.10 content_restaurant_links

用途：
- 內容與店家的 linking 表
- 一篇文可提到多家店

關鍵欄位：
- `match_method`
- `confidence_score`
- `mention_text`
- `is_primary`

規則：
- linking 是獨立責任，不應硬塞在 contents
- 若之後改用更好的 matcher，可重算這張表

## 2.11 review_aspects

用途：
- 將內容拆成口味、服務、環境、價格等面向

## 2.12 tags / restaurant_tags / content_tags

用途：
- 建立 cuisine / scenario / vibe / feature 等標籤系統

例子：
- 拉麵
- 咖啡廳
- 深夜食堂
- 寵物友善
- 高 CP
- 約會

## 2.13 ingestion_logs

用途：
- 追蹤抓取、解析、linking、分類過程中的 debug / warn / error

---

## 三、標準資料流

### 3.1 從來源到 raw

來源請求
→ api_request_cache 檢查
→ 真實請求（若需要）
→ raw_documents / raw_assets

### 3.2 從 raw 到 structured

raw_documents
→ parser
→ contents / restaurants / restaurant_external_refs
→ content_restaurant_links
→ tags / review_aspects

---

## 四、第一版實際存放原則

1. 真相在 PostgreSQL
2. cache 是加速層
3. raw 與 structured 分開
4. linking 與 tagging 分開
5. 外部平台 id 與 restaurant master 分開

---

## 五、第一版預留欄位與第二版可擴充方向

### 第一版先預留

- `raw_documents.parser_version`：第一版 schema 先允許 `NULL`，之後 parser 上線後填固定版本字串，用於重跑與追蹤

### 第二版可擴充方向

- 增加 snapshots / history tables，保留 rating 與評論數歷史變化
- 增加 embeddings / pgvector
- 增加 queue / worker tables 或直接接 Redis
- 增加 classifier_version 欄位
