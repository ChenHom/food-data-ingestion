# 爬取規則與資料取得政策

## 目標

本文件定義：
1. 哪些來源優先抓
2. 哪些來源只能低頻 / PoC
3. 怎樣減少直接打真實 API / 真實頁面
4. 怎樣保護資料正確性與可追溯性

---

## 一、來源分級

### A 級：優先接，穩定度較高
適合第一版優先接入。

- Google Places API
- 公開部落格 / 新聞 / 美食網站
- 公開論壇文章頁
- 自己提供的 URL / 店名 / 關鍵字清單

策略：
- 作為店家主檔與長文心得的主要來源
- **優先用免費來源做 discovery，再用 Google Places 補主檔**
- Google Places 預設只做 `place_id` 驅動的 place detail
- keyword search / nearby search 不作為第一版預設入口
- URL 類型頁面先做 request cache + raw 落地

### B 級：可做，但需低頻與風險隔離

- Google Maps 公開頁 / 評論頁
- Instagram 公開貼文
- Threads 公開貼文

策略：
- 第一版只做定向蒐集，不做全站掃描
- 只抓公開可讀內容
- 優先使用店名 / 關鍵字 / 指定帳號 / 指定 hashtag
- 必須有頻率控制、重試與 cooldown

### C 級：第一版不碰

- 私人帳號 / 私人群組
- 需要大量登入互動才能取得的內容
- 明顯高風險、高驗證成本的資料來源

---

## 二、抓取規則

## 2.1 基本原則

1. 先查 cache，再打來源
2. 先落 raw_documents，再做 parse
3. 每次抓取都要保留來源資訊
4. 失敗結果不可長時間快取
5. 不同來源要用不同 TTL 與頻率策略
6. 預設先走免費來源 discovery，再進付費 enrichment
7. 付費 API 必須有每日 budget guard
8. 各 job 的年份範圍、文章類型與處理邊界應由 job policy 決定
9. 各來源 parser 規則應由 parser profile 決定，不要散落在 job 與 repository 內
10. parser 產出的 discovery 結果應先轉成統一的 normalized model，再交給統一入口寫入

## 2.2 單次抓取流程

標準流程：

1. 根據 `source_targets` 產生 crawl job
2. 依 provider + resource_type + identifier 組 cache key
3. 查 `ingestion.api_request_cache`
4. 若 cache 仍有效：
   - 回傳 cache 內容
   - 更新 `last_accessed_at` 與 `hit_count`
5. 若 cache 不存在或已過期：
   - 發送真實請求
   - 落 raw_documents
   - 更新 api_request_cache
   - 將 raw 交給 parser

### 2.2.1 第一版免費優先流程

第一版預設流程應為：

1. 從免費來源取得候選店家資料
   - seed 名單
   - 公開文章 / URL / 論壇頁
   - 已知 Google Maps 分享連結
2. 抽取並正規化店名、地址、URL 等識別資訊
3. 盡可能先收斂成 `place_id`
4. 僅在已取得 `place_id` 後才進入 Google Places detail ingestion
5. `searchText` / `Nearby Search` 僅能作為少量 fallback，且必須受 budget guard 控制

## 2.3 cache 規則

本節只描述 cache 策略原則；**固定 TTL 數值以 `docs/implementation-spec.md` 第 7 節為唯一真相來源**。

### 店家基本資料 API
例如 Google Places detail
- TTL：依 implementation-spec 固定值執行
- refresh_after：依 implementation-spec 固定值執行，且需小於 `expires_at`
- 用途：減少重複查同一家店

### 搜尋結果頁
例如某地區餐廳搜尋
- 第一版不作為預設流程
- 若 fallback 啟用：TTL 依 implementation-spec 固定值執行
- 用途：僅限少量補洞，避免重複搜尋

### 文章 / 貼文 detail
- TTL：依 implementation-spec 固定值執行
- 用途：內容通常穩定，可短期重用

### 評論列表頁 / 動態頁
- TTL：由 connector policy helper 決定，但不得與 implementation-spec 衝突
- 用途：變動較快，避免快取太久

### 失敗結果
- timeout / 5xx：依 implementation-spec 固定值執行
- 429：依 implementation-spec 固定值執行
- 403 / suspicious / captcha：依 implementation-spec 固定值執行

---

## 三、頻率控制與風險控制

## 3.1 必做控制

- 每個來源要有 request cache
- 每個來源要有 rate limit state
- 每個 target 要有 crawl lock
- 429 / 403 要有 cooldown 機制
- 相同 external_id / content_hash 要去重
- 付費 API 要有每日 budget guard
- 付費 search 要有顯式開關，預設關閉

第一版補充規則：
- crawl lock 明確採 PostgreSQL `pg_try_advisory_lock(...)`
- lock key 由 `platform + resource_type + identifier` 穩定映射而成
- 若 advisory lock 取得失敗，該次 job 應記錄為 `skipped` 或等價狀態，不應繼續打真實來源
- `content_hash` 一律採 `SHA-256`
- 若來源為 JSON，必須先做 canonical serialization 再 hash
- 若來源為文字 / HTML，必須先做 normalized text hash

## 3.2 Google Places API

建議：
- 優先用來建 restaurants 主檔
- 預設只做 `place_detail(place_id)`
- place detail 可直接接 cache 表
- field mask 先收斂在建主檔最小必要欄位
- 請求應記錄 request_params 與 provider

第一版免費優先策略：
- `place_id` 為主入口，不以 keyword search 當預設入口
- search / nearby 僅作少量 fallback，用於免費來源無法補齊 `place_id` 的情況
- detail 預設走冷資料 TTL；熱資料需由 `crawl_policy` 或明確 target policy 升級
- Places detail、Geocoding、Places search 都要有每日 request budget

禁止：
- 把 Places API 當作唯一真相而不留 raw
- 在第一版把 paid search 當成市場掃描主力
- 沒有 budget guard 就直接放大 Places search / Nearby Search

## 3.3 Google Maps 公開頁

建議：
- 第一版只做少量 PoC
- 優先抓已知店家頁，而不是盲掃
- 先驗證內容欄位是否穩定，再放大

注意：
- 結構容易變
- 評論頁動態載入多
- 不應高頻輪詢

## 3.4 Instagram / Threads

建議：
- 只抓公開可讀貼文
- 先做指定帳號 / hashtag / 關鍵字 / 店名定向蒐集
- 先以 post 為主，不先追求留言全抓

禁止：
- 第一版就做全站 coverage 幻想
- 把登入後內容當成基本功能依賴

---

## 四、可追溯性規則

每筆資料至少要能追到：
- 來源平台
- source_url
- external_id
- fetched_at
- observed_at（若來源可提供）
- raw_document_id

要求：
- structured content 必須可回推 raw_document
- restaurant linking 必須保留 match_method 與 confidence_score
- aspect / tags 必須保留 source 或 extracted_by

---

## 五、資料正確性規則

1. cache 影響的是 freshness，不是真相
2. raw_documents 不可由 cache 取代
3. restaurants / contents / linking 必須持久化在 PostgreSQL
4. 同一來源多次抓取允許保留歷史觀測值
5. parser / classifier 邏輯更新時，不應直接覆蓋 raw 真相

---

## 六、第一版建議來源策略

### 起手式
1. 免費來源 discovery：seed 名單、URL、文章頁、論壇頁
2. Google Places API：以 `place_id` 補店家主檔
3. Blog / article scraper：長文心得
4. IG / Threads：定向蒐集公開貼文

### 建議 target 類型
- keyword
- hashtag
- account
- url
- place_id
- geo_area

---

## 七、第一版實作順序

1. 完成 cache repository 與 DB adapter
2. 先接 `place_id` 驅動的 Google Places detail connector
3. 建 raw landing + parser pipeline
4. 接免費來源 discovery（article / URL / seed 名單）
5. 最後才評估少量 search fallback 與 IG / Threads / Maps PoC
