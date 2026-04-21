# 專案總覽

## 專案目的

`food-data-ingestion` 是一個獨立的資料蒐集專案。
第一版先聚焦在「資料取得」而不是直接做美食 app，目標是先把多來源資料抓取、快取、原始落地、結構化存放這幾件事做穩。

目前主要蒐集主題雖然是美食，但整體設計刻意做成泛用 ingestion 架構，後續可擴展到其他主題。

## 第一版目標

1. 降低直接打真實 API / 真實頁面的次數
2. 為多來源抓取建立統一的原始資料落地格式
3. 為後續分類、標籤、推薦、搜尋保留可追溯資料
4. 將資料取得邏輯與 app / 推薦邏輯解耦

## 第一版範圍

- 第一層 request cache
- source target 與 crawl job 管理
- raw documents / raw assets 落地
- restaurants 主檔
- contents 結構化內容
- content 與 restaurant linking
- tags / aspects 基礎欄位
- ingestion logs

## 不在第一版範圍

- 完整推薦系統
- 前端 app
- 複雜排序模型
- 即時串流處理
- 大規模分散式爬蟲叢集

## 主要來源類型

### 低風險 / 優先來源
- Google Places API（店家基本資料）
- 部落格 / 新聞 / 美食網站 / 論壇公開頁面
- 公開可讀文章頁

### 高價值但高風險來源
- Google Maps 公開頁 / 評論頁
- Instagram 公開貼文
- Threads 公開貼文

## 核心設計原則

1. 先保 raw，再談 parse
2. 先可追溯，再談智慧分析
3. 先分清 freshness，再談 correctness
4. cache 是加速層，不是真相來源
5. 用 schema 與模組分層保持來源可插拔

## 文件導覽

- `docs/crawling-rules.md`：爬取規則、來源分級、頻率控制與法務/工程邊界
- `docs/data-model.md`：資料怎麼結構化存、每張表的用途
- `docs/architecture.md`：架構圖、資料流、模組邊界、實作建議
