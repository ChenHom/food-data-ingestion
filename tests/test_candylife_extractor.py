from __future__ import annotations

from food_data_ingestion.parsers.candylife import extract_candylife_article


SAMPLE_HTML = """
<html>
  <head>
    <title>255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶 - 糖糖's 享食生活</title>
    <meta property="article:published_time" content="2026-04-21T11:45:48+00:00" />
  </head>
  <body>
    <article>
      <h1>255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶</h1>
      <a href="https://candylife.tw/category/taichung-food/">台中美食</a>
      <a href="https://candylife.tw/category/taichung-food/taichung-dessert/">台中甜點</a>
      <a href="https://candylife.tw/category/taichung-food/taichung-cafe/">台中咖啡</a>
      <p>想在實驗室裡喝下午茶嗎？台中南屯255 LAB café 鄰近IKEA，是近期超人氣的實驗室主題咖啡廳。</p>
      <h2>255 LAB café位置</h2>
      <p>255 LAB café就坐落在台中南屯的大墩十一街上，門面潔白簡約，非常顯眼。</p>
      <p>《店家資訊》</p>
      <p>店家：255 LAB café | 二五五咖啡實驗所 南屯 電話：04-22512075 地址：台中市南屯區大墩十一街392號 時間：平日08:00~16:00；假日10:00~18:00</p>
    </article>
  </body>
</html>
"""


def test_extract_candylife_article_returns_article_metadata_and_restaurant_candidate():
    result = extract_candylife_article(
        html=SAMPLE_HTML,
        source_url="https://candylife.tw/255labcafe/",
    )

    assert result.source_url == "https://candylife.tw/255labcafe/"
    assert result.title == "255 LAB café｜台中南屯咖啡廳推薦，吸睛試管咖啡，鄰近IKEA的實驗室風格下午茶"
    assert result.published_at == "2026-04-21T11:45:48+00:00"
    assert result.categories == ("台中美食", "台中甜點", "台中咖啡")
    assert result.restaurant_candidates[0].name == "255 LAB café"
    assert result.restaurant_candidates[0].address == "台中市南屯區大墩十一街392號"
    assert result.restaurant_candidates[0].phone == "04-22512075"
    assert result.restaurant_candidates[0].opening_hours == "平日08:00~16:00；假日10:00~18:00"
    assert result.restaurant_candidates[0].source_url == "https://candylife.tw/255labcafe/"


def test_extract_candylife_article_uses_h1_when_title_tag_contains_site_name_suffix():
    result = extract_candylife_article(
        html=SAMPLE_HTML,
        source_url="https://candylife.tw/255labcafe/",
    )

    assert result.title.endswith("實驗室風格下午茶")
    assert "糖糖's 享食生活" not in result.title


def test_extract_candylife_article_returns_empty_candidates_when_store_info_missing():
    html = """
    <html>
      <head><title>普通文章 - 糖糖's 享食生活</title></head>
      <body>
        <article>
          <h1>普通文章</h1>
          <p>這是一篇沒有店家資訊的文章。</p>
        </article>
      </body>
    </html>
    """

    result = extract_candylife_article(html=html, source_url="https://candylife.tw/plain/")

    assert result.title == "普通文章"
    assert result.restaurant_candidates == ()
