/***************************************************************************************************
Asset:        Zero to Snowflake - Cortex AI Functions でレビュー分析
Version:      v4
Copyright(c): 2025 Snowflake Inc. All rights reserved.

ストーリー:
  Kitakata Ramen Bar 東京店のマネージャーが、溜まったレビューを AI 関数で分析し、次のアクションを決める。

目次:
  1. セッションの初期設定
  2. データ確認           — 日本語レビューで内容を把握
  3. AI_COMPLETE (JSON)   — 多面評価 + 集計・可視化
  4. AI_AGG              — 経営層向けエグゼクティブサマリー

前提条件:
  - setup.sql 実行済み
  - ロール tb_data_engineer、ウェアハウス tb_de_wh が利用可能
  - 対象ビュー: TB_101.HARMONIZED.TRUCK_REVIEWS_V
***************************************************************************************************/

-- ============================================================
-- 1. セッションの初期設定
-- ============================================================

USE ROLE tb_data_engineer;
USE DATABASE tb_101;
USE WAREHOUSE tb_de_wh;

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1,"minor":4},"attributes":{"is_quickstart":1,"source":"tastybytes","vignette":"cortex_ai_functions"}}';


-- ============================================================
-- 2. データ確認 — 日本語レビューで内容を把握
-- ============================================================
-- まず日本語レビューを確認し、どんな内容が書かれているか把握する。

SELECT
    review
FROM harmonized.truck_reviews_v
WHERE truck_brand_name = 'Kitakata Ramen Bar'
  AND primary_city = 'Tokyo'
  AND language = 'ja'
  AND review IS NOT NULL
ORDER BY date DESC
LIMIT 10;


-- ============================================================
-- 3. AI_COMPLETE (JSON) — 1 件ごとの多面評価 + 集計
-- ============================================================
-- AI_COMPLETE + response_format で「推論・分類」を構造化 JSON で受け取る。
-- 1 回の AI 呼び出しで以下の 5 次元を同時に取得する:
--   • is_complaint   — クレームを含むか (boolean)
--   • category       — 主なトピック (Food Quality / Service / Value for Money / Atmosphere / Other)
--   • sentiment      — 感情 (positive / negative / neutral / mixed)
--   • key_issue      — クレームの具体的な問題点 (なければ null)
--   • mentioned_item — 言及されたメニュー名 (なければ null)
--
-- 結果をテーブルに保存し、後続の集計クエリを高速化・再実行コストを削減する。

-- 3-a. 全レビューを AI で多面評価してテーブルに保存（JSON をカラムにパース済み）
CREATE OR REPLACE TABLE analytics.kitakata_reviews_analysis AS
WITH raw_analysis AS (
    SELECT
        review_id,
        date,
        review,
        AI_COMPLETE(
            'claude-sonnet-4-6',
            'Analyze the following ramen restaurant review and respond in JSON only.' || CHR(10) || CHR(10) || review,
            response_format => {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'is_complaint':   {'type': 'boolean', 'description': 'true if the review contains a complaint or dissatisfaction'},
                        'category':       {'type': 'string',  'description': 'Primary topic: Food Quality, Service, Value for Money, Atmosphere, or Other'},
                        'sentiment':      {'type': 'string',  'description': 'Overall sentiment: positive, negative, neutral, or mixed'},
                        'key_issue':      {'type': 'string',  'description': 'Main complaint or issue. null if no complaint'},
                        'mentioned_item': {'type': 'string',  'description': 'Specific menu item mentioned. null if none'}
                    },
                    'required': ['is_complaint', 'category', 'sentiment', 'key_issue', 'mentioned_item']
                }
            }
        )::VARIANT AS analysis
    FROM harmonized.truck_reviews_v
    WHERE truck_brand_name = 'Kitakata Ramen Bar'
      AND primary_city = 'Tokyo'
      AND review IS NOT NULL
)
SELECT
    review_id,
    date,
    review,
    analysis:is_complaint::BOOLEAN   AS is_complaint,
    analysis:category::STRING        AS category,
    analysis:sentiment::STRING       AS sentiment,
    analysis:key_issue::STRING       AS key_issue,
    NULLIF(analysis:mentioned_item::STRING, 'null') AS mentioned_item
FROM raw_analysis;

-- 3-b. 分析結果サンプルの確認
SELECT review, is_complaint, category, sentiment, key_issue, mentioned_item
FROM analytics.kitakata_reviews_analysis
LIMIT 5;

-- 3-c. KPI サマリー: クレーム率とセンチメント内訳
SELECT
    COUNT(*)                                                        AS total_reviews,
    COUNT_IF(is_complaint)                                          AS complaint_count,
    ROUND(COUNT_IF(is_complaint) * 100.0 / COUNT(*), 1)             AS complaint_rate_pct,
    ROUND(COUNT_IF(sentiment = 'positive') * 100.0 / COUNT(*), 1)   AS positive_rate_pct,
    ROUND(COUNT_IF(sentiment = 'negative') * 100.0 / COUNT(*), 1)   AS negative_rate_pct,
    ROUND(COUNT_IF(sentiment = 'mixed')    * 100.0 / COUNT(*), 1)   AS mixed_rate_pct,
    ROUND(COUNT_IF(sentiment = 'neutral')  * 100.0 / COUNT(*), 1)   AS neutral_rate_pct
FROM analytics.kitakata_reviews_analysis;

-- 3-d. カテゴリ別件数分布
SELECT
    category,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM analytics.kitakata_reviews_analysis
GROUP BY category
ORDER BY review_count DESC;

-- 3-e. センチメント×カテゴリのクロス集計
SELECT
    category,
    sentiment,
    COUNT(*) AS count
FROM analytics.kitakata_reviews_analysis
GROUP BY category, sentiment
ORDER BY category, count DESC;

-- 3-f. メニューアイテム別の評価サマリー
SELECT
    mentioned_item AS menu_item,
    COUNT(*) AS mention_count,
    ROUND(COUNT_IF(is_complaint) * 100.0 / COUNT(*), 1)              AS complaint_rate_pct,
    ROUND(COUNT_IF(sentiment = 'positive') * 100.0 / COUNT(*), 1)    AS positive_pct,
    ROUND(COUNT_IF(sentiment = 'negative') * 100.0 / COUNT(*), 1)    AS negative_pct,
    ROUND(COUNT_IF(sentiment = 'mixed') * 100.0 / COUNT(*), 1)       AS mixed_pct
FROM analytics.kitakata_reviews_analysis
WHERE mentioned_item IS NOT NULL
GROUP BY menu_item
ORDER BY mention_count DESC
LIMIT 10;


-- ============================================================
-- 4. AI_AGG — 経営層向けエグゼクティブサマリー
-- ============================================================
-- レビュー全体をカテゴリごとに店舗運営の改善提案を生成する。

SELECT
    category,
    AI_AGG(
        review,
        'You are a restaurant consultant. Analyze the following customer reviews for Kitakata Ramen Bar in Tokyo and provide improvement recommendations in Japanese.

Reply ONLY in the following format with no preamble:

【顧客満足度の現状】
（1〜2文で全体の傾向を要約）

【高評価ポイント トップ3】
1. [強み1]
2. [強み2]
3. [強み3]

【改善すべき点 トップ3】
1. [改善点1]
2. [改善点2]
3. [改善点3]

【推奨アクション】
（最も優先度の高い施策を1つ、具体的に記述）'
    ) AS executive_summary
FROM analytics.kitakata_reviews_analysis
GROUP BY category;
