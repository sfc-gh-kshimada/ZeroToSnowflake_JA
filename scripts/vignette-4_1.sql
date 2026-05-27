/***************************************************************************************************
Asset:        Zero to Snowflake - Cortex AI Functions でレビュー分析
Version:      v2
Copyright(c): 2025 Snowflake Inc. All rights reserved.

ストーリー:
  Smoky BBQ 東京店の顧客満足度を改善するため、日本語レビュー47件を
  Cortex AI Functions で多角的に分析し、経営層への改善提案を作成する。

目次:
  1. セッションの初期設定
  2. AI_CLASSIFY   — レビューのカテゴリ分類
  3. AI_FILTER     — ネガティブレビューの WHERE 句フィルタリング
  4. AI_SENTIMENT  — アスペクト別センチメント分析
  5. AI_EXTRACT    — メニュー名・不満点の構造化抽出
  6. AI_COMPLETE   — 改善提案の構造化 JSON 生成
  7. AI_AGG        — 経営層向けエグゼクティブサマリーの生成

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

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1,"minor":2},"attributes":{"is_quickstart":1,"source":"tastybytes","vignette":"cortex_ai_functions"}}';


-- ============================================================
-- 2. AI_CLASSIFY — 顧客は何についてコメントしているか？
-- ============================================================
-- AI_CLASSIFY はテキストをユーザー定義カテゴリに AI で分類する。
-- キーワードマッチングではなく意味理解に基づくため、表現揺れに強い。
--
-- 戻り値: { "labels": ["Food Quality"], "scores": [0.92] }

-- 2-a. 個別レビューの分類結果を確認
SELECT
    review,
    AI_CLASSIFY(
        review,
        ['Food Quality', 'Service', 'Value for Money', 'Atmosphere']
    ) AS classification
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL
LIMIT 5;

-- 2-b. カテゴリ別の件数分布
WITH classified AS (
    SELECT
        AI_CLASSIFY(
            review,
            ['Food Quality', 'Service', 'Value for Money', 'Atmosphere']
        ):labels[0]::STRING AS category
    FROM harmonized.truck_reviews_v
    WHERE language = 'ja'
      AND truck_brand_name = 'Smoky BBQ'
      AND review IS NOT NULL
)
SELECT
    category,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM classified
GROUP BY category
ORDER BY review_count DESC;


-- ============================================================
-- 3. AI_FILTER — ネガティブレビューを WHERE 句で絞り込む
-- ============================================================
-- AI_FILTER は SQL の WHERE 句に直接組み込めるセマンティックフィルタ。
-- TRUE / FALSE を返すため、他の条件と AND/OR で自由に組み合わせられる。

SELECT
    review
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL
  AND AI_FILTER(PROMPT('This review contains a specific complaint about food quality or taste: {0}', review));


-- ============================================================
-- 4. AI_SENTIMENT — アスペクト別センチメント分析
-- ============================================================
-- AI_SENTIMENT の第2引数にアスペクトを指定すると
-- 「何に対して」ポジティブ/ネガティブかを切り分けられる。
--
-- 戻り値: { "categories": [{"name": "Food Quality", "sentiment": "negative", "score": 0.85}] }

-- 4-a. 個別レビューのアスペクト別センチメント
SELECT
    review,
    AI_SENTIMENT(review, ['Food Quality', 'Service', 'Value for Money']) AS sentiment
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL
LIMIT 5;

-- 4-b. アスペクト別のセンチメント分布集計
WITH sentiments AS (
    SELECT
        c.value:name::STRING AS aspect,
        c.value:sentiment::STRING AS sentiment
    FROM harmonized.truck_reviews_v,
        LATERAL FLATTEN(input => AI_SENTIMENT(review, ['Food Quality', 'Service', 'Value for Money']):categories) c
    WHERE language = 'ja'
      AND truck_brand_name = 'Smoky BBQ'
      AND review IS NOT NULL
)
SELECT
    aspect,
    COUNT(*) AS total,
    ROUND(COUNT_IF(sentiment = 'positive') * 100.0 / COUNT(*), 1) AS positive_pct,
    ROUND(COUNT_IF(sentiment = 'negative') * 100.0 / COUNT(*), 1) AS negative_pct,
    ROUND(COUNT_IF(sentiment = 'neutral')  * 100.0 / COUNT(*), 1) AS neutral_pct,
    ROUND(COUNT_IF(sentiment = 'mixed')    * 100.0 / COUNT(*), 1) AS mixed_pct
FROM sentiments
GROUP BY aspect
ORDER BY negative_pct DESC;


-- ============================================================
-- 5. AI_EXTRACT — メニュー名と不満点を構造化抽出
-- ============================================================
-- AI_EXTRACT はテキストから指定フィールドを JSON で抽出する。
-- AI_COMPLETE と異なり、テキスト内の情報を「そのまま」取り出す用途に最適。

SELECT
    review,
    AI_EXTRACT(
        review,
        {'menu_items': 'メニューアイテム名のリスト', 'complaint': '具体的な不満点', 'recommendation': '再来店するかどうか'}
    ) AS extracted
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL
LIMIT 10;


-- ============================================================
-- 6. AI_COMPLETE（構造化出力）— 改善提案の生成
-- ============================================================
-- AI_COMPLETE + response_format で、LLM に推論・要約させた結果を
-- 決まった JSON スキーマで受け取れる。AI_EXTRACT が「抜き出し」なのに対し、
-- AI_COMPLETE は「生成・推論」が可能。

SELECT
    review,
    AI_COMPLETE(
        'claude-sonnet-4-6',
        '以下のフードトラックレビューを分析し、改善点と良い点と具体的なアクションを日本語で回答してください。\n\n' || review,
        response_format => {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
                    'complaint':          {'type': 'string', 'description': '改善が必要な点。なければ「なし」'},
                    'praise':             {'type': 'string', 'description': '良い点・称賛。なければ「なし」'},
                    'recommended_action': {'type': 'string', 'description': '店舗が取るべき具体的アクション1つ'}
                },
                'required': ['complaint', 'praise', 'recommended_action']
            }
        }
    )::VARIANT AS analysis
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL
LIMIT 5;


-- ============================================================
-- 7. AI_AGG — 経営層向けエグゼクティブサマリーの生成
-- ============================================================
-- AI_AGG は複数行のテキストを LLM で集約する関数。
-- コンテキストウィンドウを超えるデータにも対応（自動チャンク処理）。
-- プロンプトで出力フォーマットを明示すると回答が安定する。

SELECT
    AI_AGG(
        review,
        'あなたは飲食コンサルタントです。以下のレビュー群を分析し、Smoky BBQ 東京店への改善提案を作成してください。

必ず以下の形式で出力し、前置きや説明文は不要です:

【顧客満足度の現状】
（1〜2文で全体の傾向を要約）

【改善すべき点 トップ3】
1. [改善点1]
2. [改善点2]
3. [改善点3]

【強みとして維持すべき点】
- [強み1]
- [強み2]

【推奨アクション】
（最も優先度の高い施策を1つ、具体的に記述）'
    ) AS executive_summary
FROM harmonized.truck_reviews_v
WHERE language = 'ja'
  AND truck_brand_name = 'Smoky BBQ'
  AND review IS NOT NULL;
