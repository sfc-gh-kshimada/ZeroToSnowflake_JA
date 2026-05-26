/***************************************************************************************************
Asset:        Zero to Snowflake - AI Functions を利用したレビュー分析
Version:      v1
Copyright(c): 2025 Snowflake Inc. All rights reserved.

目次:
  1. セッションの初期設定
  2. AI_CLASSIFY   — フィードバックカテゴリの分類
  3. AI_SENTIMENT  — アスペクトベースのセンチメント分析
  4. AI_COMPLETE   — 改善点・良い点の構造化抽出
  5. AI_AGG + AI_TRANSLATE — ブランド別エグゼクティブサマリーの生成

前提条件:
  - setup.sql 実行済み
  - ロール tb_analyst、ウェアハウス tb_analyst_wh が利用可能
  - 対象テーブル: TB_101.harmonized.truck_reviews_v
***************************************************************************************************/

-- ============================================================
-- 1. セッションの初期設定
-- ============================================================

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "aisql_functions"}}';
USE ROLE tb_analyst;
USE DATABASE tb_101;
USE WAREHOUSE tb_analyst_wh;


-- ============================================================
-- 2. AI_CLASSIFY — フィードバックカテゴリの分類
-- ============================================================
-- ビジネス上の問い: 顧客は主に何についてコメントしているか？
--
-- AI_CLASSIFY の特徴:
--   - キーワードマッチングではなく AI の理解に基づいてテキストを分類する
--   - 分類カテゴリはユーザーが自由に定義できる
--   - 戻り値は JSON: { "labels": ["Food Quality"], "scores": [0.92] }
--   - :labels[0] → 最上位カテゴリ
--   - :scores[0] → 確信度スコア（0〜1）

-- レビューを 4 カテゴリに自動分類してカテゴリ別の件数を集計する
WITH classified_reviews AS (
  SELECT
    truck_brand_name,
    AI_CLASSIFY(
      review,
      ['Food Quality', 'Pricing', 'Service Experience', 'Staff Behavior']
    ):labels[0] AS feedback_category
  FROM harmonized.truck_reviews_v
  WHERE language ILIKE '%en%'
    AND review IS NOT NULL
    AND LENGTH(review) > 30
  LIMIT 10000
)
SELECT
  feedback_category,
  COUNT(*) AS number_of_reviews
FROM classified_reviews
GROUP BY feedback_category
ORDER BY number_of_reviews DESC;


-- ============================================================
-- 3. AI_SENTIMENT — アスペクトベースのセンチメント分析
-- ============================================================
-- ビジネス上の問い: Food Quality への評価が高いトラックはどこか？
--
-- AI_SENTIMENT の特徴:
--   - 第2引数にアスペクト（観点）を指定することで「何に対してポジティブか」を絞り込める
--   - 戻り値の categories 配列を LATERAL FLATTEN で1行1アスペクトに展開する
--   - センチメント値: "positive" / "negative" / "neutral" / "mixed"
--
-- 戻り値の構造:
-- {
--   "categories": [
--     { "name": "Food Quality", "sentiment": "positive", "score": 0.87 }
--   ]
-- }

-- Food Quality のアスペクト別センチメントを集計し、ポジティブ率ランキングを作成する
WITH with_sentiment AS (
    -- Step 1: AI_SENTIMENT でアスペクトごとのセンチメントを取得
    SELECT
        truck_brand_name,
        AI_SENTIMENT(review, ['Food Quality']) AS sentiment_result
    FROM harmonized.truck_reviews_v
    WHERE language ILIKE '%en%'
        AND review IS NOT NULL
        AND LENGTH(review) > 30
    LIMIT 10000
),
flattened AS (
    -- Step 2: LATERAL FLATTEN で categories 配列を行に展開
    SELECT
        truck_brand_name,
        c.value:sentiment::STRING AS sentiment
    FROM with_sentiment,
        LATERAL FLATTEN(input => sentiment_result:categories) c
    WHERE c.value:name::STRING = 'Food Quality'
),
counts AS (
    -- Step 3: センチメント別に集計
    SELECT
        truck_brand_name,
        COUNT(*) AS total,
        COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) AS positive_count,
        COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) AS negative_count,
        COUNT(CASE WHEN sentiment = 'neutral'  THEN 1 END) AS neutral_count,
        COUNT(CASE WHEN sentiment = 'mixed'    THEN 1 END) AS mixed_count
    FROM flattened
    GROUP BY truck_brand_name
)
SELECT
    RANK() OVER (ORDER BY ROUND(positive_count / total * 100, 1) DESC) AS rank,
    truck_brand_name,
    total AS total_reviews,
    ROUND(positive_count / total * 100, 1) AS positive_pct,
    ROUND(negative_count / total * 100, 1) AS negative_pct,
    ROUND(neutral_count  / total * 100, 1) AS neutral_pct,
    ROUND(mixed_count    / total * 100, 1) AS mixed_pct
FROM counts
ORDER BY rank;


-- ============================================================
-- 4. AI_COMPLETE（構造化出力）— 改善点・良い点の抽出
-- ============================================================
-- ビジネス上の問い: 各レビューの改善点と良い点を日本語でまとめてほしい
--
-- AI_COMPLETE 構造化出力の特徴:
--   - response_format で JSON スキーマを定義すると指定したフィールドが必ず返ってくる
--   - テキストをそのまま抜き出す（AI_EXTRACT）のではなく LLM が推論・要約してフィールドを生成する
--   - シンプルなパスアクセスで値を取得: feedback:complaint::STRING
--
-- 戻り値の構造（response_format 指定時）:
-- { "complaint": "待ち時間が長すぎる", "praise": "スタッフが親切で食事も美味しかった" }

-- レビューを日本語翻訳してから AI_COMPLETE で改善点・良い点を構造化抽出する
WITH translated AS (
    -- Step 1: AI_TRANSLATE でレビューを日本語化する
    SELECT
        truck_brand_name,
        AI_TRANSLATE(review, 'en', 'ja') AS review_ja
    FROM harmonized.truck_reviews_v
    WHERE language = 'en'
        AND review IS NOT NULL
        AND LENGTH(review) > 100
    ORDER BY truck_brand_name, primary_city ASC
    LIMIT 100
),
analyzed AS (
    -- Step 2: AI_COMPLETE で complaint / praise を構造化 JSON として生成する
    SELECT
        truck_brand_name,
        review_ja,
        AI_COMPLETE(
            'claude-sonnet-4-6',
            review_ja,
            response_format => {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'complaint': {'type': 'string', 'description': '改善が必要な点や苦情。なければ「なし」'},
                        'praise':    {'type': 'string', 'description': '良い点や称賛。なければ「なし」'}
                    },
                    'required': ['complaint', 'praise']
                }
            }
        )::VARIANT AS feedback
    FROM translated
)
-- Step 3: JSON パスでカラムとして取り出す
SELECT
    truck_brand_name,
    review_ja,
    feedback:complaint::STRING AS complaint,
    feedback:praise::STRING    AS praise
FROM analyzed;


-- ============================================================
-- 5. AI_AGG + AI_TRANSLATE — エグゼクティブサマリーの生成
-- ============================================================
-- ビジネス上の問い: ブランドごとに改善すべき点を3つ挙げてほしい
--
-- AI_AGG の特徴:
--   - GROUP BY 対象のすべてのテキストを LLM に渡して集約する
--   - プロンプトで出力フォーマットを明示すると回答が安定する
--   - AI_TRANSLATE と組み合わせて多言語出力も可能

-- ブランドごとにレビューを集約し、改善点トップ3を日本語サマリーとして生成する
SELECT
    truck_brand_name,
    AI_TRANSLATE(
        AI_AGG(
            review,
            '改善すべき点を3つ答えてください。必ず以下の形式で出力し、前置きや説明文は不要です。\n- [改善点1]\n- [改善点2]\n- [改善点3]'
        ),
        'en', 'ja'
    ) AS review_summary_ja
FROM (
    SELECT * FROM harmonized.truck_reviews_v
    WHERE language = 'en' AND review IS NOT NULL
    LIMIT 100
)
GROUP BY truck_brand_name;
