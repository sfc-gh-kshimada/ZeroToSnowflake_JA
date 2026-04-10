/*************************************************************************************************** 
Asset:        Zero to Snowflake - AI SQL 関数
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

AI SQL 関数
1. SENTIMENT() を使用してトラックの顧客レビューをポジティブ・ネガティブ・ニュートラルにスコアリングおよびラベリングする
2. AI_CLASSIFY() を使用して食品品質やサービス体験などのテーマでレビューを分類する
3. EXTRACT_ANSWER() を使用してレビューテキストから具体的な苦情や称賛を抽出する
4. AI_SUMMARIZE_AGG() を使用してトラックブランド名ごとの顧客センチメントのクイックサマリーを生成する

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "aisql_functions"}}';

/*
    ここでは TastyBytes のデータアナリストの役割を担い、AI SQL 関数を活用して
    顧客レビューからインサイトを得ることを目的として、コンテキストを適切に設定しましょう。
*/

USE ROLE tb_analyst;
USE DATABASE tb_101;
USE WAREHOUSE tb_analyst_wh;

/* 1. 大規模なセンチメント分析
    ***************************************************************
    すべてのフードトラックブランドにわたる顧客センチメントを分析して、
    どのトラックが最もパフォーマンスが高いかを特定し、
    フリート全体の顧客満足度指標を作成します。
    Cortex Playground では個別のレビューを手動で分析しました。
    次に SENTIMENT() 関数を使用して、Snowflake の公式センチメント範囲に従い、
    顧客レビューを -1（ネガティブ）から +1（ポジティブ）で自動的にスコアリングします。
    ***************************************************************/

-- ビジネス上の質問: 「各トラックブランドに対して顧客はどのように感じているか？」
-- このクエリを実行して、フードトラックネットワーク全体の顧客センチメントを分析し、フィードバックを分類する

SELECT
    truck_brand_name,
    COUNT(*) AS total_reviews,
    AVG(CASE WHEN sentiment >= 0.5 THEN sentiment END) AS avg_positive_score,
    AVG(CASE WHEN sentiment BETWEEN -0.5 AND 0.5 THEN sentiment END) AS avg_neutral_score,
    AVG(CASE WHEN sentiment <= -0.5 THEN sentiment END) AS avg_negative_score
FROM (
    SELECT
        truck_brand_name,
        SNOWFLAKE.CORTEX.SENTIMENT (review) AS sentiment
    FROM harmonized.truck_reviews_v
    WHERE
        language ILIKE '%en%'
        AND review IS NOT NULL
    LIMIT 10000
)
GROUP BY
    truck_brand_name
ORDER BY total_reviews DESC;

/*
    重要なインサイト:
        Cortex Playground での1件ずつの分析から、数千件の体系的な処理への移行に注目してください。
        SENTIMENT() 関数は自動的にすべてのレビューをスコアリングし、
        ポジティブ・ネガティブ・ニュートラルに分類することで、
        フリート全体の顧客満足度指標を瞬時に提供します。
    センチメントスコアの範囲:
        ポジティブ:   0.5 〜 1
        ニュートラル: -0.5 〜 0.5
        ネガティブ:  -0.5 〜 -1
*/

/* 2. 顧客フィードバックの分類
    ***************************************************************
    次に、顧客がサービスのどの側面について最も話しているかを把握するために、
    すべてのレビューを分類します。AI_CLASSIFY() 関数を使用します。
    この関数は、単純なキーワードマッチングではなく AI の理解に基づいて
    ユーザー定義のカテゴリにレビューを自動的に分類します。
    このステップでは、顧客フィードバックをビジネス上関連する運用エリアに分類し、
    その分布パターンを分析します。
    ***************************************************************/

-- ビジネス上の質問: 「顧客は主に何についてコメントしているか - 食品品質、サービス、または配送体験？」
-- 分類クエリを実行する:

WITH classified_reviews AS (
  SELECT
    truck_brand_name,
    AI_CLASSIFY(
      review,
      ['Food Quality', 'Pricing', 'Service Experience', 'Staff Behavior']
    ):labels[0] AS feedback_category
  FROM
    harmonized.truck_reviews_v
  WHERE
    language ILIKE '%en%'
    AND review IS NOT NULL
    AND LENGTH(review) > 30
  LIMIT
    10000
)
SELECT
  truck_brand_name,
  feedback_category,
  COUNT(*) AS number_of_reviews
FROM
  classified_reviews
GROUP BY
  truck_brand_name,
  feedback_category
ORDER BY
  truck_brand_name,
  number_of_reviews DESC;
                
/*
    重要なインサイト:
        AI_CLASSIFY() が数千件のレビューを「食品品質」「サービス体験」などの
        ビジネス上関連するテーマに自動的に分類したことを観察してください。
        食品品質がすべてのトラックブランドで最も議論されているトピックであることが即座にわかり、
        オペレーションチームに顧客の優先事項について明確で実行可能なインサイトを提供します。
*/

/* 3. 具体的な運用インサイトの抽出
    ***************************************************************
    次に、非構造化テキストから正確な回答を得るために、EXTRACT_ANSWER() 関数を使用します。
    この強力な関数により、顧客フィードバックに対して特定のビジネス上の質問をし、
    直接的な回答を受け取ることができます。
    このステップでは、顧客レビューに記載された具体的な運用上の問題を特定し、
    即座の対応が必要な特定の問題を浮き彫りにすることを目標とします。
    ***************************************************************/

-- ビジネス上の質問: 「各顧客レビュー内にはどのような具体的な運用上の問題やポジティブな言及があるか？」
-- 次のクエリを実行する:

  SELECT
    truck_brand_name,
    primary_city,
    LEFT(review, 100) || '...' AS review_preview,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review,
        'What specific improvement or complaint is mentioned in this review?'
    ) AS specific_feedback
FROM 
    harmonized.truck_reviews_v
WHERE 
    language = 'en'
    AND review IS NOT NULL
    AND LENGTH(review) > 50
ORDER BY truck_brand_name, primary_city ASC
LIMIT 10000;

/*
    重要なインサイト:
        EXTRACT_ANSWER() が長い顧客レビューから具体的で実行可能なインサイトを抽出することに注目してください。
        手動でのレビューの代わりに、この関数は「フレンドリーなスタッフが救いだった」や
        「ホットドッグが完璧に調理されている」などの具体的なフィードバックを自動的に識別します。
        結果として、密度の高いテキストをオペレーションチームが即座に活用できる
        具体的で引用可能なフィードバックに変換します。
*/

/* 4. エグゼクティブサマリーの生成
    ***************************************************************
    最後に、顧客フィードバックの簡潔なサマリーを作成するために SUMMARIZE() 関数を使用します。
    この強力な関数は、長い非構造化テキストから短くまとまったサマリーを生成します。
    このステップでは、各トラックブランドの顧客レビューのエッセンスを
    読みやすいサマリーに抽出し、全体的なセンチメントと重要ポイントの
    クイックオーバービューを提供することを目標とします。
    ***************************************************************/

-- ビジネス上の質問: 「各トラックブランドの主要なテーマと全体的なセンチメントは何か？」
-- サマリークエリを実行する:

SELECT
  truck_brand_name,
  AI_SUMMARIZE_AGG (review) AS review_summary
FROM
  (
    SELECT
      truck_brand_name,
      review
    FROM
      harmonized.truck_reviews_v
    LIMIT
      100
  )
GROUP BY
  truck_brand_name;


/*
  重要なインサイト:
      AI_SUMMARIZE_AGG() 関数は、長いレビューを明確なブランドレベルのサマリーに凝縮します。
      これらのサマリーは繰り返し現れるテーマとセンチメントのトレンドを浮き彫りにし、
      意思決定者に各フードトラックのパフォーマンスのクイックオーバービューを提供することで、
      個々のレビューを読むことなく顧客の認識をより速く理解できるようにします。
*/

/*************************************************************************************************** 
    AI SQL 関数の革新的な力を実際に体験しました。顧客フィードバック分析を、
    個別レビュー処理から体系的な本番規模のインテリジェンスへと転換しました。
    4つのコア関数を通じた探求は、それぞれが異なる分析目的を果たすことを明確に示しており、
    生の顧客の声を包括的なビジネスインテリジェンスへと変換します —
    体系的で、スケーラブルで、即座に実行可能です。
    かつては個別レビュー分析が必要だったものが、今では数秒で何千件ものレビューを処理し、
    データドリブンな運用改善に不可欠な感情的コンテキストと具体的な詳細の両方を提供します。
****************************************************************************************************/
