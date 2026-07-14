/***************************************************************************************************
Asset:        Zero to Snowflake - Cortex による BI エージェントの構築
Version:      v1
Copyright(c): 2025 Snowflake Inc. All rights reserved.

目次:
  1. セッションの初期設定
  2. Cortex Search Service の作成
  3. Cortex Search テスト（基本検索 / フィルター付き検索）
  4. Cortex Analyst — Semantic View の作成
  5. Cortex Agent の作成

前提条件:
  - setup.sql 実行済み
  - ロール TB_DATA_ENGINEER、ウェアハウス TB_DE_WH が利用可能
  - TB_101.HARMONIZED.TRUCK_REVIEWS_V（Cortex Search ソース）
  - TB_101.SEMANTIC_LAYER.ORDERS_V（Cortex Analyst ソース）
  - TB_101.SEMANTIC_LAYER.CUSTOMER_LOYALTY_METRICS_V（Cortex Analyst ソース）
***************************************************************************************************/

-- ============================================================
-- 1. セッションの初期設定
-- ============================================================

USE ROLE TB_DATA_ENGINEER;
USE DATABASE TB_101;
USE WAREHOUSE TB_DE_WH;


-- ============================================================
-- 2. Cortex Search — レビュー意味検索サービスの構築
-- ============================================================
-- Cortex Search は、テキストをベクトル化してインデックスし、
-- 意味的に近いテキストを検索できるサービスです。
--
-- パラメーター説明:
--   ON          : ベクトル化・インデックス対象カラム
--   ATTRIBUTES  : 検索時に filter で絞り込みに使えるカラム
--   WAREHOUSE   : インデックス構築・更新に使うウェアハウス
--   TARGET_LAG  : ベーステーブルの更新がインデックスに反映されるまでの最大遅延
--   EMBEDDING_MODEL : テキストをベクトル化する埋め込みモデル

-- Cortex Search Service の作成
CREATE OR REPLACE CORTEX SEARCH SERVICE TB_101.HARMONIZED.TASTY_BYTES_REVIEW_SEARCH
  ON REVIEW
  ATTRIBUTES REVIEW_ID, ORDER_ID, TRUCK_ID, LANGUAGE, PRIMARY_CITY, CUSTOMER_ID, DATE, TRUCK_BRAND_NAME
  WAREHOUSE = TB_DE_WH
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT
        REVIEW,
        REVIEW_ID,
        ORDER_ID,
        TRUCK_ID,
        LANGUAGE,
        PRIMARY_CITY,
        CUSTOMER_ID,
        DATE,
        TRUCK_BRAND_NAME
    FROM TB_101.HARMONIZED.TRUCK_REVIEWS_V
    WHERE REVIEW IS NOT NULL
  );


-- ============================================================
-- 3. Cortex Search テスト
-- ============================================================
-- SNOWFLAKE.CORTEX.SEARCH_PREVIEW で検索し、
-- TABLE(FLATTEN(...))['results'] でテーブル形式に展開します。

-- [1] 基本検索: キーワードで関連レビューを意味検索する
SELECT
    r.value['TRUCK_BRAND_NAME']::STRING                 AS truck_brand_name,
    r.value['PRIMARY_CITY']::STRING                     AS primary_city,
    r.value['DATE']::STRING                             AS date,
    r.value['REVIEW']::STRING                           AS review,
    AI_TRANSLATE(r.value['REVIEW']::STRING, 'en', 'ja') AS review_ja
FROM TABLE(FLATTEN(
    PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'TB_101.HARMONIZED.TASTY_BYTES_REVIEW_SEARCH',
            '{
                "query": "best tacos ever",
                "columns": ["REVIEW", "TRUCK_BRAND_NAME", "PRIMARY_CITY", "DATE"],
                "limit": 5
            }'
        )
    )['results']
)) r;

-- [2] フィルター付き検索: PRIMARY_CITY = 'Seattle' に絞り込み
--     @eq = 完全一致  |  @contains = 部分一致
SELECT
    r.value['TRUCK_BRAND_NAME']::STRING                      AS truck_brand_name,
    r.value['PRIMARY_CITY']::STRING                          AS primary_city,
    r.value['REVIEW']::STRING                                AS review,
    AI_TRANSLATE(r.value['REVIEW']::STRING, 'en', 'ja')      AS review_ja
FROM TABLE(FLATTEN(
    PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'TB_101.HARMONIZED.TASTY_BYTES_REVIEW_SEARCH',
            '{
                "query": "staff behavior was bad",
                "columns": ["REVIEW", "TRUCK_BRAND_NAME", "PRIMARY_CITY"],
                "filter": {"@eq": {"PRIMARY_CITY": "Seattle"}},
                "limit": 5
            }'
        )
    )['results']
)) r;


-- ============================================================
-- 4. Cortex Analyst — Semantic View の構築
-- ============================================================
-- Cortex Analyst は自然言語の質問を SQL に自動変換して実行するサービスです。
-- Semantic View はデータの「意味」を定義するオブジェクトで、
-- テーブル・リレーション・メトリクス・AI 生成ルールを一体で管理します。
--
-- Semantic View の構成要素:
--   TABLES           : 分析対象テーブルとエイリアス定義
--   RELATIONSHIPS    : テーブル間の結合条件
--   FACTS            : 集計対象となる数値カラム
--   DIMENSIONS       : フィルタ・グループ化に使う属性カラム
--   METRICS          : よく使う集計の定義（SUM / AVG / COUNT など）
--   AI_SQL_GENERATION: SQL 生成時の挙動を制御するカスタム指示

-- Semantic View の作成
CREATE OR REPLACE SEMANTIC VIEW TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS

    TABLES (
        ORDERS AS TB_101.SEMANTIC_LAYER.ORDERS_V
            WITH SYNONYMS = ('注文', '売上', 'sales', 'order data')
            COMMENT = '注文データ。トラックブランド・メニュー・都市・顧客属性・金額・数量などを含む Tasty Bytes 売上分析の起点テーブル。1行＝1注文明細（ORDER_DETAIL_ID が一意）。',
        CUSTOMER_LOYALTY AS TB_101.SEMANTIC_LAYER.CUSTOMER_LOYALTY_METRICS_V
            PRIMARY KEY (CUSTOMER_ID)
            WITH SYNONYMS = ('顧客ロイヤルティ', '顧客', 'customer loyalty', 'customer')
            COMMENT = '顧客ロイヤルティ指標。顧客ごとの累計売上・居住都市・訪問ロケーション履歴を含む。'
    )

    RELATIONSHIPS (
        ORDERS_TO_LOYALTY AS ORDERS(CUSTOMER_ID) REFERENCES CUSTOMER_LOYALTY(CUSTOMER_ID)
    )

    FACTS (
        ORDERS.ORDER_TOTAL AS ORDER_TOTAL
            COMMENT = '注文の合計金額（USD）',
        ORDERS.QUANTITY AS QUANTITY
            COMMENT = '注文内のアイテム数量',
        CUSTOMER_LOYALTY.TOTAL_SALES AS TOTAL_SALES
            COMMENT = '顧客の累計売上金額（USD）'
    )

    DIMENSIONS (
        ORDERS.FRANCHISE_FLAG AS FRANCHISE_FLAG
            WITH SYNONYMS = ('フランチャイズ', 'FC', 'franchise')
            COMMENT = 'フランチャイズ店舗かどうか（1=フランチャイズ、0=直営）',
        ORDERS.ORDER_ID AS ORDER_ID
            COMMENT = '注文の一意識別子',
        ORDERS.ORDER_DETAIL_ID AS ORDER_DETAIL_ID
            COMMENT = '注文明細の一意識別子（1注文に複数明細が存在する）',
        ORDERS.ORDER_CUSTOMER_ID AS CUSTOMER_ID
            WITH SYNONYMS = ('顧客ID', 'customer id')
            COMMENT = '注文を行った顧客 ID（注文テーブル側）',
        ORDERS.TRUCK_ID AS TRUCK_ID
            COMMENT = '注文を担当したトラックの一意識別子',
        ORDERS.TRUCK_BRAND_NAME AS TRUCK_BRAND_NAME
            WITH SYNONYMS = ('トラックブランド', 'ブランド', 'brand', 'truck brand')
            COMMENT = 'フードトラックのブランド名（Freezing Point, Smoky BBQ, Guac n Roll, Peking Truck, Kitakata Ramen Bar 等 全15ブランド）',
        ORDERS.MENU_ITEM_ID AS MENU_ITEM_ID
            COMMENT = 'メニューアイテムの一意識別子',
        ORDERS.MENU_ITEM_NAME AS MENU_ITEM_NAME
            WITH SYNONYMS = ('メニュー名', '料理名', 'dish name')
            COMMENT = '注文されたメニューアイテム名（Tonkotsu Ramen, Sugar Cone, Pastrami, Three Taco Combo Plate 等 全58種）',
        ORDERS.MENU_TYPE AS MENU_TYPE
            WITH SYNONYMS = ('メニュータイプ', 'カテゴリ', 'cuisine')
            COMMENT = 'メニューの種類（BBQ, Chinese, Crepes, Ethiopian, Grilled Cheese, Gyros, Hot Dogs, Ice Cream, Indian, Mac & Cheese, Poutine, Ramen, Sandwiches, Tacos, Vegetarian）',
        ORDERS.PRIMARY_CITY AS PRIMARY_CITY
            WITH SYNONYMS = ('都市', '市', 'city')
            COMMENT = '注文が発生した主要都市名（Tokyo, New York City, Paris, London, Seoul, Sydney 等 全30都市）',
        ORDERS.REGION AS REGION
            WITH SYNONYMS = ('地域', '州', 'state', 'region')
            COMMENT = '注文が発生した地域・州（Kantō, California, Île-de-France, Greater London 等 全30地域）',
        ORDERS.ORDER_COUNTRY AS COUNTRY
            WITH SYNONYMS = ('国', 'country')
            COMMENT = '注文が発生した国（Japan, United States, France, England, Brazil, South Korea, India 等 全15カ国）',
        ORDERS.GENDER AS GENDER
            WITH SYNONYMS = ('性別', 'sex')
            COMMENT = '注文を行った顧客の性別（Male / Female / Undisclosed）',
        ORDERS.MARITAL_STATUS AS MARITAL_STATUS
            WITH SYNONYMS = ('婚姻状況', '結婚', 'marriage status')
            COMMENT = '注文を行った顧客の婚姻状況（Single / Married / Divorced/Seperated / Undisclosed）',
        ORDERS.FRANCHISE_ID AS FRANCHISE_ID
            COMMENT = '注文を受けたフランチャイズの一意識別子',
        ORDERS.LOCATION_ID AS LOCATION_ID
            WITH SYNONYMS = ('ロケーション', '場所', 'location')
            COMMENT = '注文が発生したロケーションの一意識別子',
        ORDERS.ORDER_DATE AS ORDER_DATE
            WITH SYNONYMS = ('注文日', '日付', 'date')
            COMMENT = '注文日（2019-01-01 〜 2022-11-01）',
        ORDERS.ORDER_YEAR AS YEAR(ORDER_DATE)
            WITH SYNONYMS = ('年', 'year')
            COMMENT = '注文年（2019〜2022）',
        ORDERS.ORDER_MONTH AS MONTH(ORDER_DATE)
            WITH SYNONYMS = ('月', 'month')
            COMMENT = '注文月（1〜12）',
        ORDERS.ORDER_QUARTER AS QUARTER(ORDER_DATE)
            WITH SYNONYMS = ('四半期', 'quarter', 'Q')
            COMMENT = '注文四半期（1〜4）',
        ORDERS.ORDER_YEAR_MONTH AS TO_CHAR(ORDER_DATE, 'YYYY-MM')
            WITH SYNONYMS = ('年月', 'year-month')
            COMMENT = '注文の年月（YYYY-MM形式、月別トレンド分析用）',
        CUSTOMER_LOYALTY.LOYALTY_CUSTOMER_ID AS CUSTOMER_ID
            WITH SYNONYMS = ('ロイヤルティ顧客ID', 'loyalty customer id')
            COMMENT = '顧客ロイヤルティの顧客 ID',
        CUSTOMER_LOYALTY.CUSTOMER_CITY AS CITY
            WITH SYNONYMS = ('居住都市', '顧客の都市', 'customer city')
            COMMENT = '顧客の居住都市',
        CUSTOMER_LOYALTY.CUSTOMER_COUNTRY AS COUNTRY
            WITH SYNONYMS = ('居住国', '顧客の国', 'customer country')
            COMMENT = '顧客の居住国',
        CUSTOMER_LOYALTY.VISITED_LOCATION_COUNT AS ARRAY_SIZE(VISITED_LOCATION_IDS_ARRAY)
            COMMENT = '顧客が訪問したユニークロケーション数'
    )

    METRICS (
        ORDERS.TOTAL_REVENUE AS SUM(ORDERS.ORDER_TOTAL)
            WITH SYNONYMS = ('総売上', '売上合計', 'total sales', 'revenue', '売上高')
            COMMENT = '注文合計金額の総売上',
        ORDERS.TOTAL_ORDERS AS COUNT(DISTINCT ORDERS.ORDER_ID)
            WITH SYNONYMS = ('注文件数', '注文数', 'order count', '受注件数')
            COMMENT = '注文の総件数（ユニーク注文数。1注文に複数明細が含まれるため DISTINCT でカウント）',
        ORDERS.TOTAL_ORDER_DETAILS AS COUNT(ORDERS.ORDER_DETAIL_ID)
            WITH SYNONYMS = ('明細件数', 'line item count', '注文明細数')
            COMMENT = '注文明細の総件数（メニューアイテム単位のカウント）',
        ORDERS.TOTAL_ITEMS AS SUM(ORDERS.QUANTITY)
            WITH SYNONYMS = ('総数量', '総アイテム数', 'total quantity')
            COMMENT = '注文アイテムの総数量',
        ORDERS.AVG_ORDER_VALUE AS AVG(ORDERS.ORDER_TOTAL)
            WITH SYNONYMS = ('平均注文額', '客単価', 'AOV', 'average order value')
            COMMENT = '注文1件あたりの平均金額',
        ORDERS.AVG_QUANTITY AS AVG(ORDERS.QUANTITY)
            COMMENT = '注文1件あたりの平均アイテム数',
        ORDERS.MAX_ORDER_VALUE AS MAX(ORDERS.ORDER_TOTAL)
            WITH SYNONYMS = ('最大注文額', 'max order')
            COMMENT = '最大注文金額',
        ORDERS.MIN_ORDER_VALUE AS MIN(ORDERS.ORDER_TOTAL)
            WITH SYNONYMS = ('最小注文額', 'min order')
            COMMENT = '最小注文金額',
        ORDERS.UNIQUE_CUSTOMERS AS COUNT(DISTINCT ORDERS.CUSTOMER_ID)
            WITH SYNONYMS = ('ユニーク顧客数', '顧客数', 'customer count', 'unique customers')
            COMMENT = 'ユニーク顧客数',
        ORDERS.UNIQUE_TRUCKS AS COUNT(DISTINCT ORDERS.TRUCK_ID)
            WITH SYNONYMS = ('トラック数', '稼働トラック数', 'truck count')
            COMMENT = '稼働したユニークトラック数',
        ORDERS.UNIQUE_LOCATIONS AS COUNT(DISTINCT ORDERS.LOCATION_ID)
            WITH SYNONYMS = ('ロケーション数', '出店場所数')
            COMMENT = '注文が発生したユニークロケーション数',
        ORDERS.UNIQUE_MENU_ITEMS AS COUNT(DISTINCT ORDERS.MENU_ITEM_NAME)
            WITH SYNONYMS = ('メニュー数', 'menu item count')
            COMMENT = '販売されたユニークメニューアイテム数',
        ORDERS.MALE_CUSTOMERS AS COUNT(DISTINCT IFF(ORDERS.GENDER = 'Male', ORDERS.CUSTOMER_ID, NULL))
            WITH SYNONYMS = ('男性顧客数', 'male customers')
            COMMENT = '男性のユニーク顧客数',
        ORDERS.FEMALE_CUSTOMERS AS COUNT(DISTINCT IFF(ORDERS.GENDER = 'Female', ORDERS.CUSTOMER_ID, NULL))
            WITH SYNONYMS = ('女性顧客数', 'female customers')
            COMMENT = '女性のユニーク顧客数',
        ORDERS.FRANCHISE_REVENUE AS SUM(IFF(ORDERS.FRANCHISE_FLAG = 1, ORDERS.ORDER_TOTAL, 0))
            WITH SYNONYMS = ('フランチャイズ売上', 'franchise revenue')
            COMMENT = 'フランチャイズ店舗の売上合計',
        ORDERS.DIRECT_REVENUE AS SUM(IFF(ORDERS.FRANCHISE_FLAG = 0, ORDERS.ORDER_TOTAL, 0))
            WITH SYNONYMS = ('直営売上', 'direct revenue', 'corporate revenue')
            COMMENT = '直営店舗の売上合計',
        CUSTOMER_LOYALTY.TOTAL_CUSTOMER_SALES AS SUM(CUSTOMER_LOYALTY.TOTAL_SALES)
            WITH SYNONYMS = ('顧客累計売上合計', 'total lifetime sales')
            COMMENT = '全顧客の累計売上合計',
        CUSTOMER_LOYALTY.AVG_CUSTOMER_SALES AS AVG(CUSTOMER_LOYALTY.TOTAL_SALES)
            WITH SYNONYMS = ('顧客平均累計売上', 'average lifetime value', 'LTV')
            COMMENT = '顧客1人あたりの平均累計売上',
        CUSTOMER_LOYALTY.LOYALTY_CUSTOMER_COUNT AS COUNT(CUSTOMER_LOYALTY.CUSTOMER_ID)
            WITH SYNONYMS = ('ロイヤルティ顧客数', 'loyalty member count')
            COMMENT = 'ロイヤルティプログラムの顧客数',
        CUSTOMER_LOYALTY.AVG_VISITED_LOCATIONS AS AVG(ARRAY_SIZE(CUSTOMER_LOYALTY.VISITED_LOCATION_IDS_ARRAY))
            WITH SYNONYMS = ('平均訪問ロケーション数', 'avg locations visited')
            COMMENT = '顧客1人あたりの平均訪問ロケーション数',
        ORDERS.REVENUE_PER_CUSTOMER AS ORDERS.TOTAL_REVENUE / NULLIF(ORDERS.UNIQUE_CUSTOMERS, 0)
            WITH SYNONYMS = ('顧客当たり売上', '顧客単価', 'revenue per customer', 'per customer revenue')
            COMMENT = '顧客1人当たりの平均売上金額（TOTAL_REVENUE ÷ UNIQUE_CUSTOMERS）',
        ORDERS.ITEMS_PER_ORDER AS ORDERS.TOTAL_ITEMS / NULLIF(ORDERS.TOTAL_ORDERS, 0)
            WITH SYNONYMS = ('バスケットサイズ', '注文当たり数量', 'basket size', 'items per order')
            COMMENT = '注文1件当たりの平均アイテム数（TOTAL_ITEMS ÷ TOTAL_ORDERS）',
        ORDERS.REVENUE_PER_TRUCK AS ORDERS.TOTAL_REVENUE / NULLIF(ORDERS.UNIQUE_TRUCKS, 0)
            WITH SYNONYMS = ('トラック当たり売上', 'revenue per truck', 'truck revenue')
            COMMENT = 'フードトラック1台当たりの平均売上（TOTAL_REVENUE ÷ UNIQUE_TRUCKS）',
        ORDERS.REVENUE_PER_LOCATION AS ORDERS.TOTAL_REVENUE / NULLIF(ORDERS.UNIQUE_LOCATIONS, 0)
            WITH SYNONYMS = ('ロケーション当たり売上', 'revenue per location', 'location revenue')
            COMMENT = 'ロケーション1拠点当たりの平均売上（TOTAL_REVENUE ÷ UNIQUE_LOCATIONS）'
    )

    COMMENT = 'Tasty Bytes エグゼクティブアナリティクス用セマンティックビュー。注文データと顧客ロイヤルティデータを統合し、売上・注文・顧客行動を自然言語でクエリ可能。'

    AI_SQL_GENERATION $$
このセマンティックビューは Tasty Bytes フードトラックビジネスの売上・注文・顧客分析用です。以下のルールに従って SQL を生成してください。

数値フォーマット:
- 金額は小数点第2位まで丸めること（ROUND(..., 2)）。
- 件数・数量は整数で表示すること。

デフォルト設定:
- 特に期間の指定がない場合は全期間のデータを対象にすること。
- TOP N の指定がない場合はデフォルトで上位 10 件を返すこと。

時間軸のルール:
- 年・月・四半期での集計には ORDER_YEAR / ORDER_MONTH / ORDER_QUARTER を使用すること。
- 年月での集計やトレンド分析には ORDER_YEAR_MONTH を使用すること。
- 直近 N 期間のような相対期間は ORDER_DATE に対して DATEADD / DATEDIFF を使用すること。

ランキング・集計のルール:
- ランキング系の質問では合計値や件数の降順でソートすること。
- 時系列の質問では ORDER_DATE の昇順でソートすること。

注文件数に関する注意:
- 注文件数を求められた場合は TOTAL_ORDERS（COUNT DISTINCT ORDER_ID）を使用すること。
- 明細件数（メニューアイテム単位）を求められた場合は TOTAL_ORDER_DETAILS を使用すること。
- このテーブルは注文明細レベル（1行＝1メニューアイテム）のため、単純な COUNT では注文明細件数になることに注意。

フランチャイズ分析のルール:
- フランチャイズ店舗の分析には FRANCHISE_FLAG = 1 でフィルタすること。
- 直営店舗の分析には FRANCHISE_FLAG = 0 でフィルタすること。

対応できない質問:
- レビュー・感情分析に関する質問は「Cortex Search を使用してください」と案内すること。
- 個人情報の詳細（氏名・住所など）に関する質問には「お答えできません」と回答すること。

曖昧な質問への対応:
- 「売上を分析して」のような曖昧な質問にはトラックブランド別の総売上と注文件数を返すこと。
- 「人気メニュー」のような質問には注文件数と総売上の両方を返すこと。

データ範囲:
- このデータセットには 2019年1月1日〜2022年11月1日 のデータのみ含まれる。範囲外の年に関する対応は AI_QUESTION_CATEGORIZATION 側のルールに従うこと。
$$

    AI_QUESTION_CATEGORIZATION $$
質問カテゴリの判別ルール:

(1) 売上・注文件数・客単価・数量・集計に関する質問 → SQL を生成して回答
(2) レビュー・口コミ・感想・評判に関する質問 → 「この質問はレビューデータの検索が必要です。Cortex Search を使用してください。」と回答
(3) 個人情報（氏名・住所・電話番号・メールアドレスなど）に関する質問 → 「個人情報に関する質問にはお答えできません。」と回答
(4) 「おすすめ」「どうすべきか」などの意思決定を求める質問 → データに基づく分析結果を提供し、意思決定はユーザーに委ねる旨を付記する
(5) 2023年以降または2018年以前の特定年を明示的に含む質問（例:「2024年の売上」「2025年のデータ」）
    → SQL を生成せず「申し訳ありませんが、[指定年] のデータはこのデータセットには含まれていません。
      利用可能なデータは 2019年1月〜2022年11月 です。
      この期間のデータで分析しますか？」と日本語で返答し、代替分析を提案する
(6) 期間が曖昧な質問（「最近の売上」「今年の売上」等）→ 全期間のデータを使用して回答する旨を明記する
(7) 特定のブランド名が不正確な場合 → 類似するブランド名を提示して確認を求める
$$

    AI_VERIFIED_QUERIES (
        top_brands_by_revenue AS (
            QUESTION '売上上位のトラックブランドは？'
            ONBOARDING_QUESTION TRUE
            SQL 'SELECT * FROM SEMANTIC_VIEW(
                TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS
                DIMENSIONS ORDERS.TRUCK_BRAND_NAME
                METRICS ORDERS.TOTAL_REVENUE, ORDERS.TOTAL_ORDERS
            ) ORDER BY TOTAL_REVENUE DESC LIMIT 10'
        ),
        revenue_by_year AS (
            QUESTION '年別の売上推移は？'
            ONBOARDING_QUESTION TRUE
            SQL 'SELECT * FROM SEMANTIC_VIEW(
                TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS
                DIMENSIONS ORDERS.ORDER_YEAR
                METRICS ORDERS.TOTAL_REVENUE, ORDERS.TOTAL_ORDERS
            ) ORDER BY ORDER_YEAR'
        ),
        top_cities_by_orders AS (
            QUESTION '注文件数が多い都市トップ10は？'
            ONBOARDING_QUESTION TRUE
            SQL 'SELECT * FROM SEMANTIC_VIEW(
                TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS
                DIMENSIONS ORDERS.PRIMARY_CITY
                METRICS ORDERS.TOTAL_ORDERS, ORDERS.TOTAL_REVENUE
            ) ORDER BY TOTAL_ORDERS DESC LIMIT 10'
        ),
        loyalty_ltv_by_country AS (
            QUESTION 'ロイヤルティ会員の居住国別の平均 LTV は？'
            ONBOARDING_QUESTION TRUE
            SQL 'SELECT * FROM SEMANTIC_VIEW(
                TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS
                DIMENSIONS CUSTOMER_LOYALTY.CUSTOMER_COUNTRY
                METRICS CUSTOMER_LOYALTY.AVG_CUSTOMER_SALES, CUSTOMER_LOYALTY.LOYALTY_CUSTOMER_COUNT
            ) ORDER BY AVG_CUSTOMER_SALES DESC'
        )
    )

    COPY GRANTS;


-- ============================================================
-- 5. Cortex Agent — BI エージェントの構築
-- ============================================================
-- Cortex Agent は複数のツール（Cortex Search・Cortex Analyst など）を
-- 組み合わせて自律的に動作する AI エージェントです。
--
-- ツール構成:
--   SALES_ANALYST  (cortex_analyst_text_to_sql) : 売上・注文件数などの数値分析
--   REVIEW_SEARCH  (cortex_search)              : レビュー本文・口コミの意味検索

-- Cortex Agent の作成（Cortex Search + Cortex Analyst を束ねた BI エージェント）
CREATE OR REPLACE AGENT TB_101.SEMANTIC_LAYER.TASTY_BYTES_BI_AGENT
    COMMENT = 'Tasty Bytes フードトラックビジネスのレビュー検索と売上分析を横断的に行う BI エージェント'
FROM SPECIFICATION $$
{
  "instructions": {
    "response": "あなたは Tasty Bytes フードトラックビジネスの高度な分析アシスタントです。2つの専門ツールを使い分けて、ユーザーの質問に日本語で丁寧に回答してください。数値データは適切にフォーマットしてください（金額はカンマ区切り、比率は%表記、平均値は小数点第2位まで）。分析結果には根拠となるデータを含めてください。複数のツールの結果を組み合わせて、包括的な回答を提供してください。",
    "orchestration": "ツールの使い分けルール:\n\n(1) 顧客レビューの内容検索・口コミ確認・特定の感想を探す\n    → REVIEW_SEARCH を使用\n\n(2) 売上・注文件数・客単価・顧客数などの数値分析\n    → SALES_ANALYST を使用\n\n複合的な質問の場合:\nまず REVIEW_SEARCH で口コミを確認し、次に SALES_ANALYST で数値を分析してください。\n\n例1: 「人気のトラックブランドとそのレビューを教えて」\n  → SALES_ANALYST でブランド別売上ランキングを取得\n  → REVIEW_SEARCH で上位ブランドのレビューを検索\n\n例2: 「食べ物が美味しいと評判のトラックの売上は？」\n  → REVIEW_SEARCH で「美味しい」関連レビューを検索\n  → SALES_ANALYST でそのブランドの売上を分析",
    "sample_questions": [
      {"question": "売上上位のトラックブランドを教えてください。"},
      {"question": "食べ物が美味しいと評判のトラックはどこですか？"},
      {"question": "都市別の注文件数と売上を比較してください。"},
      {"question": "サービスが良いというレビューが多いブランドはどこですか？"},
      {"question": "月別の売上トレンドを教えてください。"},
      {"question": "フランチャイズと直営で売上に差はありますか？"}
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "SALES_ANALYST",
        "description": "Tasty Bytes の売上・注文・顧客データの数値分析ツール。売上金額の集計、注文件数、客単価、トラックブランド別・メニュー別・都市別・国別の売上ランキング、時系列トレンド、フランチャイズ vs 直営比較、顧客属性別の購買傾向分析に使用します。"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "REVIEW_SEARCH",
        "description": "Tasty Bytes のレビュー本文の意味検索ツール。特定のトラックブランド・食事体験・サービス・雰囲気に関する口コミや感想を探したい場合に使用します。"
      }
    }
  ],
  "tool_resources": {
    "SALES_ANALYST": {
      "semantic_view": "TB_101.SEMANTIC_LAYER.TASTY_BYTES_BUSINESS_ANALYTICS",
      "execution_environment": {
        "type": "warehouse",
        "warehouse": "TB_CORTEX_WH"
      }
    },
    "REVIEW_SEARCH": {
      "name": "TB_101.HARMONIZED.TASTY_BYTES_REVIEW_SEARCH",
      "max_results": 10
    }
  }
}
$$;
