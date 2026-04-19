/***************************************************************************************************       
Asset:        Zero to Snowflake - セットアップ
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************/

USE ROLE sysadmin;

-- セッションにクエリタグを設定する
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "setup"}}';

/*--
 • データベース、スキーマ、ウェアハウスの作成
--*/

-- tb_101 データベースを作成する
CREATE OR REPLACE DATABASE tb_101;

-- raw_pos スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.raw_pos;

-- raw_customer スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.raw_customer;

-- harmonized スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.harmonized;

-- analytics スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.analytics;

-- governance スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.governance;

-- raw_support スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.raw_support;

-- セマンティックレイヤー用スキーマを作成する
CREATE OR REPLACE SCHEMA tb_101.semantic_layer
COMMENT = 'ビジネス向けセマンティックレイヤー用スキーマ。分析利用に最適化されています。';

-- ウェアハウスを作成する
CREATE OR REPLACE WAREHOUSE tb_de_wh
    WAREHOUSE_SIZE = 'large' -- 初回データロード用にLarge。このスクリプトの最後でXSmallにスケールダウンします
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Tasty Bytes データエンジニアリング用ウェアハウス';

CREATE OR REPLACE WAREHOUSE tb_dev_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Tasty Bytes 開発者用ウェアハウス';

-- アナリスト用ウェアハウスを作成する
CREATE OR REPLACE WAREHOUSE tb_analyst_wh
    COMMENT = 'TastyBytes アナリスト用ウェアハウス'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'large'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true,
    AUTO_RESUME = true;

-- 分析ワークロード専用のラージウェアハウスを作成する
CREATE OR REPLACE WAREHOUSE tb_cortex_wh
    WAREHOUSE_SIZE = 'LARGE'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Cortex Analyst およびその他の分析ツール専用のラージウェアハウス。';

-- ロールを作成する
USE ROLE securityadmin;

-- 機能ロール
CREATE ROLE IF NOT EXISTS tb_admin
    COMMENT = 'Tasty Bytes 管理者';
    
CREATE ROLE IF NOT EXISTS tb_data_engineer
    COMMENT = 'Tasty Bytes データエンジニア';
    
CREATE ROLE IF NOT EXISTS tb_dev
    COMMENT = 'Tasty Bytes 開発者';
    
CREATE ROLE IF NOT EXISTS tb_analyst
    COMMENT = 'Tasty Bytes アナリスト';
    
-- ロール階層
GRANT ROLE tb_admin TO ROLE sysadmin;
GRANT ROLE tb_data_engineer TO ROLE tb_admin;
GRANT ROLE tb_dev TO ROLE tb_data_engineer;
GRANT ROLE tb_analyst TO ROLE tb_data_engineer;

-- 権限付与
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE tb_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tb_admin;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON DATABASE tb_101 TO ROLE tb_dev;

GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_support TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.analytics TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.governance TO ROLE tb_dev;

GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_admin;
GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_data_engineer;
GRANT ALL ON SCHEMA tb_101.semantic_layer TO ROLE tb_dev;

-- ウェアハウス権限付与
GRANT OWNERSHIP ON WAREHOUSE tb_de_wh TO ROLE tb_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_de_wh TO ROLE tb_data_engineer;

GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_dev_wh TO ROLE tb_dev;

GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_analyst_wh TO ROLE tb_dev;

GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_admin;
GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_data_engineer;
GRANT ALL ON WAREHOUSE tb_cortex_wh TO ROLE tb_dev;

-- 将来の権限付与（FUTURE GRANTS）
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_pos TO ROLE tb_dev;

GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_admin;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE TABLES IN SCHEMA tb_101.raw_customer TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.harmonized TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.analytics TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.governance TO ROLE tb_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA tb_101.semantic_layer TO ROLE tb_dev;

-- マスキングポリシー権限の付与
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_admin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE tb_data_engineer;
  
-- tb_admin への権限付与
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE tb_admin;

-- tb_analyst への権限付与
GRANT ALL ON SCHEMA harmonized TO ROLE tb_analyst;
GRANT ALL ON SCHEMA analytics TO ROLE tb_analyst;
GRANT OPERATE, USAGE ON WAREHOUSE tb_analyst_wh TO ROLE tb_analyst;

-- Cortex Search サービスへの権限付与
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE TB_DEV;
GRANT USAGE ON SCHEMA TB_101.HARMONIZED TO ROLE TB_DEV;
GRANT USAGE ON WAREHOUSE TB_DE_WH TO ROLE TB_DEV;


-- raw_pos テーブルの作成
USE ROLE sysadmin;
USE WAREHOUSE tb_de_wh;

/*--
 • ファイルフォーマットとステージの作成
--*/

CREATE OR REPLACE FILE FORMAT tb_101.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tb_101.public.s3load
COMMENT = 'Quickstarts S3 ステージ接続'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tb_101.public.csv_ff;

CREATE OR REPLACE STAGE tb_101.public.truck_reviews_s3load
COMMENT = 'トラックレビュー用ステージ'
url = 's3://sfquickstarts/tastybytes-voc/'
file_format = tb_101.public.csv_ff;

-- このステージはYAMLファイルのアップロードに使用します。
CREATE OR REPLACE STAGE tb_101.semantic_layer.semantic_model_stage
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Cortex Analyst セマンティックモデルのYAMLファイルをアップロードするための内部ステージ。';

/*--
 RAW ゾーン テーブルの作成
--*/

-- country テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- franchise テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

-- location テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- menu テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- truck テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_header テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

-- order_detail テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- customer_loyalty テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

/*--
 raw_support ゾーン テーブルの作成
--*/
CREATE OR REPLACE TABLE tb_101.raw_support.truck_reviews
(
    order_id NUMBER(38,0),
    language VARCHAR(16777216),
    source VARCHAR(16777216),
    review VARCHAR(16777216),
    review_id NUMBER(38,0)  
);

/*--
 • Harmonized ビューの作成
--*/

-- orders_v ビュー
CREATE OR REPLACE VIEW tb_101.harmonized.orders_v
    AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tb_101.raw_pos.order_detail od
JOIN tb_101.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tb_101.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tb_101.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN tb_101.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tb_101.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tb_101.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v ビュー
CREATE OR REPLACE VIEW tb_101.harmonized.customer_loyalty_metrics_v
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw_customer.customer_loyalty cl
JOIN tb_101.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

-- truck_reviews_v ビュー
  CREATE OR REPLACE VIEW tb_101.harmonized.truck_reviews_v
      AS
  SELECT DISTINCT
      r.review_id,
      r.order_id,
      oh.truck_id,
      r.language,
      source,
      r.review,
      t.primary_city,
      oh.customer_id,
      TO_DATE(oh.order_ts) AS date,
      m.truck_brand_name
  FROM raw_support.truck_reviews r
  JOIN raw_pos.order_header oh
      ON oh.order_id = r.order_id
  JOIN raw_pos.truck t
      ON t.truck_id = oh.truck_id
  JOIN raw_pos.menu m
      ON m.menu_type_id = t.menu_type_id;

/*--
 • Analytics ビューの作成
--*/

-- orders_v ビュー
CREATE OR REPLACE VIEW tb_101.analytics.orders_v
COMMENT = 'Tasty Bytes 注文詳細ビュー'
    AS
SELECT DATE(o.order_ts) AS date, * FROM tb_101.harmonized.orders_v o;

-- customer_loyalty_metrics_v ビュー
CREATE OR REPLACE VIEW tb_101.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes 顧客ロイヤルティメンバー指標ビュー'
    AS
SELECT * FROM tb_101.harmonized.customer_loyalty_metrics_v;

-- truck_reviews_v ビュー
CREATE OR REPLACE VIEW tb_101.analytics.truck_reviews_v 
    AS
SELECT * FROM harmonized.truck_reviews_v;
GRANT USAGE ON SCHEMA raw_support to ROLE tb_admin;
GRANT SELECT ON TABLE raw_support.truck_reviews TO ROLE tb_admin;

-- Streamlit アプリ用ビュー
CREATE OR REPLACE VIEW tb_101.analytics.japan_menu_item_sales_feb_2022
AS
SELECT
    DISTINCT menu_item_name,
    date,
    order_total
FROM analytics.orders_v
WHERE country = 'Japan'
    AND YEAR(date) = '2022'
    AND MONTH(date) = '2'
GROUP BY ALL
ORDER BY date;

-- セマンティックレイヤー用注文ビュー

CREATE OR REPLACE VIEW tb_101.semantic_layer.orders_v
COMMENT = 'サービス利用顧客と既知の場所からの注文のみを含む、ビジネス向けクリーンな注文データビュー。'
AS
SELECT
    order_id::VARCHAR AS order_id,
    truck_id::VARCHAR AS truck_id,
    order_detail_id::VARCHAR AS order_detail_id,
    truck_brand_name,
    menu_type,
    primary_city,
    region,
    country,
    franchise_flag,
    franchise_id::VARCHAR AS franchise_id,
    location_id::VARCHAR AS location_id,
    customer_id::VARCHAR AS customer_id,
    gender,
    marital_status,
    menu_item_id::VARCHAR AS menu_item_id,
    menu_item_name,
    quantity,
    order_total,
    DATE(order_ts) AS order_date
FROM 
    tb_101.harmonized.orders_v
WHERE
    customer_id IS NOT NULL 
    AND primary_city IS NOT NULL;

-- セマンティックレイヤー用顧客ロイヤルティ指標ビュー
CREATE OR REPLACE VIEW tb_101.semantic_layer.customer_loyalty_metrics_v
AS
SELECT
    cl.customer_id::VARCHAR AS customer_id,
    cl.city,
    cl.country,
    SUM(o.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT o.location_id::VARCHAR) WITHIN GROUP (ORDER BY o.location_id::VARCHAR) AS visited_location_ids_array
    FROM tb_101.harmonized.customer_loyalty_metrics_v AS cl
    JOIN tb_101.harmonized.orders_v AS o
        ON cl.customer_id = o.customer_id
    GROUP BY
        cl.customer_id,
        cl.city,
        cl.country;

/*--
 RAW ゾーン テーブルへのデータロード
--*/

-- truck_reviews テーブルのロード
COPY INTO tb_101.raw_support.truck_reviews
FROM @tb_101.public.truck_reviews_s3load/raw_support/truck_reviews/;

-- country テーブルのロード
COPY INTO tb_101.raw_pos.country
FROM @tb_101.public.s3load/raw_pos/country/;

-- franchise テーブルのロード
COPY INTO tb_101.raw_pos.franchise
FROM @tb_101.public.s3load/raw_pos/franchise/;

-- location テーブルのロード
COPY INTO tb_101.raw_pos.location
FROM @tb_101.public.s3load/raw_pos/location/;

-- menu テーブルのロード
COPY INTO tb_101.raw_pos.menu
FROM @tb_101.public.s3load/raw_pos/menu/;

-- truck テーブルのロード
COPY INTO tb_101.raw_pos.truck
FROM @tb_101.public.s3load/raw_pos/truck/;

-- customer_loyalty テーブルのロード
COPY INTO tb_101.raw_customer.customer_loyalty
FROM @tb_101.public.s3load/raw_customer/customer_loyalty/;

-- order_header テーブルのロード
COPY INTO tb_101.raw_pos.order_header
FROM @tb_101.public.s3load/raw_pos/order_header/;

-- トラック詳細のセットアップ
USE WAREHOUSE tb_de_wh;

-- order_detail テーブルのロード
COPY INTO tb_101.raw_pos.order_detail
FROM @tb_101.public.s3load/raw_pos/order_detail/;

-- truck_build カラムを追加する
ALTER TABLE tb_101.raw_pos.truck
ADD COLUMN truck_build OBJECT;

-- year, make, model からオブジェクトを構築して truck_build カラムに格納する
UPDATE tb_101.raw_pos.truck
    SET truck_build = OBJECT_CONSTRUCT(
        'year', year,
        'make', make,
        'model', model
    );

-- truck_build オブジェクト内の make データに意図的な誤りを混入させる
UPDATE tb_101.raw_pos.truck
SET truck_build = OBJECT_INSERT(
    truck_build,
    'make',
    'Ford',
    TRUE
)
WHERE 
    truck_build:make::STRING = 'Ford_'
    AND 
    truck_id % 2 = 0;

-- truck_details テーブルの作成
CREATE OR REPLACE TABLE tb_101.raw_pos.truck_details
AS 
SELECT * EXCLUDE (year, make, model)
FROM tb_101.raw_pos.truck;

-- 'tasty_bytes_review_search' という名前の Cortex Search サービスを作成する。
CREATE OR REPLACE CORTEX SEARCH SERVICE tb_101.harmonized.tasty_bytes_review_search
ON REVIEW 
ATTRIBUTES LANGUAGE, ORDER_ID, REVIEW_ID, TRUCK_BRAND_NAME, PRIMARY_CITY, DATE, SOURCE 
WAREHOUSE = tb_de_wh
TARGET_LAG = '1 hour' 
AS (
    SELECT
        REVIEW,             
        LANGUAGE,           
        ORDER_ID,           
        REVIEW_ID,          
        TRUCK_BRAND_NAME,  
        PRIMARY_CITY,       
        DATE,               
        SOURCE             
    FROM
        tb_101.harmonized.truck_reviews_v 
    WHERE
        REVIEW IS NOT NULL 
);

USE ROLE securityadmin;
-- セマンティックレイヤーへの追加権限付与
GRANT SELECT ON VIEW tb_101.semantic_layer.orders_v TO ROLE PUBLIC;
GRANT SELECT ON VIEW tb_101.semantic_layer.customer_loyalty_metrics_v TO ROLE PUBLIC;
GRANT READ ON STAGE tb_101.semantic_layer.semantic_model_stage TO ROLE tb_admin;
GRANT WRITE ON STAGE tb_101.semantic_layer.semantic_model_stage TO ROLE tb_admin;

-- 参加者アカウントの設定 パート3
USE ROLE ACCOUNTADMIN;

-- LLMモデルがリージョン外の場合もクロスリージョン実行を許可する
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
 
-- データベースを作成する
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;

-- エージェントを格納するスキーマに権限を付与する
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE TB_DEV;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE TB_DEV;

-- agents スキーマに対して CREATE AGENT 権限を付与する
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE TB_DEV;
